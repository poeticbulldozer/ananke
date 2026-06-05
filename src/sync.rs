use crossbeam_channel::bounded;
use flate2::read::GzDecoder;
use rusqlite::{params, Connection};
use std::{
    fs::{self, File},
    io::{BufReader, Read, Write},
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

use crate::config::*;
use crate::db::{current_time_secs, db_writer_worker, init_db};
use crate::models::SpanshSystem;

pub fn download_file(url: &str, target: &str) -> bool {
    let tmp = FILE_SYSTEMS_DOWNLOADING;
    let _ = fs::remove_file(tmp);

    info!("Downloading {} -> {} ...", url, target);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(7200))
        .connect_timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let mut resp = match client.get(url).send() {
        Ok(r) => r,
        Err(e) => { error!("Download failed to connect: {}", e); return false; }
    };

    if !resp.status().is_success() {
        error!("Download HTTP error: {}", resp.status());
        return false;
    }

    let total_bytes = resp.content_length().unwrap_or(0);
    let total_mb = total_bytes as f64 / (1024.0 * 1024.0);

    let mut out = match File::create(tmp) {
        Ok(f) => f,
        Err(e) => { error!("Failed to create temp file: {}", e); return false; }
    };

    let mut downloaded: u64 = 0;
    let mut buf = [0u8; 256 * 1024];
    let mut last_log = Instant::now();

    loop {
        let n = match Read::read(&mut resp, &mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                error!("Download read error at {:.1} MB: {}", downloaded as f64 / 1048576.0, e);
                let _ = fs::remove_file(tmp);
                return false;
            }
        };

        if out.write_all(&buf[..n]).is_err() {
            error!("Failed to write to temp file");
            let _ = fs::remove_file(tmp);
            return false;
        }

        downloaded += n as u64;

        if last_log.elapsed() >= Duration::from_secs(15) {
            let dl_mb = downloaded as f64 / 1048576.0;
            if total_bytes > 0 {
                let pct = (downloaded as f64 / total_bytes as f64) * 100.0;
                info!("Download progress: {:.1}/{:.1} MB ({:.1}%)", dl_mb, total_mb, pct);
            } else {
                info!("Download progress: {:.1} MB (size unknown)", dl_mb);
            }
            last_log = Instant::now();
        }
    }

    drop(out);

    let _ = fs::remove_file(target);
    if let Err(e) = fs::rename(tmp, target) {
        error!("Failed to rename {} -> {}: {}", tmp, target, e);
        let _ = fs::remove_file(tmp);
        return false;
    }

    let final_mb = downloaded as f64 / 1048576.0;
    info!("Download complete: {:.1} MB written to {}", final_mb, target);
    true
}

pub fn process_systems_dump(filename: &str) {
    info!("Processing GALAXY DATA from: {}...", filename);
    let file = File::open(filename).unwrap();
    let decoder = GzDecoder::new(file);
    let reader = BufReader::with_capacity(1024 * 1024 * 8, decoder);

    let (sender, receiver) = bounded(10);
    let writer_thread = std::thread::spawn(move || db_writer_worker(receiver));

    let stream = serde_json::Deserializer::from_reader(reader).into_iter::<SpanshSystem>();
    let mut batch = Vec::with_capacity(5000);
    let mut count = 0;

    for item in stream {
        if let Ok(sys) = item {
            batch.push(sys);
            count += 1;
            if batch.len() >= 5000 {
                sender.send(std::mem::take(&mut batch)).unwrap();
                print!("\rImported {} systems...", count);
                std::io::stdout().flush().unwrap();
            }
        }
    }

    if !batch.is_empty() { sender.send(batch).unwrap(); }
    drop(sender);
    writer_thread.join().unwrap();

    let conn = Connection::open(DB_FILE).unwrap();
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('last_sync_time', ?)", params![current_time_secs().to_string()]).unwrap();
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('import_complete', 'true')", []).unwrap();
    info!("\nImport Finished. {} systems. Database Ready.", count);
}

pub async fn sync_manager() {
    loop {
        let needs_sync = tokio::task::spawn_blocking(|| {
            let conn = Connection::open(DB_FILE).unwrap();
            init_db(&conn).unwrap();

            // One-time backfill: neutron_systems
            let neutron_count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM neutron_systems", [], |r| r.get(0)
            ).unwrap_or(0);
            if neutron_count == 0 {
                let body_count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM bodies WHERE subType='Neutron Star' LIMIT 1", [], |r| r.get(0)
                ).unwrap_or(0);
                if body_count > 0 {
                    info!("Backfilling neutron_systems table from bodies (one-time migration)...");
                    conn.execute_batch(
                        "INSERT OR IGNORE INTO neutron_systems (systemId64)
                         SELECT DISTINCT systemId64 FROM bodies WHERE subType='Neutron Star'"
                    ).unwrap();
                    info!("neutron_systems backfill complete.");
                }
            }

            // One-time backfill: prison_systems
            let prison_count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM prison_systems", [], |r| r.get(0)
            ).unwrap_or(0);
            if prison_count == 0 {
                let sys_count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM systems LIMIT 1", [], |r| r.get(0)
                ).unwrap_or(0);
                if sys_count > 0 {
                    info!("Backfilling prison_systems table from systems and stations (one-time migration)...");
                    conn.execute_batch(
                        "INSERT OR IGNORE INTO prison_systems (systemId64)
                            SELECT id64 FROM systems
                            WHERE government LIKE '%rison%' COLLATE NOCASE;
                         INSERT OR IGNORE INTO prison_systems (systemId64)
                            SELECT DISTINCT systemId64 FROM stations
                            WHERE government LIKE '%rison%' COLLATE NOCASE;"
                    ).unwrap();
                    let n: i64 = conn.query_row(
                        "SELECT COUNT(*) FROM prison_systems", [], |r| r.get(0)
                    ).unwrap_or(0);
                    info!("prison_systems backfill complete: {} systems flagged.", n);
                }
            }

            let last_sync: Result<String, _> = conn.query_row("SELECT value FROM meta WHERE key='last_sync_time'", [], |row| row.get(0));
            let last_sync_time: u64 = last_sync.unwrap_or_else(|_| "0".to_string()).parse().unwrap_or(0);
            let age = current_time_secs() - last_sync_time;
            let stale = age > SYNC_INTERVAL_SECONDS;
            if stale {
                info!("Sync needed: last sync was {}h {}m ago (threshold: {}h)",
                    age / 3600, (age % 3600) / 60, SYNC_INTERVAL_SECONDS / 3600);
            } else {
                let remaining = SYNC_INTERVAL_SECONDS - age;
                info!("Sync not needed yet. Next sync in {}h {}m.",
                    remaining / 3600, (remaining % 3600) / 60);
            }
            stale
        }).await.unwrap();

        if needs_sync {
            info!("Starting Spansh galaxy sync...");
            let t_start = Instant::now();

            if download_file(URL_SYSTEMS_1DAY, FILE_SYSTEMS_1DAY) {
                tokio::task::spawn_blocking(|| {
                    process_systems_dump(FILE_SYSTEMS_1DAY);
                    if let Err(e) = fs::remove_file(FILE_SYSTEMS_1DAY) {
                        warn!("Could not remove dump file after import: {}", e);
                    } else {
                        info!("Cleaned up dump file after import.");
                    }
                }).await.unwrap();

                let elapsed = t_start.elapsed();
                info!("Full sync cycle completed in {}m {}s.",
                    elapsed.as_secs() / 60, elapsed.as_secs() % 60);
            } else {
                error!("Sync failed: download unsuccessful. Will retry next cycle.");
            }
        }

        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}