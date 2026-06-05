use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use crossbeam_channel::{bounded, Receiver};
use flate2::read::GzDecoder;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, Result as SqliteResult};
use serde::Deserialize;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fs::File,
    io::{BufReader, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Semaphore};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

// --- CONFIGURATION ---
const DB_FILE: &str = "edsm_cube.db";
const PORT: u16 = 8000;
const URL_SYSTEMS_1DAY: &str = "https://downloads.spansh.co.uk/galaxy_1day.json.gz";
const FILE_SYSTEMS_1DAY: &str = "galaxy_1day.json.gz";
const SYNC_INTERVAL_SECONDS: u64 = 86400; // 1 Day
const MAX_CONCURRENT_QUERIES: usize = 6;

// --- STATE ---
#[allow(dead_code)]
struct AppState {
    db_pool: Pool<SqliteConnectionManager>,
    query_semaphore: Arc<Semaphore>,
    carrier_cache: Mutex<CarrierCache>,
}

#[allow(dead_code)]
struct CarrierCache {
    data: Option<serde_json::Value>,
    expires_at: u64,
}

// --- MODELS ---
#[derive(Deserialize, Debug)]
struct SpanshCoords { x: f64, y: f64, z: f64 }

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct SpanshSystem {
    id64: i64,
    name: String,
    population: Option<i64>,
    coords: Option<SpanshCoords>,
    bodies: Option<Vec<serde_json::Value>>,
    stations: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct CubeSearchQuery {
    ref_system: Option<String>,
    center: Option<String>,
    x: Option<f64>,
    y: Option<f64>,
    z: Option<f64>,
    size: Option<f64>,
    #[serde(rename = "bodyType", alias = "body_type")]
    body_type: Option<String>,
}

#[derive(Deserialize)]
struct SystemQuery {
    #[serde(rename = "systemName", alias = "name")]
    system_name: Option<String>,
}

// Ship Router Query
#[derive(Deserialize)]
struct RouteQuery {
    source: String,
    destination: String,
}

// Fleet Carrier Router Query
#[derive(Deserialize)]
struct CarrierRouteQuery {
    current_system: String,
    destination: String,
    #[serde(alias = "cargo_capacity")]
    used_cargo: f64,
    #[serde(alias = "current_fuel")]
    tank_fuel: f64,
    #[serde(alias = "market_tritium")]
    stored_tritium: f64,
    is_squadron: Option<bool>,
}

// Neutron Router Query
#[derive(Deserialize)]
struct NeutronRouteQuery {
    source: String,
    destination: String,
    range: f64,
    supercharge_type: String,
}

// A* Node for ship routing
#[derive(Clone)]
struct RouteNode {
    g_score: usize,
    f_score: f64,
    id64: i64,
    x: f64,
    y: f64,
    z: f64,
}

impl PartialEq for RouteNode {
    fn eq(&self, other: &Self) -> bool { self.id64 == other.id64 }
}
impl Eq for RouteNode {}
impl PartialOrd for RouteNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for RouteNode {
    fn cmp(&self, other: &Self) -> Ordering {
        other.f_score.partial_cmp(&self.f_score).unwrap_or(Ordering::Equal)
    }
}

// --- DATABASE SETUP ---
fn setup_db_pool() -> Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::file(DB_FILE).with_init(|c| {
        c.execute_batch("
            PRAGMA mmap_size = 8589934592;
            PRAGMA cache_size = -2097152;
            PRAGMA temp_store = MEMORY;
            PRAGMA journal_size_limit = 1073741824;
            PRAGMA journal_mode = WAL;
            PRAGMA busy_timeout = 60000;
            PRAGMA synchronous = NORMAL;
        ")
    });
    Pool::builder().max_size(15).build(manager).expect("Failed to create DB pool")
}

fn init_db(conn: &Connection) -> SqliteResult<()> {
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS systems (id64 INTEGER PRIMARY KEY, name TEXT, population INTEGER, last_update INTEGER);
        CREATE INDEX IF NOT EXISTS idx_systems_name_nocase ON systems(name COLLATE NOCASE);
        CREATE VIRTUAL TABLE IF NOT EXISTS systems_index USING rtree(id, minX, maxX, minY, maxY, minZ, maxZ);
        CREATE TABLE IF NOT EXISTS bodies (systemId64 INTEGER, bodyId INTEGER, name TEXT, type TEXT, subType TEXT, distanceToArrival REAL, isLandable INTEGER, gravity REAL, earthMasses REAL, radius REAL, surfaceTemperature INTEGER, orbitalPeriod REAL, semiMajorAxis REAL, orbitalEccentricity REAL, orbitalInclination REAL, argOfPeriapsis REAL, rotationalPeriod REAL, isTidallyLocked INTEGER, axisTilt REAL, volcanismType TEXT, atmosphereType TEXT, terraformingState TEXT, PRIMARY KEY (systemId64, bodyId)) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS stations (id INTEGER, marketId INTEGER, systemId64 INTEGER, name TEXT, type TEXT, distanceToArrival REAL, allegiance TEXT, government TEXT, economy TEXT, secondEconomy TEXT, haveMarket INTEGER, haveShipyard INTEGER, haveOutfitting INTEGER, otherServices TEXT, updateTime TEXT, PRIMARY KEY (systemId64, id)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS neutron_systems (systemId64 INTEGER PRIMARY KEY);
    ")
}

// --- BACKGROUND SYNC MANAGER ---
fn current_time_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// Fetches https://spansh.co.uk/dumps and returns the "Generated" timestamp string
/// for the given filename (e.g. "galaxy_1day.json.gz"), or None on any failure.
/// Spansh's dump page contains rows like:
///   galaxy_1day.json.gz  ...  Generated: 2025-01-15 12:34:56
/// We search for the filename and then scan ahead for the first YYYY-MM-DD HH:MM:SS pattern.
fn fetch_spansh_dump_generated_time(filename: &str) -> Option<String> {
    let resp = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build().ok()?
        .get("https://spansh.co.uk/dumps")
        .send().ok()?;

    let body = resp.text().ok()?;

    // Find the row containing this filename, then scan up to 800 chars ahead for a timestamp.
    let anchor = body.find(filename)?;
    let window = &body[anchor .. (anchor + 800).min(body.len())];

    // Walk through the window looking for the first YYYY-MM-DD HH:MM:SS
    for i in 0 .. window.len().saturating_sub(19) {
        let s = &window[i .. i + 19];
        let b = s.as_bytes();
        // Quick structural check: ????-??-?? ??:??:??
        if b[4] == b'-' && b[7] == b'-' && b[10] == b' ' && b[13] == b':' && b[16] == b':' {
            // Verify all other positions are ASCII digits
            let digit_positions = [0,1,2,3, 5,6, 8,9, 11,12, 14,15, 17,18];
            if digit_positions.iter().all(|&p| b[p].is_ascii_digit()) {
                return Some(s.to_string());
            }
        }
    }
    None
}

fn download_file(url: &str, target: &str) -> bool {
    if Path::new(target).exists() {
        info!("File {} exists, skipping download.", target);
        return true;
    }
    info!("Downloading {}...", url);
    let mut resp = match reqwest::blocking::get(url) {
        Ok(r) => r,
        Err(e) => { error!("Download failed: {}", e); return false; }
    };
    let mut out = File::create(target).unwrap();
    std::io::copy(&mut resp, &mut out).is_ok()
}

fn get_i64(v: &serde_json::Value, k: &str) -> Option<i64> { v.get(k).and_then(|x| x.as_i64()) }
fn get_f64(v: &serde_json::Value, k: &str) -> Option<f64> { v.get(k).and_then(|x| x.as_f64().or_else(|| x.as_i64().map(|i| i as f64))) }
fn get_str<'a>(v: &'a serde_json::Value, k: &str) -> Option<&'a str> { v.get(k).and_then(|x| x.as_str()) }
fn get_bool(v: &serde_json::Value, k: &str) -> i32 { v.get(k).and_then(|x| x.as_bool()).map(|x| if x { 1 } else { 0 }).unwrap_or(0) }

fn db_writer_worker(receiver: Receiver<Vec<SpanshSystem>>) {
    let mut conn = Connection::open(DB_FILE).unwrap();
    conn.execute_batch("PRAGMA synchronous = OFF; PRAGMA journal_mode = WAL;").unwrap();

    while let Ok(batch) = receiver.recv() {
        let tx = conn.transaction().unwrap();
        {
            let mut stmt_sys = tx.prepare_cached("INSERT OR REPLACE INTO systems (id64, name, population, last_update) VALUES (?, ?, ?, ?)").unwrap();
            let mut stmt_idx = tx.prepare_cached("INSERT OR REPLACE INTO systems_index (id, minX, maxX, minY, maxY, minZ, maxZ) VALUES (?, ?, ?, ?, ?, ?, ?)").unwrap();
            let mut stmt_bodies = tx.prepare_cached("INSERT OR REPLACE INTO bodies (systemId64, bodyId, name, type, subType, distanceToArrival, isLandable, gravity, earthMasses, radius, surfaceTemperature, orbitalPeriod, semiMajorAxis, orbitalEccentricity, orbitalInclination, argOfPeriapsis, rotationalPeriod, isTidallyLocked, axisTilt, volcanismType, atmosphereType, terraformingState) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").unwrap();
            let mut stmt_stations = tx.prepare_cached("INSERT OR REPLACE INTO stations (id, marketId, systemId64, name, type, distanceToArrival, allegiance, government, economy, secondEconomy, haveMarket, haveShipyard, haveOutfitting, otherServices, updateTime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").unwrap();
            let mut stmt_neutron = tx.prepare_cached("INSERT OR IGNORE INTO neutron_systems (systemId64) VALUES (?)").unwrap();

            let now = current_time_secs() as i64;
            for sys in batch {
                let pop = sys.population.unwrap_or(0);
                stmt_sys.execute(params![sys.id64, sys.name, pop, now]).unwrap();

                if let Some(c) = sys.coords {
                    stmt_idx.execute(params![sys.id64, c.x, c.x, c.y, c.y, c.z, c.z]).unwrap();
                }

                if let Some(bodies) = &sys.bodies {
                    for b in bodies {
                        stmt_bodies.execute(params![
                            sys.id64, get_i64(b, "bodyId"), get_str(b, "name"), get_str(b, "type"), get_str(b, "subType"),
                            get_f64(b, "distanceToArrival"), get_bool(b, "isLandable"), get_f64(b, "gravity"), get_f64(b, "earthMasses"),
                            get_f64(b, "radius"), get_i64(b, "surfaceTemperature"), get_f64(b, "orbitalPeriod"), get_f64(b, "semiMajorAxis"),
                            get_f64(b, "orbitalEccentricity"), get_f64(b, "orbitalInclination"), get_f64(b, "argOfPeriapsis"), get_f64(b, "rotationalPeriod"),
                            get_bool(b, "rotationalPeriodTidallyLocked"), get_f64(b, "axialTilt"), get_str(b, "volcanismType"), get_str(b, "atmosphereType"),
                            get_str(b, "terraformingState")
                        ]).ok();
                        if get_str(b, "subType") == Some("Neutron Star") {
                            stmt_neutron.execute(params![sys.id64]).ok();
                        }
                    }
                }

                if let Some(stations) = &sys.stations {
                    for st in stations {
                        let svcs = st.get("services").and_then(|s| s.as_array());
                        let mut has_market = 0; let mut has_shipyard = 0; let mut has_outfitting = 0;
                        let mut other_svcs = Vec::new();

                        if let Some(arr) = svcs {
                            for v in arr {
                                if let Some(s) = v.as_str() {
                                    match s {
                                        "Market" => has_market = 1,
                                        "Shipyard" => has_shipyard = 1,
                                        "Outfitting" => has_outfitting = 1,
                                        "Dock" | "Autodock" => {},
                                        _ => other_svcs.push(s),
                                    }
                                }
                            }
                        }
                        let other_svcs_json = serde_json::to_string(&other_svcs).unwrap_or_else(|_| "[]".to_string());

                        stmt_stations.execute(params![
                            get_i64(st, "id"), get_i64(st, "marketId"), sys.id64, get_str(st, "name"), get_str(st, "type"), get_f64(st, "distanceToArrival"),
                            get_str(st, "allegiance"), get_str(st, "government"), get_str(st, "primaryEconomy"), get_str(st, "secondaryEconomy"),
                            has_market, has_shipyard, has_outfitting, other_svcs_json, get_str(st, "updateTime")
                        ]).ok();
                    }
                }
            }
        }
        tx.commit().unwrap();
    }
    info!("DB Writer Worker shut down successfully.");
}

fn process_systems_dump(filename: &str, dump_time: Option<String>) {
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

    // Record the Spansh dump generation timestamp so future runs can detect actual changes.
    if let Some(ts) = dump_time {
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('spansh_dump_time', ?)", params![ts]).unwrap();
        info!("\nImport Finished. Database Ready. (Spansh dump timestamp: {})", ts);
    } else {
        info!("\nImport Finished. Database Ready.");
    }
}

async fn sync_manager(skip_initial: bool) {
    let mut first_run = true;

    loop {
        let skip_this_run = skip_initial && first_run;
        first_run = false;

        // Run the blocking DB/network check on a worker thread.
        let sync_decision = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(DB_FILE).unwrap();
            init_db(&conn).unwrap();

            // One-time migration: populate neutron_systems from bodies if empty.
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

            // Honour --skip-download on the very first loop iteration.
            if skip_this_run {
                info!("--skip-download flag set: skipping initial galaxy data sync.");
                return (false, None::<String>);
            }

            // --- Smart dump detection via spansh.co.uk/dumps ---
            match fetch_spansh_dump_generated_time("galaxy_1day.json.gz") {
                Some(remote_ts) => {
                    let stored_ts: Option<String> = conn.query_row(
                        "SELECT value FROM meta WHERE key='spansh_dump_time'",
                        [],
                        |r| r.get(0),
                    ).ok();

                    match stored_ts {
                        Some(ref local_ts) if local_ts == &remote_ts => {
                            info!("Spansh dump unchanged (Generated: {}), skipping sync.", remote_ts);
                            (false, None)
                        }
                        _ => {
                            info!("New Spansh dump detected (Generated: {}), queuing sync.", remote_ts);
                            (true, Some(remote_ts))
                        }
                    }
                }
                None => {
                    // Couldn't reach spansh.co.uk/dumps — fall back to time-based check.
                    info!("Could not reach spansh.co.uk/dumps; falling back to time-based sync check.");
                    let last_sync_time: u64 = conn
                        .query_row("SELECT value FROM meta WHERE key='last_sync_time'", [], |r| r.get::<_, String>(0))
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    let needs = current_time_secs() - last_sync_time > SYNC_INTERVAL_SECONDS;
                    (needs, None)
                }
            }
        }).await.unwrap();

        let (needs_sync, remote_dump_time) = sync_decision;

        if needs_sync {
            // Remove any stale local copy so download_file always fetches fresh.
            if Path::new(FILE_SYSTEMS_1DAY).exists() {
                info!("Removing stale {}", FILE_SYSTEMS_1DAY);
                let _ = std::fs::remove_file(FILE_SYSTEMS_1DAY);
            }

            if download_file(URL_SYSTEMS_1DAY, FILE_SYSTEMS_1DAY) {
                let dump_time = remote_dump_time.clone();
                tokio::task::spawn_blocking(move || process_systems_dump(FILE_SYSTEMS_1DAY, dump_time)).await.unwrap();
            }
        }

        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

// --- API ENDPOINTS ---

async fn get_system(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SystemQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;
    let name = params.system_name.ok_or((StatusCode::BAD_REQUEST, "Missing systemName".into()))?;

    let pool = state.db_pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let mut stmt = conn.prepare("
            SELECT s.id64, s.name, s.population, i.minX, i.minY, i.minZ
            FROM systems s JOIN systems_index i ON s.id64 = i.id
            WHERE s.name = ? COLLATE NOCASE LIMIT 1
        ").unwrap();

        let row = stmt.query_row(params![name], |row| {
            Ok(serde_json::json!({
                "id64": row.get::<_, i64>(0)?, "name": row.get::<_, String>(1)?, "population": row.get::<_, i64>(2)?,
                "coords": {"x": row.get::<_, f64>(3)?, "y": row.get::<_, f64>(4)?, "z": row.get::<_, f64>(5)?}
            }))
        });

        row.map_err(|_| "System not found".to_string())
    }).await.unwrap();

    match result {
        Ok(json) => Ok(Json(json)),
        Err(_) => Err((StatusCode::NOT_FOUND, "System not found".into())),
    }
}

async fn get_system_bodies(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SystemQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;
    let name = params.system_name.ok_or((StatusCode::BAD_REQUEST, "Missing systemName".into()))?;

    let pool = state.db_pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let sys_row: Result<(i64, String), _> = conn.query_row(
            "SELECT id64, name FROM systems WHERE name = ? COLLATE NOCASE LIMIT 1",
            params![name],
            |row| Ok((row.get(0)?, row.get(1)?))
        );

        let (sys_id, sys_name) = match sys_row {
            Ok(res) => res,
            Err(_) => return Ok(serde_json::json!({"bodies": []})),
        };

        let mut stmt = conn.prepare("SELECT * FROM bodies WHERE systemId64 = ? ORDER BY distanceToArrival ASC").unwrap();
        let rows = stmt.query_map(params![sys_id], |b| {
            let is_landable: i64 = b.get("isLandable").unwrap_or(0);
            let is_tidally_locked: i64 = b.get("isTidallyLocked").unwrap_or(0);
            let tf_state: Option<String> = b.get("terraformingState").unwrap_or(None);

            Ok(serde_json::json!({
                "id": b.get::<_, i64>("bodyId")?,
                "id64": b.get::<_, i64>("bodyId")?,
                "name": b.get::<_, String>("name")?,
                "type": b.get::<_, Option<String>>("type")?,
                "subType": b.get::<_, Option<String>>("subType")?,
                "distanceToArrival": b.get::<_, Option<f64>>("distanceToArrival")?,
                "isLandable": is_landable == 1,
                "gravity": b.get::<_, Option<f64>>("gravity")?,
                "earthMasses": b.get::<_, Option<f64>>("earthMasses")?,
                "radius": b.get::<_, Option<f64>>("radius")?,
                "surfaceTemperature": b.get::<_, Option<i64>>("surfaceTemperature")?,
                "volcanismType": b.get::<_, Option<String>>("volcanismType")?,
                "atmosphereType": b.get::<_, Option<String>>("atmosphereType")?,
                "terraformingState": tf_state.unwrap_or_else(|| "Not terraformable".to_string()),
                "orbitalPeriod": b.get::<_, Option<f64>>("orbitalPeriod")?,
                "semiMajorAxis": b.get::<_, Option<f64>>("semiMajorAxis")?,
                "orbitalEccentricity": b.get::<_, Option<f64>>("orbitalEccentricity")?,
                "orbitalInclination": b.get::<_, Option<f64>>("orbitalInclination")?,
                "argOfPeriapsis": b.get::<_, Option<f64>>("argOfPeriapsis")?,
                "rotationalPeriod": b.get::<_, Option<f64>>("rotationalPeriod")?,
                "rotationalPeriodTidallyLocked": is_tidally_locked == 1,
                "axialTilt": b.get::<_, Option<f64>>("axisTilt")?
            }))
        }).unwrap();

        let bodies: Vec<_> = rows.filter_map(Result::ok).collect();
        Ok(serde_json::json!({"id64": sys_id, "name": sys_name, "bodies": bodies}))
    }).await.unwrap().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(result))
}

async fn get_system_stations(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SystemQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;
    let name = params.system_name.ok_or((StatusCode::BAD_REQUEST, "Missing systemName".into()))?;

    let pool = state.db_pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let sys_row: Result<(i64, String), _> = conn.query_row(
            "SELECT id64, name FROM systems WHERE name = ? COLLATE NOCASE LIMIT 1",
            params![name],
            |row| Ok((row.get(0)?, row.get(1)?))
        );

        let (sys_id, sys_name) = match sys_row {
            Ok(res) => res,
            Err(_) => return Ok(serde_json::json!({"stations": []})),
        };

        let mut stmt = conn.prepare("SELECT * FROM stations WHERE systemId64 = ? ORDER BY distanceToArrival ASC").unwrap();
        let rows = stmt.query_map(params![sys_id], |s| {
            let have_market: i64 = s.get("haveMarket").unwrap_or(0);
            let have_shipyard: i64 = s.get("haveShipyard").unwrap_or(0);
            let have_outfitting: i64 = s.get("haveOutfitting").unwrap_or(0);

            let other_services_str: String = s.get("otherServices").unwrap_or_else(|_| "[]".to_string());
            let other_services: Vec<String> = serde_json::from_str(&other_services_str).unwrap_or_default();

            Ok(serde_json::json!({
                "id": s.get::<_, i64>("id")?,
                "marketId": s.get::<_, Option<i64>>("marketId")?,
                "type": s.get::<_, Option<String>>("type")?,
                "name": s.get::<_, Option<String>>("name")?,
                "distanceToArrival": s.get::<_, Option<f64>>("distanceToArrival")?,
                "allegiance": s.get::<_, Option<String>>("allegiance")?,
                "government": s.get::<_, Option<String>>("government")?,
                "economy": s.get::<_, Option<String>>("economy")?,
                "secondEconomy": s.get::<_, Option<String>>("secondEconomy")?,
                "haveMarket": have_market == 1,
                "haveShipyard": have_shipyard == 1,
                "haveOutfitting": have_outfitting == 1,
                "otherServices": other_services,
                "updateTime": { "information": s.get::<_, Option<String>>("updateTime")? }
            }))
        }).unwrap();

        let stations: Vec<_> = rows.filter_map(Result::ok).collect();
        Ok(serde_json::json!({"id64": sys_id, "name": sys_name, "stations": stations}))
    }).await.unwrap().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(result))
}

async fn get_carrier_progression(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let current_time = current_time_secs();

    {
        let cache = state.carrier_cache.lock().await;
        if current_time < cache.expires_at {
            if let Some(data) = &cache.data {
                return Ok(Json(data.clone()));
            }
        }
    }

    let pool = state.db_pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let resp = reqwest::blocking::get("https://scoreboard.projectgaltea.org/api/carrier").map_err(|e| e.to_string())?;
        let carrier_data: serde_json::Value = resp.json().map_err(|e| e.to_string())?;

        let parse_coord = |v: Option<&serde_json::Value>| -> f64 {
            match v {
                Some(val) if val.is_f64() => val.as_f64().unwrap(),
                Some(val) if val.is_i64() => val.as_i64().unwrap() as f64,
                Some(val) if val.is_string() => val.as_str().unwrap().parse().unwrap_or(0.0),
                _ => 0.0,
            }
        };

        let cx = parse_coord(carrier_data.pointer("/record/system_x"));
        let cy = parse_coord(carrier_data.pointer("/record/system_y"));
        let cz = parse_coord(carrier_data.pointer("/record/system_z"));

        let conn = pool.get().map_err(|e| e.to_string())?;

        let get_coords = |id: i64| -> Option<(f64, f64, f64)> {
            conn.query_row(
                "SELECT i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.id64 = ? LIMIT 1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            ).ok()
        };

        let start_pos = get_coords(10477373803).unwrap_or((0.0, 0.0, 0.0));
        let target_pos = get_coords(10938300568211).unwrap_or((0.0, 0.0, 0.0));

        let ab_x = target_pos.0 - start_pos.0;
        let ab_y = target_pos.1 - start_pos.1;
        let ab_z = target_pos.2 - start_pos.2;

        let ac_x = cx - start_pos.0;
        let ac_y = cy - start_pos.1;
        let ac_z = cz - start_pos.2;

        let dot = (ac_x * ab_x) + (ac_y * ab_y) + (ac_z * ab_z);
        let mag = (ab_x.powi(2)) + (ab_y.powi(2)) + (ab_z.powi(2));

        let percentage = if mag != 0.0 { dot / mag * 100.0 } else { 0.0 };
        let rounded_percentage = (percentage * 10000.0).round() / 10000.0;

        Ok(serde_json::json!({ "route_progression_percentage": rounded_percentage }))
    }).await.unwrap().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let mut cache = state.carrier_cache.lock().await;
    cache.data = Some(result.clone());
    cache.expires_at = current_time + 86400;

    Ok(Json(result))
}

async fn do_cube_search(
    state: Arc<AppState>,
    params: CubeSearchQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;
    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let mut cx = params.x.unwrap_or(0.0);
        let mut cy = params.y.unwrap_or(0.0);
        let mut cz = params.z.unwrap_or(0.0);

        let ref_sys = params.ref_system.or(params.center);
        if let Some(sys_name) = &ref_sys {
            if let Ok(row) = conn.query_row("SELECT i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64=i.id WHERE s.name=? COLLATE NOCASE LIMIT 1", rusqlite::params![sys_name], |r| Ok((r.get::<_, f64>(0)?, r.get::<_, f64>(1)?, r.get::<_, f64>(2)?))) {
                cx = row.0; cy = row.1; cz = row.2;
            }
        }

        let h = params.size.unwrap_or(20.0).min(500.0) / 2.0;
        let min_x = cx - h; let max_x = cx + h;
        let min_y = cy - h; let max_y = cy + h;
        let min_z = cz - h; let max_z = cz + h;

        let body_filter = params.body_type.unwrap_or_default();
        let mut target_subtypes: Vec<&str> = Vec::new();
        let mut is_terraformable = false;

        match body_filter.to_lowercase().as_str() {
            "earth-like" | "earth-like world" | "earth-like worlds" => target_subtypes.push("Earth-like world"),
            "water world" | "water worlds" => target_subtypes.push("Water world"),
            "ammonia world" | "ammonia worlds" => target_subtypes.push("Ammonia world"),
            "neutron star" | "neutron stars" => target_subtypes.push("Neutron Star"),
            "black hole" | "black holes" => target_subtypes.push("Black Hole"),
            "terraformable" => is_terraformable = true,
            _ => {}
        };

        let mut results: Vec<serde_json::Value>;

        if is_terraformable || !target_subtypes.is_empty() {
            let mut sql = format!("
                SELECT s.id64, s.name as systemName, s.population, i.minX, i.minY, i.minZ,
                       b.bodyId, b.name as bodyName, b.subType, b.distanceToArrival
                FROM systems_index i JOIN systems s ON i.id = s.id64
                JOIN bodies b ON s.id64 = b.systemId64
                WHERE i.minX >= ? AND i.maxX <= ? AND i.minY >= ? AND i.maxY <= ? AND i.minZ >= ? AND i.maxZ <= ?
            ");

            if is_terraformable {
                sql.push_str(" AND (b.terraformingState = 'Terraformable' OR b.terraformingState = 'Terraforming completed')");
            } else {
                let placeholders = target_subtypes.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                sql.push_str(&format!(" AND b.subType IN ({})", placeholders));
            }

            sql.push_str(" LIMIT 5000");

            let mut stmt = conn.prepare(&sql).unwrap();

            let mut query_params: Vec<&dyn rusqlite::ToSql> = vec![
                &min_x, &max_x, &min_y, &max_y, &min_z, &max_z
            ];
            if !is_terraformable {
                for st in &target_subtypes {
                    query_params.push(st);
                }
            }

            let rows = stmt.query_map(&*query_params, |row| {
                let id64: i64 = row.get(0)?;
                let sys_name: String = row.get(1)?;
                let pop: i64 = row.get(2)?;
                let x: f64 = row.get(3)?;
                let y: f64 = row.get(4)?;
                let z: f64 = row.get(5)?;
                let body_id: i64 = row.get(6)?;
                let body_name: String = row.get(7)?;
                let sub_type: String = row.get(8)?;
                let dist_arrival: f64 = row.get(9)?;

                let dist_ly = ((x - cx).powi(2) + (y - cy).powi(2) + (z - cz).powi(2)).sqrt();
                let body_short = body_name.replace(&sys_name, "").trim().to_string();

                Ok(serde_json::json!({
                    "systemId64": id64.to_string(),
                    "bodyId": body_id,
                    "uniqueId": format!("{}-{}", id64, body_id),
                    "system": sys_name,
                    "body": if body_short.is_empty() { body_name.clone() } else { body_short },
                    "fullBodyName": body_name,
                    "type": sub_type,
                    "systemDistLy": (dist_ly * 100.0).round() / 100.0,
                    "arrivalDistLs": dist_arrival,
                    "inhabited": if pop > 0 { "Yes" } else { "No" },
                    "coords": {"x": x, "y": y, "z": z}
                }))
            }).unwrap();

            results = rows.filter_map(Result::ok).collect();

        } else {
            let mut stmt = conn.prepare("
                SELECT s.id64, s.name, s.population, i.minX, i.minY, i.minZ
                FROM systems_index i JOIN systems s ON i.id = s.id64
                WHERE i.minX >= ? AND i.maxX <= ? AND i.minY >= ? AND i.maxY <= ? AND i.minZ >= ? AND i.maxZ <= ? LIMIT 5000
            ").unwrap();

            let rows = stmt.query_map(rusqlite::params![min_x, max_x, min_y, max_y, min_z, max_z], |row| {
                let id64: i64 = row.get(0)?;
                let sys_name: String = row.get(1)?;
                let pop: i64 = row.get(2)?;
                let (x, y, z): (f64, f64, f64) = (row.get(3)?, row.get(4)?, row.get(5)?);
                let dist = ((x-cx).powi(2) + (y-cy).powi(2) + (z-cz).powi(2)).sqrt();
                Ok(serde_json::json!({
                    "systemId64": id64.to_string(),
                    "uniqueId": id64.to_string(),
                    "system": sys_name, "body": "-", "systemDistLy": (dist * 100.0).round() / 100.0,
                    "arrivalDistLs": 0, "inhabited": if pop > 0 { "Yes" } else { "No" },
                    "coords": {"x": x, "y": y, "z": z}
                }))
            }).unwrap();

            results = rows.filter_map(Result::ok).collect();
        }

        results.sort_by(|a, b| a["systemDistLy"].as_f64().unwrap().partial_cmp(&b["systemDistLy"].as_f64().unwrap()).unwrap());

        Ok(serde_json::json!({"cubeSize": h*2.0, "count": results.len(), "results": results}))
    }).await.unwrap().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(result))
}

async fn cube_search_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CubeSearchQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_cube_search(state, params).await
}

async fn cube_search_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<CubeSearchQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_cube_search(state, params).await
}

async fn do_ship_route(
    state: Arc<AppState>,
    params: RouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;

    let pool = state.db_pool.clone();
    let source_name = params.source.clone();
    let dest_name = params.destination.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let get_sys = |sys_input: &str| -> Result<(i64, String, f64, f64, f64), String> {
            if let Ok(id) = sys_input.parse::<i64>() {
                conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.id64 = ? LIMIT 1",
                    rusqlite::params![id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                ).map_err(|_| format!("System ID '{}' not found in database", id))
            } else {
                conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.name = ? COLLATE NOCASE LIMIT 1",
                    rusqlite::params![sys_input],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                ).map_err(|_| format!("System '{}' not found in database", sys_input))
            }
        };

        let (src_id, src_exact_name, src_x, src_y, src_z) = get_sys(&source_name)?;
        let (dst_id, dst_exact_name, dst_x, dst_y, dst_z) = get_sys(&dest_name)?;

        let mut open_set = BinaryHeap::new();
        let mut g_score_map: HashMap<i64, usize> = HashMap::new();
        let mut came_from: HashMap<i64, i64> = HashMap::new();

        let start_h = ((dst_x - src_x).powi(2) + (dst_y - src_y).powi(2) + (dst_z - src_z).powi(2)).sqrt() / 15.00;

        open_set.push(RouteNode {
            g_score: 0,
            f_score: start_h,
            id64: src_id,
            x: src_x,
            y: src_y,
            z: src_z,
        });

        g_score_map.insert(src_id, 0);

        let mut stmt = conn.prepare("
            SELECT id, minX, minY, minZ
            FROM systems_index
            WHERE minX >= ? AND maxX <= ?
              AND minY >= ? AND maxY <= ?
              AND minZ >= ? AND maxZ <= ?
        ").map_err(|e| e.to_string())?;

        let mut max_iterations = 500_000;

        while let Some(current) = open_set.pop() {
            if current.id64 == dst_id {
                let mut path_ids = vec![dst_id];
                let mut curr_trace = dst_id;

                while let Some(&parent) = came_from.get(&curr_trace) {
                    path_ids.push(parent);
                    curr_trace = parent;
                }
                path_ids.reverse();

                let mut path_stmt = conn.prepare("SELECT s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64=i.id WHERE s.id64=?").unwrap();

                let mut route_json = Vec::with_capacity(path_ids.len());
                let mut prev_x = src_x;
                let mut prev_y = src_y;
                let mut prev_z = src_z;

                for (step_idx, &node_id) in path_ids.iter().enumerate() {
                    let (n_name, n_x, n_y, n_z) = if node_id == src_id {
                        (src_exact_name.clone(), src_x, src_y, src_z)
                    } else if node_id == dst_id {
                        (dst_exact_name.clone(), dst_x, dst_y, dst_z)
                    } else {
                        path_stmt.query_row(rusqlite::params![node_id], |r| {
                            Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?, r.get::<_, f64>(2)?, r.get::<_, f64>(3)?))
                        }).unwrap()
                    };

                    let dist_from_prev = if step_idx == 0 {
                        0.0
                    } else {
                        ((n_x - prev_x).powi(2) + (n_y - prev_y).powi(2) + (n_z - prev_z).powi(2)).sqrt()
                    };

                    route_json.push(serde_json::json!({
                        "system": n_name,
                        "id64": node_id.to_string(),
                        "coords": {"x": n_x, "y": n_y, "z": n_z},
                        "distance_from_prev": (dist_from_prev * 100.0).round() / 100.0,
                        "jumps": step_idx
                    }));

                    prev_x = n_x;
                    prev_y = n_y;
                    prev_z = n_z;
                }

                return Ok(serde_json::json!({
                    "source": source_name,
                    "destination": dest_name,
                    "totalJumps": path_ids.len() - 1,
                    "route": route_json
                }));
            }

            max_iterations -= 1;
            if max_iterations == 0 {
                return Err("Route calculation exceeded maximum internal iteration limit. Try breaking up your journey.".to_string());
            }

            if current.g_score > *g_score_map.get(&current.id64).unwrap_or(&usize::MAX) {
                continue;
            }

            let min_x = current.x - 14.99; let max_x = current.x + 14.99;
            let min_y = current.y - 14.99; let max_y = current.y + 14.99;
            let min_z = current.z - 14.99; let max_z = current.z + 14.99;

            let neighbors = stmt.query_map(rusqlite::params![min_x, max_x, min_y, max_y, min_z, max_z], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, f64>(3)?
                ))
            }).map_err(|e| e.to_string())?;

            for neighbor_res in neighbors {
                if let Ok((n_id, n_x, n_y, n_z)) = neighbor_res {
                    let dist = ((n_x - current.x).powi(2) + (n_y - current.y).powi(2) + (n_z - current.z).powi(2)).sqrt();

                    if dist <= 14.99 {
                        let tentative_g = current.g_score + 1;

                        if tentative_g < *g_score_map.get(&n_id).unwrap_or(&usize::MAX) {
                            came_from.insert(n_id, current.id64);
                            g_score_map.insert(n_id, tentative_g);

                            let h_score = ((dst_x - n_x).powi(2) + (dst_y - n_y).powi(2) + (dst_z - n_z).powi(2)).sqrt() / 14.99;
                            let f_score = tentative_g as f64 + h_score;

                            open_set.push(RouteNode {
                                g_score: tentative_g,
                                f_score,
                                id64: n_id,
                                x: n_x,
                                y: n_y,
                                z: n_z,
                            });
                        }
                    }
                }
            }
        }

        Err("No valid route found connecting these systems within the maximum jump limit of 14.99 ly.".to_string())
    }).await.unwrap();

    match result {
        Ok(json) => Ok(Json(json)),
        Err(e) => {
            let status = if e.contains("not found") { StatusCode::NOT_FOUND } else { StatusCode::BAD_REQUEST };
            Err((status, e))
        }
    }
}

async fn ship_route_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<RouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_ship_route(state, params).await
}

async fn ship_route_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<RouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_ship_route(state, params).await
}

async fn do_carrier_route(
    state: Arc<AppState>,
    params: CarrierRouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;
    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let mut conn = pool.get().map_err(|e| e.to_string())?;

        let get_coords = |sys_input: &str| -> Result<(i64, f64, f64, f64), String> {
            if let Ok(id) = sys_input.parse::<i64>() {
                conn.query_row(
                    "SELECT s.id64, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.id64 = ? LIMIT 1",
                    rusqlite::params![id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?, r.get::<_, f64>(2)?, r.get::<_, f64>(3)?))
                ).map_err(|_| format!("System ID '{}' not found", id))
            } else {
                conn.query_row(
                    "SELECT s.id64, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.name = ? COLLATE NOCASE LIMIT 1",
                    rusqlite::params![sys_input],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?, r.get::<_, f64>(2)?, r.get::<_, f64>(3)?))
                ).map_err(|_| format!("System '{}' not found", sys_input))
            }
        };

        let (src_id, x1, y1, z1) = get_coords(&params.current_system)?;
        let (dest_id, x2, y2, z2) = get_coords(&params.destination)?;

        let total_distance = ((x2 - x1).powi(2) + (y2 - y1).powi(2) + (z2 - z1).powi(2)).sqrt();
        let base_cargo = params.used_cargo;

        let is_squadron = params.is_squadron.unwrap_or(false);
        let carrier_base_mass = if is_squadron { 60000.0 } else { 25000.0 };

        let mut tank = params.tank_fuel;
        let mut market = params.stored_tritium;
        let max_tank_capacity = params.tank_fuel;

        let mut jumps = Vec::new();
        let mut current_pos = (x1, y1, z1);
        let mut current_sys_name = params.current_system.clone();
        let mut current_id = src_id;
        let mut total_fuel_used = 0.0;
        let mut distance_remaining = total_distance;
        let mut loop_counter = 0;

        jumps.push(serde_json::json!({
            "system": current_sys_name,
            "id64": current_id.to_string(),
            "distance_from_start": 0.0,
            "distance_to_destination": (distance_remaining * 100.0).round() / 100.0,
            "jump_distance": 0.0,
            "fuel_used": 0.0,
            "fuel_left_tank": tank,
            "tritium_in_market": market,
            "has_enough_fuel": true
        }));

        let tx = conn.transaction().map_err(|e| e.to_string())?;

        while distance_remaining > 500.0 && loop_counter < 500 {
            loop_counter += 1;

            let v_x = x2 - current_pos.0;
            let v_y = y2 - current_pos.1;
            let v_z = z2 - current_pos.2;
            let v_mag = (v_x.powi(2) + v_y.powi(2) + v_z.powi(2)).sqrt();

            let u_x = v_x / v_mag;
            let u_y = v_y / v_mag;
            let u_z = v_z / v_mag;

            let target_dist = 498.5;
            let t_x = current_pos.0 + u_x * target_dist;
            let t_y = current_pos.1 + u_y * target_dist;
            let t_z = current_pos.2 + u_z * target_dist;

            let mut best_system = String::new();
            let mut best_pos = current_pos;
            let mut best_id = 0;
            let mut min_dist_to_dest = distance_remaining;
            let mut jump_dist = 0.0;
            let mut search_radius = 20.0;
            let mut found_system = false;

            for _ in 0..5 {
                let min_x = t_x - search_radius; let max_x = t_x + search_radius;
                let min_y = t_y - search_radius; let max_y = t_y + search_radius;
                let min_z = t_z - search_radius; let max_z = t_z + search_radius;

                let mut stmt = tx.prepare_cached("
                    SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                    FROM systems_index i JOIN systems s ON i.id = s.id64
                    WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
                ").unwrap();

                let rows = stmt.query_map(rusqlite::params![min_x, max_x, min_y, max_y, min_z, max_z], |row| {
                    Ok((
                        row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, f64>(2)?, row.get::<_, f64>(3)?, row.get::<_, f64>(4)?
                    ))
                }).unwrap();

                for row in rows.filter_map(Result::ok) {
                    let (id, name, cx, cy, cz) = row;
                    let dist_from_current = ((cx - current_pos.0).powi(2) + (cy - current_pos.1).powi(2) + (cz - current_pos.2).powi(2)).sqrt();

                    if dist_from_current <= 500.0 && dist_from_current > 0.0 {
                        let dist_to_dest = ((x2 - cx).powi(2) + (y2 - cy).powi(2) + (z2 - cz).powi(2)).sqrt();
                        if dist_to_dest < min_dist_to_dest {
                            min_dist_to_dest = dist_to_dest;
                            best_system = name;
                            best_pos = (cx, cy, cz);
                            best_id = id;
                            jump_dist = dist_from_current;
                            found_system = true;
                        }
                    }
                }

                if found_system { break; } else { search_radius += 30.0; }
            }

            if !found_system {
                return Err(format!("Route failed: Could not find a star within range after system '{}'.", current_sys_name));
            }

            let c = base_cargo + market;
            let mut r = tank;
            if r < 0.0 { r = 0.0; }

            let jump_fuel = (5.0 + (jump_dist * (c + r + carrier_base_mass)) / 200000.0).ceil();
            total_fuel_used += jump_fuel;

            tank -= jump_fuel;
            let has_enough_fuel = tank >= 0.0;

            let top_off_amount = (max_tank_capacity - tank).max(0.0).min(market);
            tank += top_off_amount;
            market -= top_off_amount;

            current_pos = best_pos;
            current_sys_name = best_system.clone();
            current_id = best_id;
            distance_remaining = min_dist_to_dest;

            let dist_from_start = ((current_pos.0 - x1).powi(2) + (current_pos.1 - y1).powi(2) + (current_pos.2 - z1).powi(2)).sqrt();

            jumps.push(serde_json::json!({
                "system": current_sys_name,
                "id64": current_id.to_string(),
                "distance_from_start": (dist_from_start * 100.0).round() / 100.0,
                "distance_to_destination": (distance_remaining * 100.0).round() / 100.0,
                "jump_distance": (jump_dist * 100.0).round() / 100.0,
                "fuel_used": jump_fuel,
                "fuel_left_tank": tank,
                "tritium_in_market": market,
                "has_enough_fuel": has_enough_fuel
            }));
        }

        if distance_remaining > 0.0 {
            let final_jump_dist = ((x2 - current_pos.0).powi(2) + (y2 - current_pos.1).powi(2) + (z2 - current_pos.2).powi(2)).sqrt();

            let c = base_cargo + market;
            let mut r = tank;
            if r < 0.0 { r = 0.0; }

            let final_jump_fuel = (5.0 + (final_jump_dist * (c + r + carrier_base_mass)) / 200000.0).ceil();
            total_fuel_used += final_jump_fuel;

            tank -= final_jump_fuel;
            let has_enough_fuel = tank >= 0.0;

            let top_off_amount = (max_tank_capacity - tank).max(0.0).min(market);
            tank += top_off_amount;
            market -= top_off_amount;

            jumps.push(serde_json::json!({
                "system": params.destination.clone(),
                "id64": dest_id.to_string(),
                "distance_from_start": (total_distance * 100.0).round() / 100.0,
                "distance_to_destination": 0.0,
                "jump_distance": (final_jump_dist * 100.0).round() / 100.0,
                "fuel_used": final_jump_fuel,
                "fuel_left_tank": tank,
                "tritium_in_market": market,
                "has_enough_fuel": has_enough_fuel
            }));
        }

        Ok(serde_json::json!({
            "source": params.current_system,
            "destination": params.destination,
            "is_squadron": is_squadron,
            "total_distance_ly": (total_distance * 100.0).round() / 100.0,
            "base_cargo_capacity_used": base_cargo,
            "initial_fuel_tank": params.tank_fuel,
            "initial_market_tritium": params.stored_tritium,
            "total_fuel_used": total_fuel_used,
            "final_fuel_tank": tank,
            "final_market_tritium": market,
            "totalJumps": jumps.len() - 1,
            "route": jumps
        }))
    }).await.unwrap().map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    Ok(Json(result))
}

async fn carrier_route_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<CarrierRouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_carrier_route(state, params).await
}

// =============================================================================
// UNIFIED LAZY A* NEUTRON ROUTER (Spansh-Killer)
// =============================================================================
async fn do_neutron_route(
    state: Arc<AppState>,
    params: NeutronRouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;
    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let t_start = Instant::now();

        let get_sys = |input: &str| -> Result<(i64, String, f64, f64, f64), String> {
            if let Ok(id) = input.parse::<i64>() {
                conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64=i.id WHERE s.id64=? LIMIT 1",
                    rusqlite::params![id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                ).map_err(|_| format!("System ID '{}' not found", input))
            } else {
                conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64=i.id WHERE s.name=? COLLATE NOCASE LIMIT 1",
                    rusqlite::params![input], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                ).map_err(|_| format!("System '{}' not found", input))
            }
        };

        let (src_id, src_name, x1, y1, z1) = get_sys(&params.source)?;
        let (dst_id, dst_name, x2, y2, z2) = get_sys(&params.destination)?;

        let total_distance = ((x2-x1).powi(2)+(y2-y1).powi(2)+(z2-z1).powi(2)).sqrt();
        let multiplier = if params.supercharge_type.to_lowercase() == "caspian" { 6.0 } else { 4.0 };
        let boosted_range = params.range * multiplier;

        info!("Neutron route: {} -> {}, {:.0} LY, boosted {:.1} LY, base {:.2} LY",
            params.source, params.destination, total_distance, boosted_range, params.range);

        // --- 1. Preload Neutron Cylinder ---
        let mut all_node_pos: HashMap<i64, (f64, f64, f64)> = HashMap::new();
        let mut all_node_name: HashMap<i64, String> = HashMap::new();
        let mut all_node_is_neutron: HashMap<i64, bool> = HashMap::new();

        let cylinder_radius = (boosted_range * 2.5).clamp(2000.0, 5000.0);
        let buf = cylinder_radius;

        let mut stmt = conn.prepare("
            SELECT s.id64, s.name, i.minX, i.minY, i.minZ
            FROM neutron_systems ns
            JOIN systems_index i ON ns.systemId64 = i.id
            JOIN systems s ON ns.systemId64 = s.id64
            WHERE i.minX BETWEEN ? AND ?
              AND i.minY BETWEEN ? AND ?
              AND i.minZ BETWEEN ? AND ?
        ").map_err(|e| e.to_string())?;

        let dv = (x2 - x1, y2 - y1, z2 - z1);
        let dv_len_sq = dv.0*dv.0 + dv.1*dv.1 + dv.2*dv.2;

        let rows: Vec<(i64, String, f64, f64, f64)> = stmt.query_map(
            rusqlite::params![
                x1.min(x2) - buf, x1.max(x2) + buf,
                y1.min(y2) - buf, y1.max(y2) + buf,
                z1.min(z2) - buf, z1.max(z2) + buf,
            ],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        ).map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .filter(|&(_, _, nx, ny, nz): &(i64, String, f64, f64, f64)| {
            if dv_len_sq < 1.0 { return true; }
            let w = (nx - x1, ny - y1, nz - z1);
            let t = ((w.0*dv.0 + w.1*dv.1 + w.2*dv.2) / dv_len_sq).clamp(0.0, 1.0);
            let (px, py, pz) = (x1 + t*dv.0, y1 + t*dv.1, z1 + t*dv.2);
            (nx-px).powi(2) + (ny-py).powi(2) + (nz-pz).powi(2) <= buf*buf
        }).collect();

        for (id, name, nx, ny, nz) in rows {
            all_node_pos.insert(id, (nx, ny, nz));
            all_node_name.insert(id, name);
            all_node_is_neutron.insert(id, true);
        }

        // --- 2. Build Optimal Entry & Exit Bridges (BFS for normal stars) ---
        let mut build_bridge = |start_id: i64, start_pos: (f64,f64,f64)| {
            let mut open_set = std::collections::VecDeque::new();
            let mut visited = HashSet::new();
            let mut found_n = 0;

            open_set.push_back((start_id, "".to_string(), start_pos.0, start_pos.1, start_pos.2, 0));
            visited.insert(start_id);

            let mut normal_stmt = conn.prepare_cached("
                SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                FROM systems_index i JOIN systems s ON i.id = s.id64
                WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
            ").unwrap();

            while let Some((id, name, cx, cy, cz, jumps)) = open_set.pop_front() {
                if id != start_id {
                    all_node_pos.insert(id, (cx, cy, cz));
                    all_node_name.insert(id, name);
                    all_node_is_neutron.entry(id).or_insert(false);
                }

                if all_node_is_neutron.get(&id).copied().unwrap_or(false) {
                    found_n += 1;
                    if found_n >= 5 { break; }
                }

                if jumps >= 8 { continue; } // max normal jumps boundary
                if visited.len() > 25000 { break; } // safety kill-switch

                let rows = normal_stmt.query_map(rusqlite::params![
                    cx - params.range, cx + params.range,
                    cy - params.range, cy + params.range,
                    cz - params.range, cz + params.range,
                ], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, f64>(2)?, row.get::<_, f64>(3)?, row.get::<_, f64>(4)?))).unwrap();

                for r in rows.filter_map(Result::ok) {
                    let (nid, nname, nx, ny, nz) = r;
                    if visited.contains(&nid) { continue; }
                    let dist = ((nx-cx).powi(2) + (ny-cy).powi(2) + (nz-cz).powi(2)).sqrt();
                    if dist <= params.range && dist > 0.0 {
                        visited.insert(nid);
                        open_set.push_back((nid, nname, nx, ny, nz, jumps + 1));
                    }
                }
            }
        };

        build_bridge(src_id, (x1, y1, z1));
        build_bridge(dst_id, (x2, y2, z2));

        all_node_pos.insert(src_id, (x1, y1, z1));
        all_node_name.insert(src_id, src_name.clone());
        all_node_is_neutron.entry(src_id).or_insert(false);

        all_node_pos.insert(dst_id, (x2, y2, z2));
        all_node_name.insert(dst_id, dst_name.clone());
        all_node_is_neutron.entry(dst_id).or_insert(false);

        // --- 3. Build Unified Spatial Grid ---
        let cell_size = boosted_range.max(50.0);
        let mut grid: HashMap<(i32,i32,i32), Vec<i64>> = HashMap::new();
        for (&nid, &(nx, ny, nz)) in &all_node_pos {
            let cell = ((nx/cell_size) as i32, (ny/cell_size) as i32, (nz/cell_size) as i32);
            grid.entry(cell).or_default().push(nid);
        }

        // --- 4. Unified Weighted A* ---
        #[derive(Clone)]
        struct ANode { g: u32, f: f64, id: i64 }
        impl PartialEq for ANode { fn eq(&self, o: &Self) -> bool { self.id == o.id } }
        impl Eq for ANode {}
        impl PartialOrd for ANode { fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) } }
        impl Ord for ANode { fn cmp(&self, o: &Self) -> Ordering { o.f.partial_cmp(&self.f).unwrap_or(Ordering::Equal) } }

        let mut open_set: BinaryHeap<ANode> = BinaryHeap::new();
        let mut g_score: HashMap<i64, u32> = HashMap::new();
        let mut came_from: HashMap<i64, i64> = HashMap::new();
        let mut closed: HashSet<i64> = HashSet::new();

        let h_fn = |x: f64, y: f64, z: f64| -> f64 {
            ((x-x2).powi(2)+(y-y2).powi(2)+(z-z2).powi(2)).sqrt() / boosted_range
        };
        let weight = 1.05_f64; // The 5% overestimation prioritizes destination laser-focus

        g_score.insert(src_id, 0);
        open_set.push(ANode { g: 0, f: weight * h_fn(x1, y1, z1), id: src_id });

        let mut final_path = None;

        while let Some(ANode { g, id, .. }) = open_set.pop() {
            if id == dst_id {
                let mut path = vec![dst_id];
                let mut cur = dst_id;
                while cur != src_id {
                    match came_from.get(&cur) {
                        Some(&p) => { cur = p; path.push(cur); }
                        None => break,
                    }
                }
                path.reverse();
                final_path = Some(path);
                break;
            }

            if closed.contains(&id) { continue; }
            closed.insert(id);

            let (cx, cy, cz) = all_node_pos[&id];
            let is_n = all_node_is_neutron[&id];
            let jump_range = if is_n { boosted_range } else { params.range };
            let rsq = jump_range * jump_range;

            // Direct connection to destination bypass check
            let d_dst = ((cx-x2).powi(2)+(cy-y2).powi(2)+(cz-z2).powi(2)).sqrt();
            if d_dst <= jump_range && !closed.contains(&dst_id) {
                let tg = g + 1;
                if tg < *g_score.get(&dst_id).unwrap_or(&u32::MAX) {
                    g_score.insert(dst_id, tg);
                    came_from.insert(dst_id, id);
                    open_set.push(ANode { g: tg, f: tg as f64, id: dst_id });
                }
            }

            let (bx, by, bz) = ((cx/cell_size) as i32, (cy/cell_size) as i32, (cz/cell_size) as i32);
            for dx in -1..=1 {
                for dy in -1..=1 {
                    for dz in -1..=1 {
                        if let Some(v) = grid.get(&(bx+dx, by+dy, bz+dz)) {
                            for &n_id in v {
                                if n_id == id || closed.contains(&n_id) { continue; }
                                let (nx, ny, nz) = all_node_pos[&n_id];
                                let d2 = (nx-cx).powi(2)+(ny-cy).powi(2)+(nz-cz).powi(2);
                                if d2 <= rsq && d2 > 0.0 {
                                    let tg = g + 1;
                                    if tg < *g_score.get(&n_id).unwrap_or(&u32::MAX) {
                                        g_score.insert(n_id, tg);
                                        came_from.insert(n_id, id);
                                        open_set.push(ANode { g: tg, f: tg as f64 + weight * h_fn(nx, ny, nz), id: n_id });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let path_ids = final_path.ok_or_else(|| "Could not find a route connecting these systems.".to_string())?;

        info!("A* Unified Route found: {} jumps in {}ms", path_ids.len() - 1, t_start.elapsed().as_millis());

        // --- 5. Build JSON Response ---
        let mut route_json: Vec<serde_json::Value> = Vec::with_capacity(path_ids.len());
        let mut dist_from_start = 0.0f64;

        for step in 0..path_ids.len() {
            let nid = path_ids[step];
            let (nname, nx, ny, nz, is_n) = if nid == src_id {
                (src_name.clone(), x1, y1, z1, all_node_is_neutron.get(&src_id).copied().unwrap_or(false))
            } else if nid == dst_id {
                (dst_name.clone(), x2, y2, z2, all_node_is_neutron.get(&dst_id).copied().unwrap_or(false))
            } else if let Some(&(cnx, cny, cnz)) = all_node_pos.get(&nid) {
                let nm = all_node_name.get(&nid).cloned().unwrap_or_default();
                let isn = all_node_is_neutron.get(&nid).copied().unwrap_or(false);
                (nm, cnx, cny, cnz, isn)
            } else { continue; };

            let d_dest = ((nx-x2).powi(2)+(ny-y2).powi(2)+(nz-z2).powi(2)).sqrt();

            let (jdist, used_boost) = if step + 1 < path_ids.len() {
                let next_id = path_ids[step + 1];
                let (nx_next, ny_next, nz_next) = if next_id == dst_id {
                    (x2, y2, z2)
                } else if let Some(&(nnx, nny, nnz)) = all_node_pos.get(&next_id) {
                    (nnx, nny, nnz)
                } else {
                    (nx, ny, nz)
                };
                let dist = ((nx_next - nx).powi(2) + (ny_next - ny).powi(2) + (nz_next - nz).powi(2)).sqrt();
                (dist, is_n)
            } else {
                (0.0, false)
            };

            route_json.push(serde_json::json!({
                "system":                  nname,
                "id64":                    nid.to_string(),
                "distance_from_start":     (dist_from_start*100.0).round()/100.0,
                "distance_to_destination": (d_dest*100.0).round()/100.0,
                "jump_distance":           (jdist*100.0).round()/100.0,
                "used_neutron_boost":      used_boost,
                "is_neutron":              is_n,
            }));

            dist_from_start += jdist;
        }

        Ok(serde_json::json!({
            "source":            params.source,
            "destination":       params.destination,
            "total_distance_ly": (total_distance*100.0).round()/100.0,
            "totalJumps":        route_json.len().saturating_sub(1),
            "optimised":         true,
            "route":             route_json,
        }))
    }).await.unwrap().map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    Ok(Json(result))
}

async fn neutron_route_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<NeutronRouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_neutron_route(state, params).await
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let skip_download = std::env::args().any(|a| a == "--skip-download");
    if skip_download {
        info!("Starting EDSM-Cube-RS (--skip-download: initial galaxy sync will be skipped)...");
    } else {
        info!("Starting EDSM-Cube-RS on Pi 5...");
    }

    let pool = setup_db_pool();
    let conn = pool.get().unwrap();
    init_db(&conn).unwrap();
    drop(conn);

    tokio::spawn(async move { sync_manager(skip_download).await; });

    let app_state = Arc::new(AppState {
        db_pool: pool,
        query_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_QUERIES)),
        carrier_cache: Mutex::new(CarrierCache { data: None, expires_at: 0 }),
    });

    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/api/system", get(get_system))
        .route("/api/system/bodies", get(get_system_bodies))
        .route("/api/bodies", get(get_system_bodies))
        .route("/api/system/stations", get(get_system_stations))
        .route("/api/stations", get(get_system_stations))
        .route("/api/cube-search", get(cube_search_get).post(cube_search_post))
        .route("/api/route", get(ship_route_get).post(ship_route_post))
        .route("/api/carrier-route", post(carrier_route_post))
        .route("/api/neutron-route", post(neutron_route_post))
        .route("/api/galtea-progression", get(get_carrier_progression))
        .layer(cors)
        .with_state(app_state);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).await.unwrap();
    info!("Server listening on port {}", PORT);
    axum::serve(listener, app).await.unwrap();
}
