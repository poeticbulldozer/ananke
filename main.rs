#![recursion_limit = "256"]
use axum::{
    extract::{Query, State},
    http::{StatusCode, HeaderMap, header},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use crossbeam_channel::{bounded, Receiver, Sender};
use flate2::read::{GzDecoder, ZlibDecoder};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, Result as SqliteResult};
use serde::Deserialize;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fs::{self, File},
    io::{BufReader, Read, Write},
    sync::{Arc, atomic::{AtomicU64, Ordering as AtomicOrdering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Semaphore};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};

// --- CONFIGURATION ---
const DB_FILE: &str = "edsm_cube.db";
const PORT: u16 = 8000;
const URL_SYSTEMS_1DAY: &str = "https://downloads.spansh.co.uk/galaxy_1day.json.gz";
const FILE_SYSTEMS_1DAY: &str = "galaxy_1day.json.gz";
const FILE_SYSTEMS_DOWNLOADING: &str = "galaxy_1day.json.gz.downloading";
const SYNC_INTERVAL_SECONDS: u64 = 21600; // 6 hours (overlaps with 1-day dump window to avoid gaps)
const MAX_CONCURRENT_QUERIES: usize = 6;
const MAX_CONCURRENT_ASTAR: usize = 2; // Heavy A* requests queue behind this to protect the Pi
const SHIP_ROUTE_BUDGET_MS: u128 = 120_000; // 2 minutes for ship A*
// EDMC API Key — set via ANANKE_EDMC_KEY env var, or empty string to disable auth
const EDMC_KEY_ENV: &str = "ANANKE_EDMC_KEY";

// --- EDDN (Elite Dangerous Data Network) live feed ---
// Public ZeroMQ relay; broadcasts every player's journal/market/etc. events
// in real time. Lets us keep the DB warm between Spansh daily dumps so we
// don't have to hit Spansh as hard.
const EDDN_RELAY_URL: &str = "tcp://eddn.edcd.io:9500";
const EDDN_RELAY_ENV: &str = "ANANKE_EDDN_RELAY";        // override the URL
const EDDN_DISABLE_ENV: &str = "ANANKE_EDDN_DISABLE";    // any non-empty value disables EDDN
const EDDN_RECV_TIMEOUT_MS: i32 = 60_000;                // reconnect if 60s of silence
const EDDN_RECONNECT_BASE_MS: u64 = 1_000;               // 1s initial backoff
const EDDN_RECONNECT_MAX_MS: u64 = 60_000;               // cap at 60s
const EDDN_FLUSH_INTERVAL_MS: u64 = 1_000;               // flush listener buffer every 1s
const EDDN_FLUSH_BATCH_SIZE: usize = 200;                // or every 200 systems, whichever first

// --- Commander hotspot heatmap ---
// 2D grid of atomic counters, bumped once per system-arrival event from
// EDDN or EDMC. Render-on-demand to a PNG with 30s cache. A background
// thread decays the grid every 5 minutes so old activity fades out.
const HEATMAP_X_MIN: f64 = -50_000.0;
const HEATMAP_X_MAX: f64 =  50_000.0;
const HEATMAP_Z_MIN: f64 = -25_000.0;
const HEATMAP_Z_MAX: f64 =  75_000.0;
const HEATMAP_W: usize = 1024;
const HEATMAP_H: usize = 1024;
const HEATMAP_DECAY_INTERVAL_SECS: u64 = 300;        // decay every 5 min
const HEATMAP_DECAY_FACTOR: f64       = 0.9928057;   // ≈8 hour half-life
const HEATMAP_RENDER_CACHE_SECS: u64  = 30;          // re-render at most every 30s

// --- STATE ---
#[allow(dead_code)]
struct AppState {
    db_pool: Pool<SqliteConnectionManager>,
    query_semaphore: Arc<Semaphore>,
    astar_semaphore: Arc<Semaphore>,
    carrier_cache: Mutex<CarrierCache>,
    edmc_sender: Sender<Vec<SpanshSystem>>,
    edmc_api_key: Option<String>,
    edmc_stats: Arc<EdmcStats>,
    eddn_stats: Arc<EddnStats>,
    heatmap: Arc<Heatmap>,
}

#[allow(dead_code)]
struct CarrierCache {
    data: Option<serde_json::Value>,
    expires_at: u64,
}

/// Live ingest counters for EDMC
struct EdmcStats {
    systems_ingested: AtomicU64,
    bodies_ingested: AtomicU64,
    stations_ingested: AtomicU64,
    last_ingest_time: AtomicU64,
}

/// Live ingest counters for EDDN (Elite Dangerous Data Network) listener.
/// Mirrors EdmcStats so /api/edmc/stats can surface both feeds.
struct EddnStats {
    /// Raw ZMQ frames received from the relay.
    messages_received: AtomicU64,
    /// Frames that survived zlib decompression + JSON parse + dispatch.
    messages_processed: AtomicU64,
    /// Frames dropped due to decompression / parse / unknown-schema.
    messages_dropped: AtomicU64,
    /// SpanshSystem records emitted to the writer channel.
    systems_emitted: AtomicU64,
    bodies_emitted: AtomicU64,
    stations_emitted: AtomicU64,
    /// Unix seconds of the most recently processed message.
    last_message_time: AtomicU64,
    /// How many times we've reconnected to the relay (a non-zero value here
    /// after startup tells you the relay or our network blipped).
    reconnects: AtomicU64,
    /// 1 if the listener is currently connected, 0 otherwise.
    connected: AtomicU64,
}

// --- MODELS ---
#[derive(Deserialize, Debug, Clone)]
struct SpanshCoords { x: f64, y: f64, z: f64 }

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct SpanshSystem {
    id64: i64,
    name: String,
    #[serde(default)]
    population: Option<i64>,
    #[serde(default)]
    coords: Option<SpanshCoords>,
    #[serde(default)]
    bodies: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    stations: Option<Vec<serde_json::Value>>,
    // System-level metadata from Spansh galaxy schema
    #[serde(default)]
    allegiance: Option<String>,
    #[serde(default)]
    government: Option<String>,
    #[serde(default, rename = "primaryEconomy")]
    primary_economy: Option<String>,
    #[serde(default, rename = "secondaryEconomy")]
    secondary_economy: Option<String>,
    #[serde(default)]
    security: Option<String>,
    #[serde(default, rename = "bodyCount")]
    body_count: Option<i64>,
    #[serde(default)]
    date: Option<String>,
    #[serde(default, rename = "controllingFaction")]
    controlling_faction: Option<serde_json::Value>,
    #[serde(default)]
    factions: Option<serde_json::Value>,
    #[serde(default, rename = "powerState")]
    power_state: Option<String>,
    #[serde(default)]
    powers: Option<serde_json::Value>,
    #[serde(default, rename = "controllingPower")]
    controlling_power: Option<String>,
    #[serde(default, rename = "powerStateControlProgress")]
    power_state_control_progress: Option<f64>,
    #[serde(default, rename = "powerStateReinforcement")]
    power_state_reinforcement: Option<f64>,
    #[serde(default, rename = "powerStateUndermining")]
    power_state_undermining: Option<f64>,
    #[serde(default, rename = "powerConflictProgress")]
    power_conflict_progress: Option<serde_json::Value>,
    #[serde(default, rename = "thargoidWar")]
    thargoid_war: Option<serde_json::Value>,
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
    /// Allows lookup by id64 (used by coloslots.py fetch_ananke_*_by_id64)
    id64: Option<i64>,
}

// Ship Router Query
#[derive(Deserialize)]
struct RouteQuery {
    source: String,
    destination: String,
}

// Nearest Station Search Query
// All filter fields are optional; omitting them means "any".
#[derive(Deserialize)]
struct NearestStationQuery {
    /// Reference system name (required). The search radiates outward from here.
    #[serde(rename = "refSystem", alias = "ref_system", alias = "system")]
    ref_system: String,
    /// Search radius in light-years (default: 50, max: 500).
    radius: Option<f64>,
    /// Maximum number of results to return (default: 25, max: 100).
    limit: Option<usize>,

    // --- System-level filters ---
    /// e.g. "Alliance", "Empire", "Federation", "Independent"
    allegiance: Option<String>,
    /// e.g. "Democracy", "Corporate", "Anarchy" …
    government: Option<String>,
    /// e.g. "High Tech", "Industrial", "Extraction" …
    /// Matched against the station's primary economy column.
    economy: Option<String>,

    // --- Station-level filters ---
    /// Station type string, e.g. "Coriolis Starport", "Outpost", "Planetary Port" …
    #[serde(rename = "stationType", alias = "station_type")]
    station_type: Option<String>,
    /// Minimum landing pad size: "Small" | "Medium" | "Large"
    #[serde(rename = "minLandingPad", alias = "min_landing_pad")]
    min_landing_pad: Option<String>,
    /// Maximum distance from star in light-seconds (station distanceToArrival).
    /// Common UI values: 500, 1000, 2500, 5000, 10000 – or null for "Any".
    #[serde(rename = "maxStationDistance", alias = "max_station_distance")]
    max_station_distance: Option<f64>,
    /// Include surface stations (Planetary Port / Outpost / Settlement)?
    /// Defaults to false (space-based only, matching the Spansh UI default).
    #[serde(rename = "useSurfaceStations", alias = "use_surface_stations", default)]
    use_surface_stations: bool,
    /// Exclude fleet carriers (Drake-Class Carrier) from results?
    /// Defaults to true (matches "Ignore fleet carriers" button in the UI).
    #[serde(rename = "ignoreFleetCarriers", alias = "ignore_fleet_carriers")]
    ignore_fleet_carriers: Option<bool>,
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
    engine: Option<String>, // "greedy" or "astar" (default: greedy)
}

// Neutron Router Query
#[derive(Deserialize)]
struct NeutronRouteQuery {
    source: String,
    destination: String,
    range: f64,
    supercharge_type: String,
    engine: Option<String>, // "greedy" or "astar" (default: astar)
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

#[derive(Deserialize)]
struct DistanceQuery {
    #[serde(rename = "systemA", alias = "system_a", alias = "from")]
    system_a: String,
    #[serde(rename = "systemB", alias = "system_b", alias = "to")]
    system_b: String,
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
        CREATE TABLE IF NOT EXISTS systems (id64 INTEGER PRIMARY KEY, name TEXT, population INTEGER, last_update INTEGER, allegiance TEXT, government TEXT, primaryEconomy TEXT, secondaryEconomy TEXT, security TEXT, bodyCount INTEGER, date TEXT, controllingFaction TEXT, factions TEXT, powerState TEXT, powers TEXT, controllingPower TEXT, powerStateControlProgress REAL, powerStateReinforcement REAL, powerStateUndermining REAL, powerConflictProgress TEXT, thargoidWar TEXT);
        CREATE INDEX IF NOT EXISTS idx_systems_name_nocase ON systems(name COLLATE NOCASE);
        CREATE VIRTUAL TABLE IF NOT EXISTS systems_index USING rtree(id, minX, maxX, minY, maxY, minZ, maxZ);
        CREATE TABLE IF NOT EXISTS bodies (systemId64 INTEGER, bodyId INTEGER, name TEXT, type TEXT, subType TEXT, distanceToArrival REAL, isLandable INTEGER, gravity REAL, earthMasses REAL, radius REAL, surfaceTemperature INTEGER, orbitalPeriod REAL, semiMajorAxis REAL, orbitalEccentricity REAL, orbitalInclination REAL, argOfPeriapsis REAL, rotationalPeriod REAL, isTidallyLocked INTEGER, axisTilt REAL, volcanismType TEXT, atmosphereType TEXT, terraformingState TEXT, stellarMass REAL, absoluteMagnitude REAL, age INTEGER, luminosity TEXT, subclass INTEGER, surfacePressure REAL, atmosphereComposition TEXT, composition TEXT, rings TEXT, parents TEXT, wasDiscovered INTEGER, wasMapped INTEGER, ascendingNode REAL, meanAnomaly REAL, signals TEXT, bodyId64 INTEGER, mainStar INTEGER, spectralClass TEXT, solarRadius REAL, materials TEXT, reserveLevel TEXT, belts TEXT, updateTime TEXT, PRIMARY KEY (systemId64, bodyId)) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS stations (id INTEGER, marketId INTEGER, systemId64 INTEGER, name TEXT, type TEXT, distanceToArrival REAL, allegiance TEXT, government TEXT, economy TEXT, secondEconomy TEXT, haveMarket INTEGER, haveShipyard INTEGER, haveOutfitting INTEGER, otherServices TEXT, updateTime TEXT, realName TEXT, carrierName TEXT, controllingFaction TEXT, controllingFactionState TEXT, state TEXT, latitude REAL, longitude REAL, landingPads TEXT, carrierDockingAccess TEXT, economies TEXT, market TEXT, shipyard TEXT, outfitting TEXT, PRIMARY KEY (systemId64, id)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS neutron_systems (systemId64 INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS prison_systems  (systemId64 INTEGER PRIMARY KEY);
    ")?;

    // Migration: add new columns to existing tables (safe to repeat)
    let sys_new_cols = [
        ("allegiance", "TEXT"), ("government", "TEXT"), ("primaryEconomy", "TEXT"),
        ("secondaryEconomy", "TEXT"), ("security", "TEXT"), ("bodyCount", "INTEGER"),
        ("date", "TEXT"), ("controllingFaction", "TEXT"), ("factions", "TEXT"),
        ("powerState", "TEXT"), ("powers", "TEXT"), ("controllingPower", "TEXT"),
        ("powerStateControlProgress", "REAL"), ("powerStateReinforcement", "REAL"),
        ("powerStateUndermining", "REAL"), ("powerConflictProgress", "TEXT"),
        ("thargoidWar", "TEXT"),
    ];
    for (col, typ) in &sys_new_cols {
        let _ = conn.execute(&format!("ALTER TABLE systems ADD COLUMN {} {}", col, typ), []);
    }

    let body_new_cols = [
        ("stellarMass", "REAL"), ("absoluteMagnitude", "REAL"), ("age", "INTEGER"),
        ("luminosity", "TEXT"), ("subclass", "INTEGER"), ("surfacePressure", "REAL"),
        ("atmosphereComposition", "TEXT"), ("composition", "TEXT"), ("rings", "TEXT"),
        ("parents", "TEXT"), ("wasDiscovered", "INTEGER"), ("wasMapped", "INTEGER"),
        ("ascendingNode", "REAL"), ("meanAnomaly", "REAL"),
        ("signals", "TEXT"),
        // Schema-complete fields
        ("bodyId64", "INTEGER"), ("mainStar", "INTEGER"), ("spectralClass", "TEXT"),
        ("solarRadius", "REAL"), ("materials", "TEXT"), ("reserveLevel", "TEXT"),
        ("belts", "TEXT"), ("updateTime", "TEXT"),
    ];
    for (col, typ) in &body_new_cols {
        let _ = conn.execute(&format!("ALTER TABLE bodies ADD COLUMN {} {}", col, typ), []);
    }

    let station_new_cols = [
        ("realName", "TEXT"), ("carrierName", "TEXT"), ("controllingFaction", "TEXT"),
        ("controllingFactionState", "TEXT"), ("state", "TEXT"),
        ("latitude", "REAL"), ("longitude", "REAL"), ("landingPads", "TEXT"),
        ("carrierDockingAccess", "TEXT"), ("economies", "TEXT"),
        ("market", "TEXT"), ("shipyard", "TEXT"), ("outfitting", "TEXT"),
    ];
    for (col, typ) in &station_new_cols {
        let _ = conn.execute(&format!("ALTER TABLE stations ADD COLUMN {} {}", col, typ), []);
    }
    Ok(())
}

// --- BACKGROUND SYNC MANAGER ---
fn current_time_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn download_file(url: &str, target: &str) -> bool {
    // Always re-download — the old file is stale from the previous sync cycle.
    // Download to a temp file first, then atomic rename so we never leave a
    // half-written dump that `process_systems_dump` would choke on.
    let tmp = FILE_SYSTEMS_DOWNLOADING;

    // Clean up any leftover partial download from a previous crash
    let _ = fs::remove_file(tmp);

    info!("Downloading {} -> {} ...", url, target);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(7200)) // 2h max for huge dumps
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
    let mut buf = [0u8; 256 * 1024]; // 256 KB read buffer
    let mut last_log = Instant::now();

    loop {
        let n = match std::io::Read::read(&mut resp, &mut buf) {
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

        // Log progress every 15 seconds
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

    // Atomic swap: remove old dump, rename temp to target
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

fn get_i64(v: &serde_json::Value, k: &str) -> Option<i64> { v.get(k).and_then(|x| x.as_i64()) }
fn get_f64(v: &serde_json::Value, k: &str) -> Option<f64> { v.get(k).and_then(|x| x.as_f64().or_else(|| x.as_i64().map(|i| i as f64))) }
fn get_str<'a>(v: &'a serde_json::Value, k: &str) -> Option<&'a str> { v.get(k).and_then(|x| x.as_str()) }
fn get_bool(v: &serde_json::Value, k: &str) -> i32 { v.get(k).and_then(|x| x.as_bool()).map(|x| if x { 1 } else { 0 }).unwrap_or(0) }
fn get_bool_opt(v: &serde_json::Value, k: &str) -> Option<i32> {
    v.get(k).and_then(|x| x.as_bool().map(|b| if b { 1 } else { 0 }).or_else(|| x.as_i64().map(|i| i as i32)))
}

fn db_writer_worker(receiver: Receiver<Vec<SpanshSystem>>) {
    let mut conn = Connection::open(DB_FILE).unwrap();
    conn.execute_batch("
        PRAGMA synchronous = OFF;
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 30000;
    ").unwrap();

    while let Ok(batch) = receiver.recv() {
        let mut attempts = 0u64;
        loop {
            attempts += 1;
            let result = (|| -> rusqlite::Result<()> {
                let tx = conn.transaction()?;
                {
                    // UPSERT with COALESCE: a partial record (e.g. an EDDN Scan event
                    // with only id64+name set) won't blow away rich metadata that an
                    // earlier FSDJump or the Spansh dump already wrote. `population`
                    // gets a CASE check because the param is unwrap_or(0)'d below to
                    // satisfy readers that expect a non-null i64.
                    let mut stmt_sys = tx.prepare_cached("INSERT INTO systems (id64, name, population, last_update, allegiance, government, primaryEconomy, secondaryEconomy, security, bodyCount, date, controllingFaction, factions, powerState, powers, controllingPower, powerStateControlProgress, powerStateReinforcement, powerStateUndermining, powerConflictProgress, thargoidWar) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id64) DO UPDATE SET name = COALESCE(excluded.name, systems.name), population = CASE WHEN excluded.population > 0 THEN excluded.population ELSE systems.population END, last_update = excluded.last_update, allegiance = COALESCE(excluded.allegiance, systems.allegiance), government = COALESCE(excluded.government, systems.government), primaryEconomy = COALESCE(excluded.primaryEconomy, systems.primaryEconomy), secondaryEconomy = COALESCE(excluded.secondaryEconomy, systems.secondaryEconomy), security = COALESCE(excluded.security, systems.security), bodyCount = COALESCE(excluded.bodyCount, systems.bodyCount), date = COALESCE(excluded.date, systems.date), controllingFaction = COALESCE(excluded.controllingFaction, systems.controllingFaction), factions = COALESCE(excluded.factions, systems.factions), powerState = COALESCE(excluded.powerState, systems.powerState), powers = COALESCE(excluded.powers, systems.powers), controllingPower = COALESCE(excluded.controllingPower, systems.controllingPower), powerStateControlProgress = COALESCE(excluded.powerStateControlProgress, systems.powerStateControlProgress), powerStateReinforcement = COALESCE(excluded.powerStateReinforcement, systems.powerStateReinforcement), powerStateUndermining = COALESCE(excluded.powerStateUndermining, systems.powerStateUndermining), powerConflictProgress = COALESCE(excluded.powerConflictProgress, systems.powerConflictProgress), thargoidWar = COALESCE(excluded.thargoidWar, systems.thargoidWar)")?;
                    let mut stmt_idx = tx.prepare_cached("INSERT OR REPLACE INTO systems_index (id, minX, maxX, minY, maxY, minZ, maxZ) VALUES (?, ?, ?, ?, ?, ?, ?)")?;
                    let mut stmt_bodies = tx.prepare_cached("INSERT INTO bodies (systemId64, bodyId, name, type, subType, distanceToArrival, isLandable, gravity, earthMasses, radius, surfaceTemperature, orbitalPeriod, semiMajorAxis, orbitalEccentricity, orbitalInclination, argOfPeriapsis, rotationalPeriod, isTidallyLocked, axisTilt, volcanismType, atmosphereType, terraformingState, stellarMass, absoluteMagnitude, age, luminosity, subclass, surfacePressure, atmosphereComposition, composition, rings, parents, wasDiscovered, wasMapped, ascendingNode, meanAnomaly, signals, bodyId64, mainStar, spectralClass, solarRadius, materials, reserveLevel, belts, updateTime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(systemId64, bodyId) DO UPDATE SET name = COALESCE(excluded.name, bodies.name), type = COALESCE(excluded.type, bodies.type), subType = COALESCE(excluded.subType, bodies.subType), distanceToArrival = COALESCE(excluded.distanceToArrival, bodies.distanceToArrival), isLandable = COALESCE(excluded.isLandable, bodies.isLandable), gravity = COALESCE(excluded.gravity, bodies.gravity), earthMasses = COALESCE(excluded.earthMasses, bodies.earthMasses), radius = COALESCE(excluded.radius, bodies.radius), surfaceTemperature = COALESCE(excluded.surfaceTemperature, bodies.surfaceTemperature), orbitalPeriod = COALESCE(excluded.orbitalPeriod, bodies.orbitalPeriod), semiMajorAxis = COALESCE(excluded.semiMajorAxis, bodies.semiMajorAxis), orbitalEccentricity = COALESCE(excluded.orbitalEccentricity, bodies.orbitalEccentricity), orbitalInclination = COALESCE(excluded.orbitalInclination, bodies.orbitalInclination), argOfPeriapsis = COALESCE(excluded.argOfPeriapsis, bodies.argOfPeriapsis), rotationalPeriod = COALESCE(excluded.rotationalPeriod, bodies.rotationalPeriod), isTidallyLocked = COALESCE(excluded.isTidallyLocked, bodies.isTidallyLocked), axisTilt = COALESCE(excluded.axisTilt, bodies.axisTilt), volcanismType = COALESCE(excluded.volcanismType, bodies.volcanismType), atmosphereType = COALESCE(excluded.atmosphereType, bodies.atmosphereType), terraformingState = COALESCE(excluded.terraformingState, bodies.terraformingState), stellarMass = COALESCE(excluded.stellarMass, bodies.stellarMass), absoluteMagnitude = COALESCE(excluded.absoluteMagnitude, bodies.absoluteMagnitude), age = COALESCE(excluded.age, bodies.age), luminosity = COALESCE(excluded.luminosity, bodies.luminosity), subclass = COALESCE(excluded.subclass, bodies.subclass), surfacePressure = COALESCE(excluded.surfacePressure, bodies.surfacePressure), atmosphereComposition = COALESCE(excluded.atmosphereComposition, bodies.atmosphereComposition), composition = COALESCE(excluded.composition, bodies.composition), rings = COALESCE(excluded.rings, bodies.rings), parents = COALESCE(excluded.parents, bodies.parents), wasDiscovered = COALESCE(excluded.wasDiscovered, bodies.wasDiscovered), wasMapped = COALESCE(excluded.wasMapped, bodies.wasMapped), ascendingNode = COALESCE(excluded.ascendingNode, bodies.ascendingNode), meanAnomaly = COALESCE(excluded.meanAnomaly, bodies.meanAnomaly), signals = COALESCE(excluded.signals, bodies.signals), bodyId64 = COALESCE(excluded.bodyId64, bodies.bodyId64), mainStar = COALESCE(excluded.mainStar, bodies.mainStar), spectralClass = COALESCE(excluded.spectralClass, bodies.spectralClass), solarRadius = COALESCE(excluded.solarRadius, bodies.solarRadius), materials = COALESCE(excluded.materials, bodies.materials), reserveLevel = COALESCE(excluded.reserveLevel, bodies.reserveLevel), belts = COALESCE(excluded.belts, bodies.belts), updateTime = COALESCE(excluded.updateTime, bodies.updateTime)")?;
                    let mut stmt_stations = tx.prepare_cached("INSERT INTO stations (id, marketId, systemId64, name, type, distanceToArrival, allegiance, government, economy, secondEconomy, haveMarket, haveShipyard, haveOutfitting, otherServices, updateTime, realName, carrierName, controllingFaction, controllingFactionState, state, latitude, longitude, landingPads, carrierDockingAccess, economies, market, shipyard, outfitting) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(systemId64, id) DO UPDATE SET marketId = COALESCE(excluded.marketId, stations.marketId), name = COALESCE(excluded.name, stations.name), type = COALESCE(excluded.type, stations.type), distanceToArrival = COALESCE(excluded.distanceToArrival, stations.distanceToArrival), allegiance = COALESCE(excluded.allegiance, stations.allegiance), government = COALESCE(excluded.government, stations.government), economy = COALESCE(excluded.economy, stations.economy), secondEconomy = COALESCE(excluded.secondEconomy, stations.secondEconomy), haveMarket = MAX(excluded.haveMarket, stations.haveMarket), haveShipyard = MAX(excluded.haveShipyard, stations.haveShipyard), haveOutfitting = MAX(excluded.haveOutfitting, stations.haveOutfitting), otherServices = COALESCE(NULLIF(excluded.otherServices, '[]'), stations.otherServices), updateTime = COALESCE(excluded.updateTime, stations.updateTime), realName = COALESCE(excluded.realName, stations.realName), carrierName = COALESCE(excluded.carrierName, stations.carrierName), controllingFaction = COALESCE(excluded.controllingFaction, stations.controllingFaction), controllingFactionState = COALESCE(excluded.controllingFactionState, stations.controllingFactionState), state = COALESCE(excluded.state, stations.state), latitude = COALESCE(excluded.latitude, stations.latitude), longitude = COALESCE(excluded.longitude, stations.longitude), landingPads = COALESCE(excluded.landingPads, stations.landingPads), carrierDockingAccess = COALESCE(excluded.carrierDockingAccess, stations.carrierDockingAccess), economies = COALESCE(excluded.economies, stations.economies), market = COALESCE(excluded.market, stations.market), shipyard = COALESCE(excluded.shipyard, stations.shipyard), outfitting = COALESCE(excluded.outfitting, stations.outfitting)")?;
                    let mut stmt_neutron = tx.prepare_cached("INSERT OR IGNORE INTO neutron_systems (systemId64) VALUES (?)")?;
                    let mut stmt_prison  = tx.prepare_cached("INSERT OR IGNORE INTO prison_systems (systemId64) VALUES (?)")?;

                    let now = current_time_secs() as i64;
                    for sys in &batch {
                        let pop = sys.population.unwrap_or(0);
                        let controlling_faction_json = sys.controlling_faction.as_ref().filter(|v| !v.is_null()).map(|v| v.to_string());
                        let factions_json = sys.factions.as_ref().filter(|v| !v.is_null()).map(|v| v.to_string());
                        let powers_json = sys.powers.as_ref().filter(|v| !v.is_null()).map(|v| v.to_string());
                        let power_conflict_json = sys.power_conflict_progress.as_ref().filter(|v| !v.is_null()).map(|v| v.to_string());
                        let thargoid_war_json = sys.thargoid_war.as_ref().filter(|v| !v.is_null()).map(|v| v.to_string());

                        stmt_sys.execute(params![
                            sys.id64, sys.name, pop, now,
                            sys.allegiance, sys.government, sys.primary_economy, sys.secondary_economy,
                            sys.security, sys.body_count, sys.date,
                            controlling_faction_json, factions_json,
                            sys.power_state, powers_json, sys.controlling_power,
                            sys.power_state_control_progress, sys.power_state_reinforcement,
                            sys.power_state_undermining, power_conflict_json, thargoid_war_json
                        ])?;

                        // System-level prison flag. Spansh uses "Prison" and "Prison Colony";
                        // substring match on "rison" catches both, case-insensitively.
                        if sys.government.as_deref()
                            .map(|g| g.to_ascii_lowercase().contains("rison"))
                            .unwrap_or(false)
                        {
                            stmt_prison.execute(params![sys.id64]).ok();
                        }

                        if let Some(c) = &sys.coords {
                            stmt_idx.execute(params![sys.id64, c.x, c.x, c.y, c.y, c.z, c.z])?;
                        }

                        if let Some(bodies) = &sys.bodies {
                            for b in bodies {
                                // Accept surfaceTemperature as int or float (journal sends float)
                                let surface_temp: Option<i64> = get_i64(b, "surfaceTemperature")
                                    .or_else(|| get_f64(b, "surfaceTemperature").map(|f| f.round() as i64));

                                // JSON fields stored as text (filter null values)
                                let atmo_comp = b.get("atmosphereComposition").filter(|v| !v.is_null()).map(|v| v.to_string());
                                // Schema uses "solidComposition", EDMC/journal uses "composition"
                                let composition = b.get("solidComposition")
                                    .or_else(|| b.get("composition"))
                                    .filter(|v| !v.is_null()).map(|v| v.to_string());
                                let rings = b.get("rings").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let parents = b.get("parents").filter(|v| !v.is_null()).map(|v| v.to_string());
                                // signals: biology/geology counts from EDMC SAASignalsFound
                                // stored as-is; both array [{"type":"Biological"}] and
                                // dict {"Biology":3} formats are handled by coloslots.py
                                let signals = b.get("signals").filter(|v| !v.is_null()).map(|v| v.to_string());
                                // Schema uses "solarMasses", EDMC/journal uses "stellarMass"
                                let stellar_mass = get_f64(b, "solarMasses").or_else(|| get_f64(b, "stellarMass"));
                                // New schema-complete fields
                                let belts = b.get("belts").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let materials = b.get("materials").filter(|v| !v.is_null()).map(|v| v.to_string());

                                stmt_bodies.execute(params![
                                    sys.id64, get_i64(b, "bodyId"), get_str(b, "name"), get_str(b, "type"), get_str(b, "subType"),
                                    get_f64(b, "distanceToArrival"), get_bool(b, "isLandable"), get_f64(b, "gravity"), get_f64(b, "earthMasses"),
                                    get_f64(b, "radius"), surface_temp, get_f64(b, "orbitalPeriod"), get_f64(b, "semiMajorAxis"),
                                    get_f64(b, "orbitalEccentricity"), get_f64(b, "orbitalInclination"), get_f64(b, "argOfPeriapsis"), get_f64(b, "rotationalPeriod"),
                                    get_bool(b, "rotationalPeriodTidallyLocked"), get_f64(b, "axialTilt"), get_str(b, "volcanismType"), get_str(b, "atmosphereType"),
                                    get_str(b, "terraformingState"),
                                    // Stellar fields (solarMasses/stellarMass dual-read)
                                    stellar_mass, get_f64(b, "absoluteMagnitude"), get_i64(b, "age"),
                                    get_str(b, "luminosity"), get_i64(b, "subclass"), get_f64(b, "surfacePressure"),
                                    atmo_comp, composition, rings, parents,
                                    get_i64(b, "wasDiscovered").or_else(|| get_bool_opt(b, "wasDiscovered").map(|v| v as i64)),
                                    get_i64(b, "wasMapped").or_else(|| get_bool_opt(b, "wasMapped").map(|v| v as i64)),
                                    get_f64(b, "ascendingNode"), get_f64(b, "meanAnomaly"),
                                    signals,
                                    // New schema-complete fields
                                    get_i64(b, "id64"),                              // bodyId64
                                    get_bool_opt(b, "mainStar").map(|v| v as i64),   // mainStar
                                    get_str(b, "spectralClass"),                     // spectralClass
                                    get_f64(b, "solarRadius"),                       // solarRadius
                                    materials,                                        // materials (JSON)
                                    get_str(b, "reserveLevel"),                      // reserveLevel
                                    belts,                                            // belts (JSON)
                                    get_str(b, "updateTime")                         // updateTime
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

                                // New schema-complete JSON fields
                                let landing_pads = st.get("landingPads").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let economies = st.get("economies").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let market = st.get("market").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let shipyard = st.get("shipyard").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let outfitting = st.get("outfitting").filter(|v| !v.is_null()).map(|v| v.to_string());
                                // Station controllingFaction is a string (not the system-level faction object)
                                let ctrl_faction = get_str(st, "controllingFaction");
                                let ctrl_faction_state = get_str(st, "controllingFactionState");

                                stmt_stations.execute(params![
                                    get_i64(st, "id"), get_i64(st, "marketId"), sys.id64, get_str(st, "name"), get_str(st, "type"), get_f64(st, "distanceToArrival"),
                                    get_str(st, "allegiance"), get_str(st, "government"), get_str(st, "primaryEconomy"), get_str(st, "secondaryEconomy"),
                                    has_market, has_shipyard, has_outfitting, other_svcs_json, get_str(st, "updateTime"),
                                    // New schema-complete fields
                                    get_str(st, "realName"),
                                    get_str(st, "carrierName"),
                                    ctrl_faction,
                                    ctrl_faction_state,
                                    get_str(st, "state"),
                                    get_f64(st, "latitude"),
                                    get_f64(st, "longitude"),
                                    landing_pads,
                                    get_str(st, "carrierDockingAccess"),
                                    economies,
                                    market,
                                    shipyard,
                                    outfitting
                                ]).ok();

                                // Station-level prison flag. Some stations have
                                // government="Prison Colony" while their host system does not.
                                // INSERT OR IGNORE makes duplicates harmless.
                                if get_str(st, "government")
                                    .map(|g| g.to_ascii_lowercase().contains("rison"))
                                    .unwrap_or(false)
                                {
                                    stmt_prison.execute(params![sys.id64]).ok();
                                }
                            }
                        }
                    }
                }
                tx.commit()?;
                Ok(())
            })();

            match result {
                Ok(()) => break,
                Err(e) => {
                    if attempts >= 3 {
                        error!("DB Writer: batch failed after {} attempts: {}", attempts, e);
                        break;
                    }
                    warn!("DB Writer: transaction failed (attempt {}), retrying: {}", attempts, e);
                    std::thread::sleep(Duration::from_millis(500 * attempts));
                }
            }
        }
    }
    info!("DB Writer Worker shut down successfully.");
}

fn process_systems_dump(filename: &str) {
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

async fn sync_manager() {
    loop {
        let needs_sync = tokio::task::spawn_blocking(|| {
            let conn = Connection::open(DB_FILE).unwrap();
            init_db(&conn).unwrap();

            // One-time backfill: if neutron_systems is empty but bodies exist, populate it.
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

            // One-time backfill: if prison_systems is empty, populate it from
            // existing systems and stations whose government contains "rison".
            // This mirrors the neutron_systems backfill above. The query is
            // cheap because the table is small (~1.6k rows in galaxy data).
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

                    // Clean up the dump file after successful import to reclaim disk space.
                    // It will be re-downloaded fresh on the next sync cycle anyway.
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

        // Sleep 1 hour between checks (actual sync gated by SYNC_INTERVAL_SECONDS)
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

// --- API ENDPOINTS ---

async fn get_system(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SystemQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;

    let by_id64 = params.id64;
    let by_name = params.system_name;
    if by_id64.is_none() && by_name.is_none() {
        return Err((StatusCode::BAD_REQUEST, "Missing systemName or id64".into()));
    }

    let pool = state.db_pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let make_row = |row: &rusqlite::Row| -> rusqlite::Result<serde_json::Value> {
            // Parse JSON text fields
            let controlling_faction: Option<serde_json::Value> = row.get::<_, Option<String>>(6).unwrap_or(None)
                .and_then(|s| serde_json::from_str(&s).ok());
            let factions: Option<serde_json::Value> = row.get::<_, Option<String>>(7).unwrap_or(None)
                .and_then(|s| serde_json::from_str(&s).ok());
            let powers: Option<serde_json::Value> = row.get::<_, Option<String>>(8).unwrap_or(None)
                .and_then(|s| serde_json::from_str(&s).ok());
            let power_conflict: Option<serde_json::Value> = row.get::<_, Option<String>>(9).unwrap_or(None)
                .and_then(|s| serde_json::from_str(&s).ok());
            let thargoid_war: Option<serde_json::Value> = row.get::<_, Option<String>>(10).unwrap_or(None)
                .and_then(|s| serde_json::from_str(&s).ok());

            Ok(serde_json::json!({
                "id64": row.get::<_, i64>(0)?, "name": row.get::<_, String>(1)?, "population": row.get::<_, i64>(2)?,
                "coords": {"x": row.get::<_, f64>(3)?, "y": row.get::<_, f64>(4)?, "z": row.get::<_, f64>(5)?},
                "allegiance": row.get::<_, Option<String>>(11).unwrap_or(None),
                "government": row.get::<_, Option<String>>(12).unwrap_or(None),
                "primaryEconomy": row.get::<_, Option<String>>(13).unwrap_or(None),
                "secondaryEconomy": row.get::<_, Option<String>>(14).unwrap_or(None),
                "security": row.get::<_, Option<String>>(15).unwrap_or(None),
                "bodyCount": row.get::<_, Option<i64>>(16).unwrap_or(None),
                "date": row.get::<_, Option<String>>(17).unwrap_or(None),
                "controllingFaction": controlling_faction,
                "factions": factions,
                "powerState": row.get::<_, Option<String>>(18).unwrap_or(None),
                "powers": powers,
                "controllingPower": row.get::<_, Option<String>>(19).unwrap_or(None),
                "powerStateControlProgress": row.get::<_, Option<f64>>(20).unwrap_or(None),
                "powerStateReinforcement": row.get::<_, Option<f64>>(21).unwrap_or(None),
                "powerStateUndermining": row.get::<_, Option<f64>>(22).unwrap_or(None),
                "powerConflictProgress": power_conflict,
                "thargoidWar": thargoid_war,
            }))
        };

        if let Some(id) = by_id64 {
            let mut stmt = conn.prepare("
                SELECT s.id64, s.name, s.population, i.minX, i.minY, i.minZ,
                       s.controllingFaction, s.factions, s.powers, s.powerConflictProgress, s.thargoidWar,
                       s.allegiance, s.government, s.primaryEconomy, s.secondaryEconomy, s.security,
                       s.bodyCount, s.date, s.powerState, s.controllingPower,
                       s.powerStateControlProgress, s.powerStateReinforcement, s.powerStateUndermining
                FROM systems s JOIN systems_index i ON s.id64 = i.id
                WHERE s.id64 = ? LIMIT 1
            ").unwrap();
            stmt.query_row(params![id], make_row).map_err(|_| "System not found".to_string())
        } else {
            let name = by_name.unwrap();
            let mut stmt = conn.prepare("
                SELECT s.id64, s.name, s.population, i.minX, i.minY, i.minZ,
                       s.controllingFaction, s.factions, s.powers, s.powerConflictProgress, s.thargoidWar,
                       s.allegiance, s.government, s.primaryEconomy, s.secondaryEconomy, s.security,
                       s.bodyCount, s.date, s.powerState, s.controllingPower,
                       s.powerStateControlProgress, s.powerStateReinforcement, s.powerStateUndermining
                FROM systems s JOIN systems_index i ON s.id64 = i.id
                WHERE s.name = ? COLLATE NOCASE LIMIT 1
            ").unwrap();
            stmt.query_row(params![name], make_row).map_err(|_| "System not found".to_string())
        }
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

    let by_id64 = params.id64;
    let by_name = params.system_name;
    if by_id64.is_none() && by_name.is_none() {
        return Err((StatusCode::BAD_REQUEST, "Missing systemName or id64".into()));
    }

    let pool = state.db_pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let sys_row: Result<(i64, String), _> = if let Some(id) = by_id64 {
            conn.query_row(
                "SELECT id64, name FROM systems WHERE id64 = ? LIMIT 1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?))
            )
        } else {
            conn.query_row(
                "SELECT id64, name FROM systems WHERE name = ? COLLATE NOCASE LIMIT 1",
                params![by_name.unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?))
            )
        };

        let (sys_id, sys_name) = match sys_row {
            Ok(res) => res,
            Err(_) => return Ok(serde_json::json!({"bodies": []})),
        };

        let mut stmt = conn.prepare("SELECT * FROM bodies WHERE systemId64 = ? ORDER BY distanceToArrival ASC").unwrap();
            let rows = stmt.query_map(params![sys_id], |b| {
                let is_landable: i64 = b.get("isLandable").unwrap_or(0);
                let is_tidally_locked: i64 = b.get("isTidallyLocked").unwrap_or(0);
                let tf_state: Option<String> = b.get("terraformingState").unwrap_or(None);
                let was_discovered: Option<i64> = b.get("wasDiscovered").unwrap_or(None);
                let was_mapped: Option<i64> = b.get("wasMapped").unwrap_or(None);

                // Parse JSON text fields back to values
                let atmo_comp: Option<serde_json::Value> = b.get::<_, Option<String>>("atmosphereComposition").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());
                let composition: Option<serde_json::Value> = b.get::<_, Option<String>>("composition").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());
                let rings: Option<serde_json::Value> = b.get::<_, Option<String>>("rings").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());
                let parents: Option<serde_json::Value> = b.get::<_, Option<String>>("parents").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());
                // signals: biology/geology signal counts pushed by EDMC plugin
                // coloslots.py _extract_biology() reads this field
                let signals: Option<serde_json::Value> = b.get::<_, Option<String>>("signals").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());
                let materials: Option<serde_json::Value> = b.get::<_, Option<String>>("materials").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());
                let belts: Option<serde_json::Value> = b.get::<_, Option<String>>("belts").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());

                Ok(serde_json::json!({
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
                    "axialTilt": b.get::<_, Option<f64>>("axisTilt")?,
                    // Stellar fields
                    "solarMasses": b.get::<_, Option<f64>>("stellarMass").unwrap_or(None),
                    "absoluteMagnitude": b.get::<_, Option<f64>>("absoluteMagnitude").unwrap_or(None),
                    "age": b.get::<_, Option<i64>>("age").unwrap_or(None),
                    "luminosity": b.get::<_, Option<String>>("luminosity").unwrap_or(None),
                    "subclass": b.get::<_, Option<i64>>("subclass").unwrap_or(None),
                    "surfacePressure": b.get::<_, Option<f64>>("surfacePressure").unwrap_or(None),
                    "atmosphereComposition": atmo_comp,
                    "solidComposition": composition,
                    "rings": rings,
                    "parents": parents,
                    "wasDiscovered": was_discovered.map(|v| v == 1),
                    "wasMapped": was_mapped.map(|v| v == 1),
                    "ascendingNode": b.get::<_, Option<f64>>("ascendingNode").unwrap_or(None),
                    "meanAnomaly": b.get::<_, Option<f64>>("meanAnomaly").unwrap_or(None),
                    "signals": signals,
                    // New schema-complete fields
                    "id64": b.get::<_, Option<i64>>("bodyId64").unwrap_or(None),
                    "bodyId": b.get::<_, Option<i64>>("bodyId").unwrap_or(None),
                    "mainStar": b.get::<_, Option<i64>>("mainStar").unwrap_or(None).map(|v| v == 1),
                    "spectralClass": b.get::<_, Option<String>>("spectralClass").unwrap_or(None),
                    "solarRadius": b.get::<_, Option<f64>>("solarRadius").unwrap_or(None),
                    "materials": materials,
                    "reserveLevel": b.get::<_, Option<String>>("reserveLevel").unwrap_or(None),
                    "belts": belts,
                    "updateTime": b.get::<_, Option<String>>("updateTime").unwrap_or(None),
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

            // Parse JSON text fields back to values
            let landing_pads: Option<serde_json::Value> = s.get::<_, Option<String>>("landingPads").unwrap_or(None)
                .and_then(|v| serde_json::from_str(&v).ok());
            let economies: Option<serde_json::Value> = s.get::<_, Option<String>>("economies").unwrap_or(None)
                .and_then(|v| serde_json::from_str(&v).ok());
            let market: Option<serde_json::Value> = s.get::<_, Option<String>>("market").unwrap_or(None)
                .and_then(|v| serde_json::from_str(&v).ok());
            let shipyard: Option<serde_json::Value> = s.get::<_, Option<String>>("shipyard").unwrap_or(None)
                .and_then(|v| serde_json::from_str(&v).ok());
            let outfitting: Option<serde_json::Value> = s.get::<_, Option<String>>("outfitting").unwrap_or(None)
                .and_then(|v| serde_json::from_str(&v).ok());

            Ok(serde_json::json!({
                "id": s.get::<_, i64>("id")?,
                "marketId": s.get::<_, Option<i64>>("marketId")?,
                "type": s.get::<_, Option<String>>("type")?,
                "name": s.get::<_, Option<String>>("name")?,
                "distanceToArrival": s.get::<_, Option<f64>>("distanceToArrival")?,
                "allegiance": s.get::<_, Option<String>>("allegiance")?,
                "government": s.get::<_, Option<String>>("government")?,
                "primaryEconomy": s.get::<_, Option<String>>("economy")?,
                "secondaryEconomy": s.get::<_, Option<String>>("secondEconomy")?,
                "haveMarket": have_market == 1,
                "haveShipyard": have_shipyard == 1,
                "haveOutfitting": have_outfitting == 1,
                "services": other_services,
                "updateTime": s.get::<_, Option<String>>("updateTime")?,
                // New schema-complete fields
                "realName": s.get::<_, Option<String>>("realName").unwrap_or(None),
                "carrierName": s.get::<_, Option<String>>("carrierName").unwrap_or(None),
                "controllingFaction": s.get::<_, Option<String>>("controllingFaction").unwrap_or(None),
                "controllingFactionState": s.get::<_, Option<String>>("controllingFactionState").unwrap_or(None),
                "state": s.get::<_, Option<String>>("state").unwrap_or(None),
                "latitude": s.get::<_, Option<f64>>("latitude").unwrap_or(None),
                "longitude": s.get::<_, Option<f64>>("longitude").unwrap_or(None),
                "landingPads": landing_pads,
                "carrierDockingAccess": s.get::<_, Option<String>>("carrierDockingAccess").unwrap_or(None),
                "economies": economies,
                "market": market,
                "shipyard": shipyard,
                "outfitting": outfitting,
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

async fn get_distance(
    State(state): State<Arc<AppState>>,
    Query(params): Query<DistanceQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;

    let name_a = params.system_a;
    let name_b = params.system_b;
    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let lookup = |name: &str| -> Result<(i64, String, f64, f64, f64), String> {
            conn.query_row(
                "SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                 FROM systems s JOIN systems_index i ON s.id64 = i.id
                 WHERE s.name = ? COLLATE NOCASE LIMIT 1",
                params![name],
                |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                )),
            )
            .map_err(|_| format!("System not found: {}", name))
        };

        let (id_a, resolved_a, ax, ay, az) = lookup(&name_a)?;
        let (id_b, resolved_b, bx, by, bz) = lookup(&name_b)?;

        let dx = bx - ax;
        let dy = by - ay;
        let dz = bz - az;
        let distance = (dx * dx + dy * dy + dz * dz).sqrt();

        Ok(serde_json::json!({
            "systemA": { "id64": id_a, "name": resolved_a, "coords": { "x": ax, "y": ay, "z": az } },
            "systemB": { "id64": id_b, "name": resolved_b, "coords": { "x": bx, "y": by, "z": bz } },
            "distanceLy": (distance * 100.0).round() / 100.0
        }))
    })
    .await
    .unwrap();

    match result {
        Ok(json) => Ok(Json(json)),
        Err(e) => Err((StatusCode::NOT_FOUND, e)),
    }
}

// --- NEAREST STATION SEARCH ---
//
// GET /api/nearest-station?refSystem=Lhou+Mans&radius=50&allegiance=Empire&…
//
// Finds the closest stations to a reference system that satisfy a set of
// operational filters matching the Spansh "nearest station" UI.
//
// Filter semantics:
//   allegiance / government / economy  – matched against the STATION's own
//       columns (not the system), so you get the faction running the port.
//   stationType       – exact match on station.type
//   minLandingPad     – Small (any pad), Medium (≥medium), Large (large only)
//   maxStationDistance– upper bound on station.distanceToArrival (light-seconds)
//   useSurfaceStations– false → skip stations where latitude IS NOT NULL
//                       true  → include planetary / settlement stations
//   ignoreFleetCarriers – true (default) → exclude Drake-Class Carriers
async fn nearest_station(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NearestStationQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;

    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        // ── 1. Resolve reference system coordinates ──────────────────────────
        let ref_name = params.ref_system.clone();
        let (ref_x, ref_y, ref_z): (f64, f64, f64) = conn.query_row(
            "SELECT i.minX, i.minY, i.minZ
             FROM systems s JOIN systems_index i ON s.id64 = i.id
             WHERE s.name = ? COLLATE NOCASE LIMIT 1",
            rusqlite::params![ref_name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).map_err(|_| format!("Reference system '{}' not found", ref_name))?;

        let limit = params.limit.unwrap_or(25).clamp(1, 100);

        // ── 2. Normalise filter values (computed once, reused each iteration) ─
        let ignore_carriers    = params.ignore_fleet_carriers.unwrap_or(true);
        let use_surface        = params.use_surface_stations;
        let allegiance_filter  = params.allegiance.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty())
            .map(|s| s.to_string());
        let government_filter  = params.government.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty())
            .map(|s| s.to_string());
        let economy_filter     = params.economy.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty())
            .map(|s| s.to_string());
        let station_type_filter = params.station_type.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty())
            .map(|s| s.to_string());
        let max_dist_filter    = params.max_station_distance;
        let pad = params.min_landing_pad.as_deref()
            .map(|s| s.to_lowercase()).unwrap_or_default();
        let require_large  = pad == "large";
        let require_medium = pad == "medium" || require_large;

        // ── 3. Build SQL ──────────────────────────────────────────────────
        // Two execution paths:
        //   • Prison search (government filter set): drive from prison_systems
        //     (~1.6k rows pre-materialised at import). Bypasses bbox + expanding
        //     search entirely — the full candidate set is small enough to evaluate
        //     in one shot at any reference coordinate, including deep space.
        //   • All other searches: drive from systems_index (rtree) with an
        //     expanding bbox loop, as before.
        //
        // The trailing filter clauses are identical for both paths. The
        // `government_filter` clause stays in place even on the prison path:
        // prison_systems is a system-level superset (a system is included if
        // EITHER its own government or any of its stations contains "rison"),
        // and we still need to filter individual stations within those
        // candidate systems down to the ones that qualify.
        let is_prison = government_filter.is_some();

        let mut sql = String::from(
            "SELECT s.id64 AS systemId64, s.name AS systemName,
                    i.minX AS x, i.minY AS y, i.minZ AS z,
                    st.id AS stationId, st.name AS stationName,
                    st.type AS stationType, st.distanceToArrival,
                    st.allegiance AS stAllegiance, st.government AS stGovernment,
                    st.economy AS stEconomy, st.secondEconomy,
                    st.haveMarket, st.haveShipyard, st.haveOutfitting,
                    st.otherServices, st.updateTime, st.landingPads,
                    st.latitude, st.longitude, st.controllingFaction,
                    st.state, st.carrierDockingAccess, st.realName, st.carrierName ");

        if is_prison {
            sql.push_str(
                "FROM prison_systems ps
                 JOIN systems s        ON s.id64 = ps.systemId64
                 JOIN systems_index i  ON i.id   = ps.systemId64
                 JOIN stations st      ON st.systemId64 = ps.systemId64
                 WHERE 1=1");
        } else {
            sql.push_str(
                "FROM systems_index i
                 JOIN systems s  ON i.id = s.id64
                 JOIN stations st ON st.systemId64 = s.id64
                 WHERE i.minX >= ? AND i.maxX <= ?
                   AND i.minY >= ? AND i.maxY <= ?
                   AND i.minZ >= ? AND i.maxZ <= ?");
        }

        if ignore_carriers    { sql.push_str(" AND st.type != 'Drake-Class Carrier'"); }
        if !use_surface       { sql.push_str(" AND st.latitude IS NULL"); }
        if allegiance_filter.is_some()   { sql.push_str(" AND st.allegiance = ? COLLATE NOCASE"); }
        if government_filter.is_some()   { sql.push_str(" AND (st.government LIKE '%rison%' OR s.government LIKE '%rison%')"); }
        if economy_filter.is_some()      { sql.push_str(" AND (st.economy = ? COLLATE NOCASE OR s.primaryEconomy = ? COLLATE NOCASE)"); }
        if station_type_filter.is_some() { sql.push_str(" AND st.type = ? COLLATE NOCASE"); }
        if max_dist_filter.is_some()     { sql.push_str(" AND (st.distanceToArrival IS NULL OR st.distanceToArrival <= ?)"); }
        if require_large {
            sql.push_str(" AND COALESCE(json_extract(st.landingPads,'$.large'),json_extract(st.landingPads,'$.Large'),0) > 0");
        } else if require_medium {
            sql.push_str(" AND (COALESCE(json_extract(st.landingPads,'$.large'),json_extract(st.landingPads,'$.Large'),0) > 0 OR COALESCE(json_extract(st.landingPads,'$.medium'),json_extract(st.landingPads,'$.Medium'),0) > 0)");
        }
        sql.push_str(" LIMIT 2000");

        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;

        // Helper: run the query. radius_opt=Some binds bbox params (rtree path);
        // radius_opt=None skips bbox binding (prison_systems path).
        let run_query = |stmt: &mut rusqlite::Statement, radius_opt: Option<f64>|
            -> Result<Vec<serde_json::Value>, String>
        {
            let mut raw_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            if let Some(radius) = radius_opt {
                let (min_x, max_x) = (ref_x - radius, ref_x + radius);
                let (min_y, max_y) = (ref_y - radius, ref_y + radius);
                let (min_z, max_z) = (ref_z - radius, ref_z + radius);
                raw_params.push(Box::new(min_x)); raw_params.push(Box::new(max_x));
                raw_params.push(Box::new(min_y)); raw_params.push(Box::new(max_y));
                raw_params.push(Box::new(min_z)); raw_params.push(Box::new(max_z));
            }
            if let Some(ref v) = allegiance_filter   { raw_params.push(Box::new(v.clone())); }
            // government_filter uses LIKE literal — no params to bind
            if let Some(ref v) = economy_filter      { raw_params.push(Box::new(v.clone())); raw_params.push(Box::new(v.clone())); }
            if let Some(ref v) = station_type_filter { raw_params.push(Box::new(v.clone())); }
            if let Some(v)     = max_dist_filter     { raw_params.push(Box::new(v)); }

            let param_refs: Vec<&dyn rusqlite::ToSql> =
                raw_params.iter().map(|b| b.as_ref()).collect();

            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let sx: f64 = row.get("x")?;
                let sy: f64 = row.get("y")?;
                let sz: f64 = row.get("z")?;
                let have_market: i64     = row.get::<_, i64>("haveMarket").unwrap_or(0);
                let have_shipyard: i64   = row.get::<_, i64>("haveShipyard").unwrap_or(0);
                let have_outfitting: i64 = row.get::<_, i64>("haveOutfitting").unwrap_or(0);
                let other_services: Vec<String> = row
                    .get::<_, Option<String>>("otherServices").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
                let landing_pads: Option<serde_json::Value> = row
                    .get::<_, Option<String>>("landingPads").unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());
                let latitude: Option<f64> = row.get::<_, Option<f64>>("latitude").unwrap_or(None);
                Ok((
                    row.get::<_, i64>("systemId64")?,
                    row.get::<_, String>("systemName")?,
                    sx, sy, sz,
                    row.get::<_, i64>("stationId")?,
                    row.get::<_, Option<String>>("stationName")?,
                    row.get::<_, Option<String>>("stationType")?,
                    row.get::<_, Option<f64>>("distanceToArrival")?,
                    row.get::<_, Option<String>>("stAllegiance")?,
                    row.get::<_, Option<String>>("stGovernment")?,
                    row.get::<_, Option<String>>("stEconomy")?,
                    row.get::<_, Option<String>>("secondEconomy")?,
                    have_market, have_shipyard, have_outfitting, other_services,
                    row.get::<_, Option<String>>("updateTime")?,
                    landing_pads, latitude,
                    row.get::<_, Option<f64>>("longitude").unwrap_or(None),
                    row.get::<_, Option<String>>("controllingFaction").unwrap_or(None),
                    row.get::<_, Option<String>>("state").unwrap_or(None),
                    row.get::<_, Option<String>>("carrierDockingAccess").unwrap_or(None),
                    row.get::<_, Option<String>>("realName").unwrap_or(None),
                    row.get::<_, Option<String>>("carrierName").unwrap_or(None),
                ))
            }).map_err(|e| e.to_string())?;

            let mut results: Vec<serde_json::Value> = rows
                .filter_map(|r| r.ok())
                .map(|(system_id64, system_name, sx, sy, sz,
                       station_id, station_name, station_type, dist_to_arrival,
                       allegiance, government, economy, second_economy,
                       have_market, have_shipyard, have_outfitting, other_services,
                       update_time, landing_pads, latitude, longitude,
                       controlling_faction, state_, carrier_docking, real_name, carrier_name)| {
                    let dist_ly = ((sx-ref_x).powi(2)+(sy-ref_y).powi(2)+(sz-ref_z).powi(2)).sqrt();
                    serde_json::json!({
                        "systemId64": system_id64.to_string(),
                        "systemName": system_name,
                        "systemDistanceLy": (dist_ly * 100.0).round() / 100.0,
                        "coords": {"x": sx, "y": sy, "z": sz},
                        "stationId": station_id,
                        "stationName": station_name,
                        "stationType": station_type,
                        "distanceToArrival": dist_to_arrival,
                        "allegiance": allegiance,
                        "government": government,
                        "primaryEconomy": economy,
                        "secondaryEconomy": second_economy,
                        "haveMarket": have_market == 1,
                        "haveShipyard": have_shipyard == 1,
                        "haveOutfitting": have_outfitting == 1,
                        "services": other_services,
                        "updateTime": update_time,
                        "landingPads": landing_pads,
                        "latitude": latitude,
                        "longitude": longitude,
                        "isSurface": latitude.is_some(),
                        "controllingFaction": controlling_faction,
                        "state": state_,
                        "carrierDockingAccess": carrier_docking,
                        "realName": real_name,
                        "carrierName": carrier_name,
                    })
                })
                .collect();

            results.sort_by(|a, b| {
                a["systemDistanceLy"].as_f64().unwrap_or(f64::MAX)
                    .partial_cmp(&b["systemDistanceLy"].as_f64().unwrap_or(f64::MAX))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            Ok(results)
        };

        // ── 4. Run search ────────────────────────────────────────────────
        // Prison path: one shot, no expanding loop. The candidate set
        // (prison_systems, ~1.6k rows) is already bounded, and we sort the
        // full result by distance in Rust before truncating to `limit`.
        // The reported `searchedRadiusLy` is the distance to the farthest
        // result we kept, since there is no explicit search radius.
        //
        // Non-prison path: expanding bbox loop, as before. Fixed steps jump
        // quickly to bubble scale and beyond — avoids 10+ doubling iterations
        // when the player is thousands of LY from any inhabited system.
        let search_steps: &[f64] = &[100.0, 500.0, 1_500.0, 4_000.0, 8_000.0, 15_000.0, 30_000.0, 70_000.0];
        let override_radius = params.radius;
        let mut results;
        let mut radius = 100.0_f64;

        if is_prison {
            results = run_query(&mut stmt, None)?;
        } else if let Some(r) = override_radius {
            // Caller specified an explicit radius — honour it, single shot.
            radius = r.clamp(1.0, 70_000.0);
            results = run_query(&mut stmt, Some(radius))?;
        } else {
            results = Vec::new();
            for &step in search_steps {
                radius = step;
                results = run_query(&mut stmt, Some(radius))?;
                if !results.is_empty() { break; }
            }
        }

        results.truncate(limit);

        // For prison searches, report the distance to the farthest kept result.
        // For other searches, report the radius the expanding loop stopped at.
        let reported_radius = if is_prison {
            results.last()
                .and_then(|r| r["systemDistanceLy"].as_f64())
                .unwrap_or(0.0)
        } else {
            radius
        };

        Ok(serde_json::json!({
            "refSystem": ref_name,
            "refCoords": {"x": ref_x, "y": ref_y, "z": ref_z},
            "searchedRadiusLy": reported_radius,
            "count": results.len(),
            "results": results,
        }))
    }).await.unwrap().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(result))
}


// CORE SEARCH LOGIC
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

// A* SHIP ROUTING ENDPOINT
async fn do_ship_route(
    state: Arc<AppState>,
    params: RouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.astar_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded — A* queue full".into()))?;

    let pool = state.db_pool.clone();
    let source_name = params.source.clone();
    let dest_name = params.destination.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let t_start = Instant::now();

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

        let mut max_iterations = 2_000_000;

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
            if max_iterations == 0 || t_start.elapsed().as_millis() > SHIP_ROUTE_BUDGET_MS {
                return Err("Route calculation exceeded time/iteration budget. Try breaking up your journey into smaller segments.".to_string());
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

// FLEET CARRIER ROUTING ENDPOINT
// =============================================================================
// FLEET CARRIER ROUTING — Greedy baseline + optional A* refinement
// =============================================================================
//
// The carrier jump range is always 500 LY (any system is a valid target).
// A* optimises for fewest jumps through a corridor of systems preloaded from
// the R-tree. Fuel simulation runs during path reconstruction, not search.
//
// Engine modes:
//   "greedy" (default) — fast, always gets a route
//   "astar"            — greedy first, then A* refinement within time budget
// =============================================================================

const CARRIER_REFINE_BUDGET_MS: u128 = 1_800_000; // 30 minutes
const CARRIER_JUMP_RANGE: f64 = 500.0;

async fn do_carrier_route(
    state: Arc<AppState>,
    params: CarrierRouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.astar_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded — A* queue full".into()))?;
    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        let t_start = Instant::now();

        // ── A* priority node ─────────────────────────────────────────────────
        #[derive(Clone)]
        struct CNode { g: u32, f: f64, id: i64 }
        impl PartialEq for CNode { fn eq(&self, o: &Self) -> bool { self.id == o.id } }
        impl Eq for CNode {}
        impl PartialOrd for CNode { fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) } }
        impl Ord for CNode {
            fn cmp(&self, o: &Self) -> Ordering { o.f.partial_cmp(&self.f).unwrap_or(Ordering::Equal) }
        }

        let get_coords = |sys_input: &str| -> Result<(i64, String, f64, f64, f64), String> {
            if let Ok(id) = sys_input.parse::<i64>() {
                conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.id64 = ? LIMIT 1",
                    rusqlite::params![id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                ).map_err(|_| format!("System ID '{}' not found", id))
            } else {
                conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.name = ? COLLATE NOCASE LIMIT 1",
                    rusqlite::params![sys_input],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                ).map_err(|_| format!("System '{}' not found", sys_input))
            }
        };

        let (src_id, src_name, x1, y1, z1) = get_coords(&params.current_system)?;
        let (dest_id, dest_name, x2, y2, z2) = get_coords(&params.destination)?;

        let total_distance = ((x2 - x1).powi(2) + (y2 - y1).powi(2) + (z2 - z1).powi(2)).sqrt();
        let base_cargo = params.used_cargo;
        let is_squadron = params.is_squadron.unwrap_or(false);
        let carrier_base_mass = if is_squadron { 60000.0 } else { 25000.0 };
        let max_tank_capacity = params.tank_fuel;

        info!("Carrier route: {} -> {}, {:.0} LY, {} carrier",
            params.current_system, params.destination, total_distance,
            if is_squadron { "squadron" } else { "personal" });

        // ── Greedy baseline ──────────────────────────────────────────────────
        // Build greedy path as Vec<(id64, name, x, y, z)>
        let mut greedy_path: Vec<(i64, String, f64, f64, f64)> = vec![
            (src_id, src_name.clone(), x1, y1, z1)
        ];
        {
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let mut current_pos = (x1, y1, z1);
            let mut _current_id = src_id;
            let mut current_sys_name = params.current_system.clone();
            let mut distance_remaining = total_distance;

            for _ in 0..500usize {
                if distance_remaining <= CARRIER_JUMP_RANGE { break; }

                let v_x = x2 - current_pos.0;
                let v_y = y2 - current_pos.1;
                let v_z = z2 - current_pos.2;
                let v_mag = (v_x.powi(2) + v_y.powi(2) + v_z.powi(2)).sqrt();
                let u_x = v_x / v_mag; let u_y = v_y / v_mag; let u_z = v_z / v_mag;

                let target_dist = 498.5;
                let t_x = current_pos.0 + u_x * target_dist;
                let t_y = current_pos.1 + u_y * target_dist;
                let t_z = current_pos.2 + u_z * target_dist;

                let mut best: Option<(i64, String, f64, f64, f64, f64)> = None; // id, name, x, y, z, dist_to_dest
                let mut min_dist_to_dest = distance_remaining;
                let mut search_radius = 20.0;

                for _ in 0..5 {
                    let (mn_x, mx_x) = (t_x - search_radius, t_x + search_radius);
                    let (mn_y, mx_y) = (t_y - search_radius, t_y + search_radius);
                    let (mn_z, mx_z) = (t_z - search_radius, t_z + search_radius);

                    let mut stmt = tx.prepare_cached("
                        SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                        FROM systems_index i JOIN systems s ON i.id = s.id64
                        WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
                    ").unwrap();

                    let rows = stmt.query_map(rusqlite::params![mn_x, mx_x, mn_y, mx_y, mn_z, mx_z], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, f64>(2)?, row.get::<_, f64>(3)?, row.get::<_, f64>(4)?))
                    }).unwrap();

                    for row in rows.filter_map(Result::ok) {
                        let (id, name, cx, cy, cz) = row;
                        let d_cur = ((cx - current_pos.0).powi(2) + (cy - current_pos.1).powi(2) + (cz - current_pos.2).powi(2)).sqrt();
                        if d_cur <= CARRIER_JUMP_RANGE && d_cur > 0.0 {
                            let d_dst = ((x2 - cx).powi(2) + (y2 - cy).powi(2) + (z2 - cz).powi(2)).sqrt();
                            if d_dst < min_dist_to_dest {
                                min_dist_to_dest = d_dst;
                                best = Some((id, name, cx, cy, cz, d_dst));
                            }
                        }
                    }

                    if best.is_some() { break; } else { search_radius += 30.0; }
                }

                match best {
                    Some((id, name, cx, cy, cz, d_dst)) => {
                        greedy_path.push((id, name.clone(), cx, cy, cz));
                        current_pos = (cx, cy, cz);
                        _current_id = id;
                        current_sys_name = name;
                        distance_remaining = d_dst;
                    }
                    None => {
                        return Err(format!("Route failed: Could not find a star within range after system '{}'.", current_sys_name));
                    }
                }
            }

            // Final hop to destination
            greedy_path.push((dest_id, dest_name.clone(), x2, y2, z2));
        }

        let greedy_jumps = greedy_path.len() - 1;
        info!("Carrier greedy: {} jumps in {}ms", greedy_jumps, t_start.elapsed().as_millis());

        let use_astar = params.engine.as_deref().unwrap_or("greedy").to_lowercase() != "greedy";

        // ── A* refinement (optional, time-bounded) ───────────────────────────
        // Preload all systems in a corridor around the straight-line path,
        // then run A* to minimise jump count. The carrier can jump to ANY
        // system within 500 LY so the graph is dense — we use a spatial grid
        // and bound by greedy_jumps to prune aggressively.
        let mut astar_path: Option<Vec<(i64, String, f64, f64, f64)>> = None;

        if use_astar {
            // Preload corridor systems
            let corridor_half = 1500.0f64; // 3x jump range gives good coverage
            let corridor_sq = corridor_half * corridor_half;

            let dv = (x2 - x1, y2 - y1, z2 - z1);
            let dv_len_sq = dv.0*dv.0 + dv.1*dv.1 + dv.2*dv.2;

            let mut preload_stmt = conn.prepare("
                SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                FROM systems_index i JOIN systems s ON i.id = s.id64
                WHERE i.minX BETWEEN ? AND ?
                  AND i.minY BETWEEN ? AND ?
                  AND i.minZ BETWEEN ? AND ?
            ").map_err(|e| e.to_string())?;

            let buf = corridor_half;
            let all_systems: Vec<(i64, String, f64, f64, f64)> = preload_stmt.query_map(
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
                (nx-px).powi(2) + (ny-py).powi(2) + (nz-pz).powi(2) <= corridor_sq
            })
            .collect();

            info!("Carrier A* corridor preload: {} systems", all_systems.len());

            // Build position/name lookup maps and spatial grid
            let mut node_pos: HashMap<i64, (f64, f64, f64)> = HashMap::with_capacity(all_systems.len());
            let mut node_name: HashMap<i64, String> = HashMap::with_capacity(all_systems.len());

            let cell_size = (CARRIER_JUMP_RANGE * 0.9).max(50.0);
            let mut grid: HashMap<(i32, i32, i32), Vec<i64>> = HashMap::new();

            for &(id, ref name, nx, ny, nz) in &all_systems {
                node_pos.insert(id, (nx, ny, nz));
                node_name.insert(id, name.clone());
                let cell = ((nx / cell_size) as i32, (ny / cell_size) as i32, (nz / cell_size) as i32);
                grid.entry(cell).or_default().push(id);
            }
            // Ensure src/dst are in the maps
            node_pos.entry(src_id).or_insert((x1, y1, z1));
            node_name.entry(src_id).or_insert(src_name.clone());
            node_pos.entry(dest_id).or_insert((x2, y2, z2));
            node_name.entry(dest_id).or_insert(dest_name.clone());

            let t_astar_start = Instant::now();

            let h_fn = |x: f64, y: f64, z: f64| -> f64 {
                (((x-x2).powi(2)+(y-y2).powi(2)+(z-z2).powi(2)).sqrt() / CARRIER_JUMP_RANGE).ceil()
            };

            // ── Bidirectional A* for long routes, unidirectional for short ────
            astar_path = if total_distance > 5_000.0 {
                // ── Bidirectional A* ─────────────────────────────────────────
                let h_bwd = |x: f64, y: f64, z: f64| -> f64 {
                    (((x-x1).powi(2)+(y-y1).powi(2)+(z-z1).powi(2)).sqrt() / CARRIER_JUMP_RANGE).ceil()
                };

                (|| {
                    let mut fwd_cf: HashMap<i64, i64> = HashMap::new();
                    let mut bwd_cf: HashMap<i64, i64> = HashMap::new();
                    let mut fwd_g: HashMap<i64, u32> = HashMap::new();
                    let mut bwd_g: HashMap<i64, u32> = HashMap::new();
                    let mut fwd_closed: HashSet<i64> = HashSet::new();
                    let mut bwd_closed: HashSet<i64> = HashSet::new();
                    let mut fwd_open: BinaryHeap<CNode> = BinaryHeap::new();
                    let mut bwd_open: BinaryHeap<CNode> = BinaryHeap::new();

                    fwd_g.insert(src_id, 0);
                    bwd_g.insert(dest_id, 0);
                    fwd_open.push(CNode { g: 0, f: h_fn(x1, y1, z1), id: src_id });
                    bwd_open.push(CNode { g: 0, f: h_bwd(x2, y2, z2), id: dest_id });

                    let mut mu: u32 = greedy_jumps as u32;
                    let mut best_meeting: Option<i64> = None;
                    let rsq = CARRIER_JUMP_RANGE * CARRIER_JUMP_RANGE;

                    loop {
                        if t_astar_start.elapsed().as_millis() > CARRIER_REFINE_BUDGET_MS { break; }
                        if fwd_open.is_empty() && bwd_open.is_empty() { break; }

                        let fwd_min_g = fwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                        let bwd_min_g = bwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                        if fwd_min_g.saturating_add(bwd_min_g) >= mu { break; }

                        let fwd_min_f = fwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                        let bwd_min_f = bwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                        let expand_fwd = fwd_min_f <= bwd_min_f;

                        macro_rules! expand_carrier {
                            ($cx:expr, $cy:expr, $cz:expr, $id:expr,
                             $my_g:expr, $my_g_map:expr, $my_cf:expr, $my_open:expr,
                             $my_closed:expr, $other_g_map:expr, $h_fn_local:expr) => {{
                                let (bx, by, bz) = (($cx/cell_size) as i32, ($cy/cell_size) as i32, ($cz/cell_size) as i32);
                                for dx in -2i32..=2 { for dy in -2i32..=2 { for dz in -2i32..=2 {
                                    if let Some(v) = grid.get(&(bx+dx, by+dy, bz+dz)) {
                                        for &n_id in v {
                                            if n_id == $id || $my_closed.contains(&n_id) { continue; }
                                            let (nx, ny, nz) = match node_pos.get(&n_id) { Some(&p) => p, None => continue };
                                            let d2 = (nx-$cx).powi(2)+(ny-$cy).powi(2)+(nz-$cz).powi(2);
                                            if d2 > rsq || d2 == 0.0 { continue; }
                                            let tg = $my_g + 1;
                                            if tg < *$my_g_map.get(&n_id).unwrap_or(&u32::MAX) {
                                                $my_g_map.insert(n_id, tg);
                                                $my_cf.insert(n_id, $id);
                                                $my_open.push(CNode { g: tg, f: tg as f64 + $h_fn_local(nx, ny, nz), id: n_id });
                                                if let Some(&og) = $other_g_map.get(&n_id) {
                                                    let total = tg + og;
                                                    if total < mu { mu = total; best_meeting = Some(n_id); }
                                                }
                                            }
                                        }
                                    }
                                }}}
                            }};
                        }

                        if expand_fwd {
                            let Some(CNode { g, id, .. }) = fwd_open.pop() else { continue; };
                            if g >= mu { continue; }
                            if fwd_closed.contains(&id) { continue; }
                            fwd_closed.insert(id);

                            if let Some(&bg) = bwd_g.get(&id) {
                                let total = g + bg;
                                if total < mu { mu = total; best_meeting = Some(id); }
                            }

                            let (cx, cy, cz) = match node_pos.get(&id) { Some(&p) => p, None => continue };

                            // Direct reach dst?
                            let d_dst = ((cx-x2).powi(2)+(cy-y2).powi(2)+(cz-z2).powi(2)).sqrt();
                            if d_dst <= CARRIER_JUMP_RANGE {
                                let tg = g + 1;
                                if tg < *fwd_g.get(&dest_id).unwrap_or(&u32::MAX) {
                                    fwd_g.insert(dest_id, tg);
                                    fwd_cf.insert(dest_id, id);
                                    fwd_open.push(CNode { g: tg, f: tg as f64, id: dest_id });
                                    if tg < mu { mu = tg; best_meeting = Some(dest_id); }
                                }
                            }

                            expand_carrier!(cx, cy, cz, id, g, fwd_g, fwd_cf, fwd_open, fwd_closed, bwd_g, h_fn);
                        } else {
                            let Some(CNode { g, id, .. }) = bwd_open.pop() else { continue; };
                            if g >= mu { continue; }
                            if bwd_closed.contains(&id) { continue; }
                            bwd_closed.insert(id);

                            if let Some(&fg) = fwd_g.get(&id) {
                                let total = fg + g;
                                if total < mu { mu = total; best_meeting = Some(id); }
                            }

                            let (cx, cy, cz) = match node_pos.get(&id) { Some(&p) => p, None => continue };

                            let d_src = ((cx-x1).powi(2)+(cy-y1).powi(2)+(cz-z1).powi(2)).sqrt();
                            if d_src <= CARRIER_JUMP_RANGE {
                                let tg = g + 1;
                                if tg < *bwd_g.get(&src_id).unwrap_or(&u32::MAX) {
                                    bwd_g.insert(src_id, tg);
                                    bwd_cf.insert(src_id, id);
                                    bwd_open.push(CNode { g: tg, f: tg as f64, id: src_id });
                                    if tg < mu { mu = tg; best_meeting = Some(src_id); }
                                }
                            }

                            expand_carrier!(cx, cy, cz, id, g, bwd_g, bwd_cf, bwd_open, bwd_closed, fwd_g, h_bwd);
                        }
                    }

                    // Reconstruct path through meeting node
                    let m = best_meeting?;

                    let mut fwd_path: Vec<i64> = vec![m];
                    let mut cur = m;
                    while cur != src_id {
                        match fwd_cf.get(&cur) {
                            Some(&p) => { cur = p; fwd_path.push(cur); }
                            None => return None,
                        }
                    }
                    fwd_path.reverse();

                    let mut bwd_path: Vec<i64> = Vec::new();
                    let mut cur = m;
                    while cur != dest_id {
                        match bwd_cf.get(&cur) {
                            Some(&p) => { cur = p; bwd_path.push(cur); }
                            None => break,
                        }
                    }
                    if *bwd_path.last().unwrap_or(&m) != dest_id {
                        bwd_path.push(dest_id);
                    }

                    fwd_path.extend(bwd_path);

                    // Convert id path to full tuples
                    let result: Vec<(i64, String, f64, f64, f64)> = fwd_path.iter().map(|&id| {
                        let (nx, ny, nz) = node_pos.get(&id).copied().unwrap_or((0.0, 0.0, 0.0));
                        let name = node_name.get(&id).cloned().unwrap_or_default();
                        (id, name, nx, ny, nz)
                    }).collect();
                    Some(result)
                })()
            } else {
                // ── Unidirectional A* for routes ≤ 5k LY ────────────────────
                (|| {
                    let mut came_from: HashMap<i64, i64> = HashMap::new();
                    let mut g_score: HashMap<i64, u32> = HashMap::new();
                    let mut closed: HashSet<i64> = HashSet::new();
                    let mut open: BinaryHeap<CNode> = BinaryHeap::new();
                    let rsq = CARRIER_JUMP_RANGE * CARRIER_JUMP_RANGE;

                    g_score.insert(src_id, 0);
                    open.push(CNode { g: 0, f: h_fn(x1, y1, z1), id: src_id });

                    while let Some(CNode { g, id, .. }) = open.pop() {
                        if t_astar_start.elapsed().as_millis() > CARRIER_REFINE_BUDGET_MS { return None; }
                        if g as usize >= greedy_jumps { continue; }

                        if id == dest_id {
                            let mut path = vec![dest_id];
                            let mut cur = dest_id;
                            while cur != src_id {
                                match came_from.get(&cur) {
                                    Some(&p) => { cur = p; path.push(cur); }
                                    None => return None,
                                }
                            }
                            path.reverse();
                            let result: Vec<(i64, String, f64, f64, f64)> = path.iter().map(|&id| {
                                let (nx, ny, nz) = node_pos.get(&id).copied().unwrap_or((0.0, 0.0, 0.0));
                                let name = node_name.get(&id).cloned().unwrap_or_default();
                                (id, name, nx, ny, nz)
                            }).collect();
                            return Some(result);
                        }

                        if closed.contains(&id) { continue; }
                        closed.insert(id);

                        let (cx, cy, cz) = match node_pos.get(&id) { Some(&p) => p, None => continue };

                        // Direct dst reach
                        let d_dst = ((cx-x2).powi(2)+(cy-y2).powi(2)+(cz-z2).powi(2)).sqrt();
                        if d_dst <= CARRIER_JUMP_RANGE && !closed.contains(&dest_id) {
                            let tg = g + 1;
                            if tg < *g_score.get(&dest_id).unwrap_or(&u32::MAX) {
                                g_score.insert(dest_id, tg);
                                came_from.insert(dest_id, id);
                                open.push(CNode { g: tg, f: tg as f64, id: dest_id });
                            }
                        }

                        let (bx, by, bz) = ((cx/cell_size) as i32, (cy/cell_size) as i32, (cz/cell_size) as i32);
                        for dx in -2i32..=2 { for dy in -2i32..=2 { for dz in -2i32..=2 {
                            if let Some(v) = grid.get(&(bx+dx, by+dy, bz+dz)) {
                                for &n_id in v {
                                    if n_id == id || closed.contains(&n_id) { continue; }
                                    let (nx, ny, nz) = match node_pos.get(&n_id) { Some(&p) => p, None => continue };
                                    let d2 = (nx-cx).powi(2)+(ny-cy).powi(2)+(nz-cz).powi(2);
                                    if d2 > rsq || d2 == 0.0 { continue; }
                                    let tg = g + 1;
                                    if tg < *g_score.get(&n_id).unwrap_or(&u32::MAX) {
                                        g_score.insert(n_id, tg);
                                        came_from.insert(n_id, id);
                                        open.push(CNode { g: tg, f: tg as f64 + h_fn(nx, ny, nz), id: n_id });
                                    }
                                }
                            }
                        }}}
                    }
                    None
                })()
            };

            if let Some(ref ap) = astar_path {
                info!("Carrier A* found {} jumps (greedy was {}), {}ms",
                    ap.len() - 1, greedy_jumps, t_start.elapsed().as_millis());
            } else {
                info!("Carrier A* did not improve on greedy ({} jumps), {}ms",
                    greedy_jumps, t_start.elapsed().as_millis());
            }
        } else {
            info!("Engine: greedy only, skipping A* refinement ({} jumps)", greedy_jumps);
        }

        // ── Select final path ─────────────────────────────────────────────────
        let final_path = if use_astar { astar_path.unwrap_or(greedy_path) } else { greedy_path };
        let is_optimal = final_path.len() - 1 < greedy_jumps;

        // ── Fuel simulation over the chosen path ─────────────────────────────
        let mut tank = params.tank_fuel;
        let mut market = params.stored_tritium;
        let mut total_fuel_used = 0.0;
        let mut jumps_json: Vec<serde_json::Value> = Vec::with_capacity(final_path.len());

        for step in 0..final_path.len() {
            let (nid, ref nname, nx, ny, nz) = final_path[step];
            let dist_from_start = ((nx - x1).powi(2) + (ny - y1).powi(2) + (nz - z1).powi(2)).sqrt();
            let dist_to_dest = ((nx - x2).powi(2) + (ny - y2).powi(2) + (nz - z2).powi(2)).sqrt();

            if step == 0 {
                jumps_json.push(serde_json::json!({
                    "system": nname,
                    "id64": nid.to_string(),
                    "distance_from_start": 0.0,
                    "distance_to_destination": (dist_to_dest * 100.0).round() / 100.0,
                    "jump_distance": 0.0,
                    "fuel_used": 0.0,
                    "fuel_left_tank": tank,
                    "tritium_in_market": market,
                    "has_enough_fuel": true
                }));
            } else {
                let (_, _, px, py, pz) = final_path[step - 1];
                let jdist = ((nx - px).powi(2) + (ny - py).powi(2) + (nz - pz).powi(2)).sqrt();

                let c = base_cargo + market;
                let r = tank.max(0.0);
                let jump_fuel = (5.0 + (jdist * (c + r + carrier_base_mass)) / 200000.0).ceil();
                total_fuel_used += jump_fuel;

                tank -= jump_fuel;
                let has_enough_fuel = tank >= 0.0;

                let top_off = (max_tank_capacity - tank).max(0.0).min(market);
                tank += top_off;
                market -= top_off;

                jumps_json.push(serde_json::json!({
                    "system": nname,
                    "id64": nid.to_string(),
                    "distance_from_start": (dist_from_start * 100.0).round() / 100.0,
                    "distance_to_destination": (dist_to_dest * 100.0).round() / 100.0,
                    "jump_distance": (jdist * 100.0).round() / 100.0,
                    "fuel_used": jump_fuel,
                    "fuel_left_tank": tank,
                    "tritium_in_market": market,
                    "has_enough_fuel": has_enough_fuel
                }));
            }
        }

        Ok(serde_json::json!({
            "source": params.current_system,
            "destination": params.destination,
            "is_squadron": is_squadron,
            "optimised": is_optimal,
            "total_distance_ly": (total_distance * 100.0).round() / 100.0,
            "base_cargo_capacity_used": base_cargo,
            "initial_fuel_tank": params.tank_fuel,
            "initial_market_tritium": params.stored_tritium,
            "total_fuel_used": total_fuel_used,
            "final_fuel_tank": tank,
            "final_market_tritium": market,
            "totalJumps": jumps_json.len().saturating_sub(1),
            "route": jumps_json
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
// NEUTRON ROUTING — Segmented greedy + per-segment A* refinement
// =============================================================================
//
// Long routes (> SEG_LY) are split into ~2000 LY segments. For each segment:
//   1. Find the nearest neutron star to the segment endpoint (DB query, O(30k))
//   2. Preload neutron stars in that segment's local corridor (fast, bounded)
//   3. Run greedy through segment → guaranteed route in milliseconds
//   4. Run A* refinement within remaining time budget → fewer jumps if possible
//
// Short routes (≤ SEG_LY) run as a single segment.
//
// The client ALWAYS gets a response. Worst case is the greedy route.
// The greedy for a 22,000 LY route completes in <2s total across all segments.
// =============================================================================

const REFINE_BUDGET_MS: u128 = 1_800_000; // 30 minutes
const SEG_LY: f64 = 2000.0;

async fn do_neutron_route(
    state: Arc<AppState>,
    params: NeutronRouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.astar_semaphore.acquire().await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded — A* queue full".into()))?;
    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let t_start = Instant::now();

        // ── A* priority node ─────────────────────────────────────────────────
        #[derive(Clone)]
        struct HNode { g: u32, f: f64, id: i64 }
        impl PartialEq for HNode { fn eq(&self, o: &Self) -> bool { self.id == o.id } }
        impl Eq for HNode {}
        impl PartialOrd for HNode { fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) } }
        impl Ord for HNode {
            fn cmp(&self, o: &Self) -> Ordering { o.f.partial_cmp(&self.f).unwrap_or(Ordering::Equal) }
        }

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

        // ── Build waypoint list ───────────────────────────────────────────────
        // Each waypoint is the nearest neutron star to a point SEG_LY along
        // the straight line. Final waypoint is always the actual destination.
        // DB query: full scan of neutron_systems (~30k rows) per waypoint — fast.
        let num_segs = (total_distance / SEG_LY).ceil() as usize;
        let dv = (x2-x1, y2-y1, z2-z1);

        // Returns (id64, name, x, y, z) of nearest neutron to (tx,ty,tz)
        let nearest_neutron = |tx: f64, ty: f64, tz: f64| -> Option<(i64, String, f64, f64, f64)> {
            // Search in expanding bbox rings — avoids full table scan
            for radius in [300.0f64, 800.0, 1500.0, 3000.0] {
                let result: rusqlite::Result<(i64, String, f64, f64, f64)> = conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                     FROM neutron_systems ns
                     JOIN systems_index i ON ns.systemId64 = i.id
                     JOIN systems s ON ns.systemId64 = s.id64
                     WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
                     ORDER BY (i.minX-?)*(i.minX-?) + (i.minY-?)*(i.minY-?) + (i.minZ-?)*(i.minZ-?)
                     LIMIT 1",
                    rusqlite::params![
                        tx-radius, tx+radius, ty-radius, ty+radius, tz-radius, tz+radius,
                        tx, tx, ty, ty, tz, tz
                    ],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                );
                if let Ok(row) = result { return Some(row); }
            }
            None
        };

        // waypoints[i] = (id64, name, x, y, z) — intermediate + final destination
        let mut waypoints: Vec<(i64, String, f64, f64, f64)> = Vec::new();
        if num_segs > 1 {
            for i in 1..num_segs {
                let t = (i as f64 * SEG_LY) / total_distance;
                let (wx, wy, wz) = (x1 + t*dv.0, y1 + t*dv.1, z1 + t*dv.2);
                if let Some(wp) = nearest_neutron(wx, wy, wz) {
                    // Don't re-add the same waypoint twice if two segments land near the same neutron
                    if waypoints.last().map(|w: &(i64,_,_,_,_)| w.0) != Some(wp.0) {
                        waypoints.push(wp);
                    }
                }
            }
        }
        waypoints.push((dst_id, dst_name.clone(), x2, y2, z2));

        info!("Segmented into {} waypoints for {:.0} LY route", waypoints.len(), total_distance);

        // ── Per-segment routing helpers ───────────────────────────────────────

        // Load neutron stars in the corridor from (ax,ay,az) → (bx,by,bz)
        let load_segment_neutrons = |ax: f64, ay: f64, az: f64, bx: f64, by: f64, bz: f64|
            -> Result<Vec<(i64, String, f64, f64, f64)>, String>
        {
            let sdv = (bx-ax, by-ay, bz-az);
            let sdv_len_sq = sdv.0*sdv.0 + sdv.1*sdv.1 + sdv.2*sdv.2;
            let corridor_half = (boosted_range * 3.0).max(1500.0);
            let corridor_sq = corridor_half * corridor_half;
            let buf = corridor_half;

            let in_seg_corridor = |x: f64, y: f64, z: f64| -> bool {
                if sdv_len_sq < 1.0 { return true; }
                let w = (x-ax, y-ay, z-az);
                let t = ((w.0*sdv.0 + w.1*sdv.1 + w.2*sdv.2) / sdv_len_sq).clamp(0.0, 1.0);
                let (px, py, pz) = (ax+t*sdv.0, ay+t*sdv.1, az+t*sdv.2);
                (x-px).powi(2)+(y-py).powi(2)+(z-pz).powi(2) <= corridor_sq
            };

            let mut stmt = conn.prepare("
                SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                FROM neutron_systems ns
                JOIN systems_index i ON ns.systemId64 = i.id
                JOIN systems s ON ns.systemId64 = s.id64
                WHERE i.minX BETWEEN ? AND ?
                  AND i.minY BETWEEN ? AND ?
                  AND i.minZ BETWEEN ? AND ?
            ").map_err(|e| e.to_string())?;

            let rows: Vec<(i64, String, f64, f64, f64)> = stmt.query_map(
                rusqlite::params![
                    ax.min(bx)-buf, ax.max(bx)+buf,
                    ay.min(by)-buf, ay.max(by)+buf,
                    az.min(bz)-buf, az.max(bz)+buf,
                ],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            ).map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .filter(|&(_, _, x, y, z)| in_seg_corridor(x, y, z))
            .collect();

            Ok(rows)
        };

        // Normal-star fallback DB query
        let mut normal_stmt = conn.prepare("
            SELECT s.id64, s.name, i.minX, i.minY, i.minZ
            FROM systems_index i JOIN systems s ON i.id = s.id64
            WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
        ").map_err(|e| e.to_string())?;

        // Cache for non-neutron node data
        let mut normal_data: HashMap<i64, (String, f64, f64, f64)> = HashMap::new();
        // All node positions (neutron + normal), for A* lookups across segments
        let mut all_node_pos: HashMap<i64, (f64, f64, f64)> = HashMap::new();
        let mut all_node_name: HashMap<i64, String> = HashMap::new();
        let mut all_node_is_neutron: HashMap<i64, bool> = HashMap::new();
        all_node_pos.insert(src_id, (x1, y1, z1));
        all_node_name.insert(src_id, src_name.clone());
        all_node_pos.insert(dst_id, (x2, y2, z2));
        all_node_name.insert(dst_id, dst_name.clone());

        // ── Greedy through all waypoints ──────────────────────────────────────
        let mut full_path: Vec<i64> = vec![src_id];
        let mut current_pos = (x1, y1, z1);
        let mut current_id = src_id;

        for (wp_id, wp_name, wx, wy, wz) in &waypoints {
            let (wp_id, wx, wy, wz) = (*wp_id, *wx, *wy, *wz);

            // Load neutrons for this segment
            let seg_neutrons = load_segment_neutrons(
                current_pos.0, current_pos.1, current_pos.2, wx, wy, wz
            )?;

            let seg_id_to_idx: HashMap<i64, usize> = seg_neutrons.iter().enumerate()
                .map(|(i, (id, ..))| (*id, i)).collect();

            // Register all segment neutrons in global maps
            for (nid, nname, nx, ny, nz) in &seg_neutrons {
                all_node_pos.insert(*nid, (*nx, *ny, *nz));
                all_node_name.insert(*nid, nname.clone());
                all_node_is_neutron.insert(*nid, true);
            }
            all_node_pos.insert(wp_id, (wx, wy, wz));
            all_node_name.insert(wp_id, wp_name.clone());

            // Build segment spatial grid
            let cell_size = (boosted_range * 0.9).max(50.0);
            let mut seg_grid: HashMap<(i32,i32,i32), Vec<usize>> = HashMap::new();
            for (i, &(_, _, nx, ny, nz)) in seg_neutrons.iter().enumerate() {
                let cell = ((nx/cell_size) as i32, (ny/cell_size) as i32, (nz/cell_size) as i32);
                seg_grid.entry(cell).or_default().push(i);
            }

            let _cur_is_neutron = current_id == src_id && seg_id_to_idx.contains_key(&src_id)
                || seg_id_to_idx.contains_key(&current_id)
                || all_node_is_neutron.get(&current_id).copied().unwrap_or(false);

            let neutrons_near_seg = |cx: f64, cy: f64, cz: f64, range: f64, excl: i64| -> Vec<usize> {
                let (bx, by, bz) = ((cx/cell_size) as i32, (cy/cell_size) as i32, (cz/cell_size) as i32);
                let rsq = range * range;
                let mut out = Vec::new();
                for dx in -2i32..=2 { for dy in -2i32..=2 { for dz in -2i32..=2 {
                    if let Some(v) = seg_grid.get(&(bx+dx, by+dy, bz+dz)) {
                        for &i in v {
                            if seg_neutrons[i].0 == excl { continue; }
                            let d2 = (seg_neutrons[i].2-cx).powi(2)
                                    +(seg_neutrons[i].3-cy).powi(2)
                                    +(seg_neutrons[i].4-cz).powi(2);
                            if d2 <= rsq && d2 > 0.0 { out.push(i); }
                        }
                    }
                }}}
                out
            };

            // Greedy to this waypoint.
            // Use a small recency window instead of a full visited set.
            // A full visited set causes permanent stuck when all unvisited
            // neutrons in a corridor are exhausted — valid escape paths through
            // already-visited stars in a different direction become inaccessible.
            // The window blocks only the last RECENT_WINDOW hops to prevent
            // micro-loops while keeping the full graph traversable.
            const RECENT_WINDOW: usize = 8;
            let mut recent: std::collections::VecDeque<i64> = std::collections::VecDeque::with_capacity(RECENT_WINDOW + 1);
            recent.push_back(current_id);

            'seg_greedy: for _ in 0..10000usize {
                let is_n = seg_id_to_idx.contains_key(&current_id)
                    || all_node_is_neutron.get(&current_id).copied().unwrap_or(false);
                let jump_range = if is_n { boosted_range } else { params.range };

                let d_wp = ((current_pos.0-wx).powi(2)+(current_pos.1-wy).powi(2)+(current_pos.2-wz).powi(2)).sqrt();
                if d_wp <= jump_range {
                    full_path.push(wp_id);
                    current_pos = (wx, wy, wz);
                    current_id = wp_id;
                    break 'seg_greedy;
                }

                // Best neutron toward waypoint (not in recent window)
                let candidates = neutrons_near_seg(current_pos.0, current_pos.1, current_pos.2, jump_range, current_id);
                let best_n = candidates.iter()
                    .filter(|&&i| !recent.contains(&seg_neutrons[i].0))
                    .min_by(|&&a, &&b| {
                        let da = (seg_neutrons[a].2-wx).powi(2)+(seg_neutrons[a].3-wy).powi(2)+(seg_neutrons[a].4-wz).powi(2);
                        let db = (seg_neutrons[b].2-wx).powi(2)+(seg_neutrons[b].3-wy).powi(2)+(seg_neutrons[b].4-wz).powi(2);
                        da.partial_cmp(&db).unwrap_or(Ordering::Equal)
                    });

                if let Some(&idx) = best_n {
                    let n = &seg_neutrons[idx];
                    if recent.len() >= RECENT_WINDOW { recent.pop_front(); }
                    recent.push_back(n.0);
                    full_path.push(n.0);
                    current_pos = (n.2, n.3, n.4);
                    current_id = n.0;
                } else {
                    // Normal-star DB fallback.
                    // Use jump_range (not params.range) — if we are on a neutron we can
                    // still make a full boosted jump to reach a normal star.
                    // If nothing is found, expand the search radius progressively up to
                    // boosted_range * 1.5 — this handles thin regions where even the
                    // nearest star is slightly outside the nominal range.
                    let search_radii = [jump_range, boosted_range, boosted_range * 1.5];
                    let mut made_move = false;

                    'expand: for &r in &search_radii {
                        let db_rows: Vec<(i64, String, f64, f64, f64)> = normal_stmt.query_map(
                            rusqlite::params![
                                current_pos.0-r, current_pos.0+r,
                                current_pos.1-r, current_pos.1+r,
                                current_pos.2-r, current_pos.2+r,
                            ],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
                        ).map_err(|e| e.to_string())?
                        .filter_map(|r| r.ok())
                        .filter(|&(nid, _, nx, ny, nz): &(i64, String, f64, f64, f64)| {
                            if nid == current_id || recent.contains(&nid) { return false; }
                            let d2 = (nx-current_pos.0).powi(2)+(ny-current_pos.1).powi(2)+(nz-current_pos.2).powi(2);
                            d2 <= r*r && d2 > 0.0
                        })
                        .collect();

                        let best = db_rows.iter().min_by(|a, b| {
                            let da = (a.2-wx).powi(2)+(a.3-wy).powi(2)+(a.4-wz).powi(2);
                            let db = (b.2-wx).powi(2)+(b.3-wy).powi(2)+(b.4-wz).powi(2);
                            da.partial_cmp(&db).unwrap_or(Ordering::Equal)
                        });

                        if let Some((nid, nname, nx, ny, nz)) = best {
                            let nid = *nid;
                            normal_data.entry(nid).or_insert((nname.clone(), *nx, *ny, *nz));
                            all_node_pos.insert(nid, (*nx, *ny, *nz));
                            all_node_name.insert(nid, nname.clone());
                            all_node_is_neutron.insert(nid, false);
                            if recent.len() >= RECENT_WINDOW { recent.pop_front(); }
                            recent.push_back(nid);
                            full_path.push(nid);
                            current_pos = (*nx, *ny, *nz);
                            current_id = nid;
                            made_move = true;
                            break 'expand;
                        }
                    }

                    if !made_move {
                        // Genuinely no stars in any direction within 1.5x boosted range.
                        // Skip this waypoint and let the next segment try from here.
                        break 'seg_greedy;
                    }
                }
            }
        }

        // If we never reached the destination, it means greedy got stuck on the
        // final waypoint segment. This should be extremely rare but handle it:
        // the path is still valid up to wherever we got — return what we have
        // rather than an error, letting the user know it is a partial route.
        if full_path.last() != Some(&dst_id) {
            // Last-ditch: try to reach destination directly from wherever we ended up
            let (lx, ly, lz) = all_node_pos.get(full_path.last().unwrap_or(&src_id))
                .copied().unwrap_or((x1, y1, z1));
            let last_is_n = all_node_is_neutron.get(full_path.last().unwrap_or(&src_id))
                .copied().unwrap_or(false);
            let last_range = if last_is_n { boosted_range } else { params.range };
            let d_final = ((lx-x2).powi(2)+(ly-y2).powi(2)+(lz-z2).powi(2)).sqrt();
            if d_final <= last_range * 1.5 {
                full_path.push(dst_id);
            } else {
                return Err(format!(
                    "Partial route only: reached {:.0} LY from destination but could not bridge                      the final gap. Base {:.2} LY / boosted {:.2} LY.                      Try a ship with longer jump range.",
                    d_final, params.range, boosted_range
                ));
            }
        }

        let greedy_jumps = full_path.len() - 1;
        info!("Greedy: {} jumps in {}ms", greedy_jumps, t_start.elapsed().as_millis());

        let use_astar = params.engine.as_deref().unwrap_or("astar").to_lowercase() != "greedy";
        let mut astar_result: Option<Vec<i64>> = None;

        if !use_astar {
            info!("Engine: greedy only, skipping A* refinement ({} jumps)", greedy_jumps);
        }

        // ── Full corridor preload for A* ──────────────────────────────────────
        // Load ALL neutron stars within a corridor around the straight-line path
        // src→dst so A* has the complete graph, not just nodes greedy visited.
        // ~100k neutrons at most on a 22k LY route — safe on 16GB RAM (~80MB).
        if use_astar {
        { // preload inner scope
            let corridor_half = (boosted_range * 6.0).max(3000.0);
            let corridor_sq = corridor_half * corridor_half;
            let buf = corridor_half;

            let dv = (x2 - x1, y2 - y1, z2 - z1);
            let dv_len_sq = dv.0*dv.0 + dv.1*dv.1 + dv.2*dv.2;

            let mut stmt = conn.prepare("
                SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                FROM neutron_systems ns
                JOIN systems_index i ON ns.systemId64 = i.id
                JOIN systems s ON ns.systemId64 = s.id64
                WHERE i.minX BETWEEN ? AND ?
                  AND i.minY BETWEEN ? AND ?
                  AND i.minZ BETWEEN ? AND ?
            ").map_err(|e| e.to_string())?;

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
                (nx-px).powi(2) + (ny-py).powi(2) + (nz-pz).powi(2) <= corridor_sq
            })
            .collect();

            info!("A* corridor preload: {} neutron stars", rows.len());

            for (nid, nname, nx, ny, nz) in rows {
                all_node_pos.entry(nid).or_insert((nx, ny, nz));
                all_node_name.entry(nid).or_insert(nname);
                all_node_is_neutron.entry(nid).or_insert(true);
            }
        } // end preload inner scope

        let t_astar_start = Instant::now(); // start AFTER preload DB query

        // ── A* refinement (time-bounded, 30 minutes) ─────────────────────────
        // Uses greedy jump count as hard upper bound on g.
        // Full corridor is preloaded so A* can find shortcuts greedy missed.
        let h_fn = |x: f64, y: f64, z: f64| -> f64 {
            (((x-x2).powi(2)+(y-y2).powi(2)+(z-z2).powi(2)).sqrt() / boosted_range).ceil()
        };

        // Build spatial grid from full preloaded corridor
        let cell_size = (boosted_range * 0.9).max(50.0);
        let mut astar_grid: HashMap<(i32,i32,i32), Vec<i64>> = HashMap::new();
        for (&nid, &(nx, ny, nz)) in &all_node_pos {
            if all_node_is_neutron.get(&nid).copied().unwrap_or(false) {
                let cell = ((nx/cell_size) as i32, (ny/cell_size) as i32, (nz/cell_size) as i32);
                astar_grid.entry(cell).or_default().push(nid);
            }
        }

        // ── A* search: bidirectional for >10k LY, unidirectional otherwise ────
        astar_result = if total_distance > 10_000.0 {
            // ── Bidirectional A* ─────────────────────────────────────────────
            // Forward search: src → dst   (h = dist_to_dst / boosted_range)
            // Backward search: dst → src  (h = dist_to_src / boosted_range)
            // Graph is symmetric so backward uses same edge set.
            // Terminate when fwd_open.min_f + bwd_open.min_f >= mu,
            // where mu = best complete path found through any meeting node.
            // Path reconstruction: fwd_came_from chain (src→m) + bwd_came_from chain (m→dst).
            info!("Using bidirectional A* for {:.0} LY route", total_distance);
            let h_bwd = |x: f64, y: f64, z: f64| -> f64 {
                (((x-x1).powi(2)+(y-y1).powi(2)+(z-z1).powi(2)).sqrt() / boosted_range).ceil()
            };

            (|| {
                let mut fwd_cf: HashMap<i64, i64> = HashMap::new();
                let mut bwd_cf: HashMap<i64, i64> = HashMap::new();
                let mut fwd_g: HashMap<i64, u32> = HashMap::new();
                let mut bwd_g: HashMap<i64, u32> = HashMap::new();
                let mut fwd_closed: HashSet<i64> = HashSet::new();
                let mut bwd_closed: HashSet<i64> = HashSet::new();
                let mut fwd_open: BinaryHeap<HNode> = BinaryHeap::new();
                let mut bwd_open: BinaryHeap<HNode> = BinaryHeap::new();

                // Seed forward from the first neutron in the greedy path,
                // and backward from the last neutron. This skips the normal-star
                // bridge hops at each end (where A* is blind — astar_grid is
                // neutrons only, and Sol etc have no neutrons within base range).
                // The bridge is fixed from greedy; A* optimises the neutron highway.
                let fwd_seed: (i64, u32) = full_path.iter().enumerate()
                    .find(|(_, &id)| all_node_is_neutron.get(&id).copied().unwrap_or(false))
                    .map(|(g, &id)| (id, g as u32))
                    .unwrap_or((src_id, 0));

                let bwd_seed: (i64, u32) = full_path.iter().enumerate().rev()
                    .find(|(_, &id)| all_node_is_neutron.get(&id).copied().unwrap_or(false))
                    .map(|(g, &id)| (id, (full_path.len() - 1 - g) as u32))
                    .unwrap_or((dst_id, 0));

                // Pre-close all greedy nodes before the fwd_seed so A* doesn't
                // redundantly re-explore the non-neutron bridge.
                let fwd_bridge_end_idx = full_path.iter().position(|&id| id == fwd_seed.0).unwrap_or(0);
                let bwd_bridge_end_idx = full_path.iter().rposition(|&id| id == bwd_seed.0).unwrap_or(full_path.len()-1);
                for &id in &full_path[..fwd_bridge_end_idx] { fwd_closed.insert(id); }
                for &id in &full_path[bwd_bridge_end_idx+1..] { bwd_closed.insert(id); }

                let (fwd_seed_id, fwd_seed_g) = fwd_seed;
                let (bwd_seed_id, bwd_seed_g) = bwd_seed;
                let (fsx, fsy, fsz) = all_node_pos.get(&fwd_seed_id).copied().unwrap_or((x1, y1, z1));
                let (bsx, bsy, bsz) = all_node_pos.get(&bwd_seed_id).copied().unwrap_or((x2, y2, z2));

                fwd_g.insert(fwd_seed_id, fwd_seed_g);
                bwd_g.insert(bwd_seed_id, bwd_seed_g);
                fwd_open.push(HNode { g: fwd_seed_g, f: fwd_seed_g as f64 + h_fn(fsx, fsy, fsz), id: fwd_seed_id });
                bwd_open.push(HNode { g: bwd_seed_g, f: bwd_seed_g as f64 + h_bwd(bsx, bsy, bsz), id: bwd_seed_id });

                // Also register the bridge nodes in came_from so path reconstruction works
                for i in 0..fwd_bridge_end_idx {
                    if i + 1 < full_path.len() { fwd_cf.insert(full_path[i+1], full_path[i]); }
                    fwd_g.insert(full_path[i], i as u32);
                }
                for i in (bwd_bridge_end_idx+1..full_path.len()).rev() {
                    if i > 0 { bwd_cf.insert(full_path[i-1], full_path[i]); }
                    bwd_g.insert(full_path[i], (full_path.len()-1-i) as u32);
                }

                let mut mu: u32 = greedy_jumps as u32;
                let mut best_meeting: Option<i64> = None;

                // Helper: expand one node from a given direction.
                // Updates mu / best_meeting when a meeting node is found.
                // Returns false if open set is empty.
                loop {
                    if t_astar_start.elapsed().as_millis() > REFINE_BUDGET_MS { break; }
                    if fwd_open.is_empty() && bwd_open.is_empty() { break; }

                    // Termination: use min g-scores (not f-scores) to bound remaining path length.
                    // f = g + h so using f would fire immediately on the first iteration.
                    let fwd_min_g = fwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                    let bwd_min_g = bwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                    if fwd_min_g.saturating_add(bwd_min_g) >= mu { break; }

                    // Expand the direction with smaller min-f (for heuristic guidance)
                    let fwd_min_f = fwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                    let bwd_min_f = bwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                    let expand_fwd = fwd_min_f <= bwd_min_f;

                    macro_rules! expand_neighbors {
                        ($cx:expr, $cy:expr, $cz:expr, $jump_range:expr, $id:expr,
                         $my_g:expr, $my_g_map:expr, $my_cf:expr, $my_open:expr,
                         $my_closed:expr, $other_g_map:expr, $h_fn_local:expr) => {{
                            let (bx, by, bz) = (($cx/cell_size) as i32, ($cy/cell_size) as i32, ($cz/cell_size) as i32);
                            let rsq = $jump_range * $jump_range;
                            for dx in -2i32..=2 { for dy in -2i32..=2 { for dz in -2i32..=2 {
                                if let Some(v) = astar_grid.get(&(bx+dx, by+dy, bz+dz)) {
                                    for &n_id in v {
                                        if n_id == $id || $my_closed.contains(&n_id) { continue; }
                                        let (nx, ny, nz) = match all_node_pos.get(&n_id) { Some(&p) => p, None => continue };
                                        let d2 = (nx-$cx).powi(2)+(ny-$cy).powi(2)+(nz-$cz).powi(2);
                                        if d2 > rsq || d2 == 0.0 { continue; }
                                        let tg = $my_g + 1;
                                        if tg < *$my_g_map.get(&n_id).unwrap_or(&u32::MAX) {
                                            $my_g_map.insert(n_id, tg);
                                            $my_cf.insert(n_id, $id);
                                            $my_open.push(HNode { g: tg, f: tg as f64 + $h_fn_local(nx, ny, nz), id: n_id });
                                            // Check for meeting
                                            if let Some(&og) = $other_g_map.get(&n_id) {
                                                let total = tg + og;
                                                if total < mu {
                                                    mu = total;
                                                    best_meeting = Some(n_id);
                                                }
                                            }
                                        }
                                    }
                                }
                            }}}
                        }};
                    }

                    if expand_fwd {
                        let Some(HNode { g, id, .. }) = fwd_open.pop() else { continue; };
                        if g >= mu { continue; }
                        if fwd_closed.contains(&id) { continue; }
                        fwd_closed.insert(id);

                        // Meeting check on expansion
                        if let Some(&bg) = bwd_g.get(&id) {
                            let total = g + bg;
                            if total < mu { mu = total; best_meeting = Some(id); }
                        }

                        let (cx, cy, cz) = match all_node_pos.get(&id) { Some(&p) => p, None => continue };
                        let is_n = all_node_is_neutron.get(&id).copied().unwrap_or(false);
                        let jump_range = if is_n { boosted_range } else { params.range };

                        // Direct reach of dst?
                        let d_dst = ((cx-x2).powi(2)+(cy-y2).powi(2)+(cz-z2).powi(2)).sqrt();
                        if d_dst <= jump_range {
                            let tg = g + 1;
                            if tg < *fwd_g.get(&dst_id).unwrap_or(&u32::MAX) {
                                fwd_g.insert(dst_id, tg);
                                fwd_cf.insert(dst_id, id);
                                fwd_open.push(HNode { g: tg, f: tg as f64, id: dst_id });
                                // dst has bwd_g = 0, total = tg
                                if tg < mu { mu = tg; best_meeting = Some(dst_id); }
                            }
                        }

                        expand_neighbors!(cx, cy, cz, jump_range, id,
                            g, fwd_g, fwd_cf, fwd_open, fwd_closed, bwd_g, h_fn);
                    } else {
                        let Some(HNode { g, id, .. }) = bwd_open.pop() else { continue; };
                        if g >= mu { continue; }
                        if bwd_closed.contains(&id) { continue; }
                        bwd_closed.insert(id);

                        if let Some(&fg) = fwd_g.get(&id) {
                            let total = fg + g;
                            if total < mu { mu = total; best_meeting = Some(id); }
                        }

                        let (cx, cy, cz) = match all_node_pos.get(&id) { Some(&p) => p, None => continue };
                        let is_n = all_node_is_neutron.get(&id).copied().unwrap_or(false);
                        let jump_range = if is_n { boosted_range } else { params.range };

                        // Direct reach of src?
                        let d_src = ((cx-x1).powi(2)+(cy-y1).powi(2)+(cz-z1).powi(2)).sqrt();
                        if d_src <= jump_range {
                            let tg = g + 1;
                            if tg < *bwd_g.get(&src_id).unwrap_or(&u32::MAX) {
                                bwd_g.insert(src_id, tg);
                                bwd_cf.insert(src_id, id);
                                bwd_open.push(HNode { g: tg, f: tg as f64, id: src_id });
                                if tg < mu { mu = tg; best_meeting = Some(src_id); }
                            }
                        }

                        expand_neighbors!(cx, cy, cz, jump_range, id,
                            g, bwd_g, bwd_cf, bwd_open, bwd_closed, fwd_g, h_bwd);
                    }
                }

                // Reconstruct path through best meeting node
                let m = best_meeting?;

                // Forward half: src → m
                let mut fwd_path: Vec<i64> = vec![m];
                let mut cur = m;
                while cur != src_id {
                    match fwd_cf.get(&cur) {
                        Some(&p) => { cur = p; fwd_path.push(cur); }
                        None => return None,
                    }
                }
                fwd_path.reverse(); // now src → m

                // Backward half: m → dst (follow bwd_cf from m toward dst)
                let mut bwd_path: Vec<i64> = Vec::new();
                let mut cur = m;
                while cur != dst_id {
                    match bwd_cf.get(&cur) {
                        Some(&p) => { cur = p; bwd_path.push(cur); }
                        None => break,
                    }
                }
                if *bwd_path.last().unwrap_or(&m) != dst_id {
                    bwd_path.push(dst_id);
                }

                fwd_path.extend(bwd_path); // src → m → ... → dst
                Some(fwd_path)
            })()
        } else {
            // ── Unidirectional A* for routes ≤ 10k LY ───────────────────────
            (|| {
                let mut came_from: HashMap<i64, i64> = HashMap::new();
                let mut g_score: HashMap<i64, u32> = HashMap::new();
                let mut closed: HashSet<i64> = HashSet::new();
                let mut open: BinaryHeap<HNode> = BinaryHeap::new();

                // Seed from first neutron in greedy path — same bridge fix as bidirectional.
                let uni_seed: (i64, u32) = full_path.iter().enumerate()
                    .find(|(_, &id)| all_node_is_neutron.get(&id).copied().unwrap_or(false))
                    .map(|(g, &id)| (id, g as u32))
                    .unwrap_or((src_id, 0));
                let (uni_seed_id, uni_seed_g) = uni_seed;
                let (usx, usy, usz) = all_node_pos.get(&uni_seed_id).copied().unwrap_or((x1, y1, z1));

                // Pre-fill bridge nodes into came_from/g_score and close them
                let uni_bridge_end = full_path.iter().position(|&id| id == uni_seed_id).unwrap_or(0);
                for i in 0..uni_bridge_end {
                    if i + 1 < full_path.len() { came_from.insert(full_path[i+1], full_path[i]); }
                    g_score.insert(full_path[i], i as u32);
                    closed.insert(full_path[i]);
                }

                g_score.insert(uni_seed_id, uni_seed_g);
                open.push(HNode { g: uni_seed_g, f: uni_seed_g as f64 + h_fn(usx, usy, usz), id: uni_seed_id });

                while let Some(HNode { g, id, .. }) = open.pop() {
                    if t_astar_start.elapsed().as_millis() > REFINE_BUDGET_MS { return None; }
                    if g as usize >= greedy_jumps { continue; }

                    if id == dst_id {
                        let mut path = vec![dst_id];
                        let mut cur = dst_id;
                        while cur != src_id {
                            match came_from.get(&cur) {
                                Some(&p) => { cur = p; path.push(cur); }
                                None => return None,
                            }
                        }
                        path.reverse();
                        return Some(path);
                    }

                    if closed.contains(&id) { continue; }
                    closed.insert(id);

                    let (cx, cy, cz) = match all_node_pos.get(&id) { Some(&p) => p, None => continue };
                    let is_n = all_node_is_neutron.get(&id).copied().unwrap_or(false);
                    let jump_range = if is_n { boosted_range } else { params.range };

                    let d_dst = ((cx-x2).powi(2)+(cy-y2).powi(2)+(cz-z2).powi(2)).sqrt();
                    if d_dst <= jump_range && !closed.contains(&dst_id) {
                        let tg = g + 1;
                        if tg < *g_score.get(&dst_id).unwrap_or(&u32::MAX) {
                            g_score.insert(dst_id, tg);
                            came_from.insert(dst_id, id);
                            open.push(HNode { g: tg, f: tg as f64, id: dst_id });
                        }
                    }

                    {
                        let (bx, by, bz) = ((cx/cell_size) as i32, (cy/cell_size) as i32, (cz/cell_size) as i32);
                        let rsq = jump_range * jump_range;
                        for dx in -2i32..=2 { for dy in -2i32..=2 { for dz in -2i32..=2 {
                            if let Some(v) = astar_grid.get(&(bx+dx, by+dy, bz+dz)) {
                                for &n_id in v {
                                    if n_id == id || closed.contains(&n_id) { continue; }
                                    let (nx, ny, nz) = match all_node_pos.get(&n_id) { Some(&p) => p, None => continue };
                                    let d2 = (nx-cx).powi(2)+(ny-cy).powi(2)+(nz-cz).powi(2);
                                    if d2 > rsq || d2 == 0.0 { continue; }
                                    let tg = g + 1;
                                    if tg < *g_score.get(&n_id).unwrap_or(&u32::MAX) {
                                        g_score.insert(n_id, tg);
                                        came_from.insert(n_id, id);
                                        open.push(HNode { g: tg, f: tg as f64 + h_fn(nx, ny, nz), id: n_id });
                                    }
                                }
                            }
                        }}}
                    }
                }
                None
            })()
        };

        } // end if use_astar

        let final_path = if use_astar { astar_result.unwrap_or(full_path) } else { full_path };
        let elapsed = t_start.elapsed().as_millis();
        let is_optimal = final_path.len() - 1 < greedy_jumps;
        if use_astar {
            info!("Final route: {} jumps (greedy was {}), {}ms", final_path.len()-1, greedy_jumps, elapsed);
        }

        // ── Build JSON response ───────────────────────────────────────────────
        let mut route_json: Vec<serde_json::Value> = Vec::with_capacity(final_path.len());
        let mut dist_from_start = 0.0f64;

        for step in 0..final_path.len() {
            let nid = final_path[step];
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

            let (jdist, used_boost) = if step + 1 < final_path.len() {
                let next_id = final_path[step + 1];
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
            "optimised":         is_optimal,
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

// =============================================================================
// EDDN — Elite Dangerous Data Network live feed
// =============================================================================
//
// EDDN is the public ZeroMQ pub/sub relay where every player's game client
// (via EDMC, EDDiscovery, etc.) broadcasts their journal events with PII
// stripped. Subscribing to it gets us live system / body / station updates
// in the seconds after a player jumps, scans, or docks — so we don't have
// to lean entirely on the 6-hourly Spansh dump.
//
// Wire format: each ZMQ frame is a single zlib-compressed JSON envelope:
//   { "$schemaRef": "https://eddn.edcd.io/schemas/journal/1",
//     "header": { uploaderID, softwareName, gatewayTimestamp, ... },
//     "message": <the actual journal event with location-leaking fields stripped> }
//
// We translate journal field names (TitleCase, journal-native units) into
// the Spansh schema (camelCase, normalised units) and feed everything
// through the same writer channel the Spansh dump and EDMC endpoint use.
// The writer's COALESCE upserts mean partial events (e.g. a Scan with no
// system metadata) won't blow away rich data already in the DB.
//
// Cargo.toml additions required:
//   zmq = "0.10"     (also needs system libzmq3-dev / libzmq-dev installed)
// flate2 is already in the dep tree for the Spansh gz dump.
// =============================================================================

/// Decompress an EDDN frame (zlib).
fn eddn_decompress(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 4);
    ZlibDecoder::new(input).read_to_end(&mut out)?;
    Ok(out)
}

/// Strip Frontier's `$token_value;` localisation wrapper, leaving just `value`.
/// Frontier sends two copies of localised strings — the raw token like
/// `$government_Democracy;` and a `*_Localised` field with `Democracy`. When
/// only the raw token is present we fall back to this.
fn strip_dollar_token(s: &str) -> String {
    let s = s.trim_start_matches('$').trim_end_matches(';');
    if let Some(idx) = s.find('_') {
        // "government_Democracy" -> "Democracy"
        s[idx + 1..].to_string()
    } else {
        s.to_string()
    }
}

/// Pull a localised string field, falling back to the de-tokenised raw field.
fn loc_str(msg: &serde_json::Value, raw_key: &str, loc_key: &str) -> Option<String> {
    if let Some(v) = msg.get(loc_key).and_then(|v| v.as_str()) {
        return Some(v.to_string());
    }
    msg.get(raw_key)
        .and_then(|v| v.as_str())
        .map(strip_dollar_token)
}

/// Frontier's one-letter star type → human-friendly Spansh subType. Anything
/// we don't recognise is passed through as-is (Spansh stores e.g. "AeBe Star",
/// "Black Hole", "Neutron Star" verbatim, so missing stars just degrade
/// gracefully).
fn star_type_to_subtype(t: &str) -> String {
    // Reference: https://elite-dangerous.fandom.com/wiki/Star
    let mapped = match t {
        "O"  => "O (Blue-White) Star",
        "B"  => "B (Blue-White) Star",
        "A"  => "A (Blue-White) Star",
        "F"  => "F (White) Star",
        "G"  => "G (White-Yellow) Star",
        "K"  => "K (Yellow-Orange) Star",
        "M"  => "M (Red dwarf) Star",
        "L"  => "L (Brown dwarf) Star",
        "T"  => "T (Brown dwarf) Star",
        "Y"  => "Y (Brown dwarf) Star",
        "TTS" => "T Tauri Star",
        "AeBe" => "Herbig Ae/Be Star",
        "W" | "WN" | "WNC" | "WC" | "WO" => "Wolf-Rayet Star",
        "CS" | "C" | "CN" | "CJ" | "CH" | "CHd" => "Carbon Star",
        "MS" => "MS-type Star",
        "S"  => "S-type Star",
        "N"  => "Neutron Star",
        "H"  => "Black Hole",
        "X"  => "Exotic",
        "SupermassiveBlackHole" => "Supermassive Black Hole",
        "D" | "DA" | "DAB" | "DAO" | "DAZ" | "DAV" |
        "DB" | "DBZ" | "DBV" |
        "DO" | "DOV" |
        "DQ" |
        "DC" | "DCV" |
        "DX" => "White Dwarf",
        _ => return t.to_string(),
    };
    mapped.to_string()
}

/// Build a SpanshSystem from an FSDJump / Location / CarrierJump event.
/// These three events all carry full system metadata + StarPos coords, so
/// they're the only events that emit a fully-populated system row.
fn eddn_jump_to_system(id64: i64, name: String, msg: &serde_json::Value) -> SpanshSystem {
    let coords = msg
        .get("StarPos")
        .and_then(|v| v.as_array())
        .filter(|arr| arr.len() == 3)
        .and_then(|arr| {
            Some(SpanshCoords {
                x: arr[0].as_f64()?,
                y: arr[1].as_f64()?,
                z: arr[2].as_f64()?,
            })
        });

    let allegiance = msg
        .get("SystemAllegiance")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let government       = loc_str(msg, "SystemGovernment",     "SystemGovernment_Localised");
    let primary_economy  = loc_str(msg, "SystemEconomy",        "SystemEconomy_Localised");
    let secondary_econ   = loc_str(msg, "SystemSecondEconomy",  "SystemSecondEconomy_Localised");
    let security         = loc_str(msg, "SystemSecurity",       "SystemSecurity_Localised");
    let population       = msg.get("Population").and_then(|v| v.as_i64());

    let controlling_faction = msg.get("SystemFaction").cloned()
        .filter(|v| !v.is_null());
    let factions = msg.get("Factions").cloned()
        .filter(|v| !v.is_null());
    let powers = msg.get("Powers").cloned()
        .filter(|v| !v.is_null());
    let power_state = msg.get("PowerplayState").and_then(|v| v.as_str()).map(|s| s.to_string());
    let controlling_power = msg
        .get("ControllingPower")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let p_ctrl   = msg.get("PowerplayStateControlProgress").and_then(|v| v.as_f64());
    let p_reinf  = msg.get("PowerplayStateReinforcement").and_then(|v| v.as_f64());
    let p_under  = msg.get("PowerplayStateUndermining").and_then(|v| v.as_f64());
    let p_conflict = msg.get("PowerplayConflictProgress").cloned().filter(|v| !v.is_null());
    let thargoid = msg.get("ThargoidWar").cloned().filter(|v| !v.is_null());
    let date = msg.get("timestamp").and_then(|v| v.as_str()).map(|s| s.to_string());

    SpanshSystem {
        id64,
        name,
        population,
        coords,
        bodies: None,
        stations: None,
        allegiance,
        government,
        primary_economy,
        secondary_economy: secondary_econ,
        security,
        body_count: None,
        date,
        controlling_faction,
        factions,
        power_state,
        powers,
        controlling_power,
        power_state_control_progress: p_ctrl,
        power_state_reinforcement:    p_reinf,
        power_state_undermining:      p_under,
        power_conflict_progress:      p_conflict,
        thargoid_war:                 thargoid,
    }
}

/// An empty SpanshSystem with only id64+name set. Used as the carrier when an
/// event only updates a body or a station — the writer's COALESCE upserts
/// preserve the existing system row's metadata, while the embedded body/station
/// gets attached.
fn eddn_carrier_system(id64: i64, name: String) -> SpanshSystem {
    SpanshSystem {
        id64,
        name,
        population: None,
        coords: None,
        bodies: None,
        stations: None,
        allegiance: None,
        government: None,
        primary_economy: None,
        secondary_economy: None,
        security: None,
        body_count: None,
        date: None,
        controlling_faction: None,
        factions: None,
        power_state: None,
        powers: None,
        controlling_power: None,
        power_state_control_progress: None,
        power_state_reinforcement: None,
        power_state_undermining: None,
        power_conflict_progress: None,
        thargoid_war: None,
    }
}

/// Translate a journal `Scan` event into a Spansh-shaped body JSON object.
/// Field name and unit conversions:
///   BodyID → bodyId, BodyName → name
///   PlanetClass → subType (type="Planet"); StarType → subType (type="Star")
///   DistanceFromArrivalLS → distanceToArrival (ls)
///   SurfaceGravity (m/s²) → gravity (g)
///   Radius (m) → radius (km)
///   StellarMass → solarMasses, Age_MY → age, RotationPeriod → rotationalPeriod
///   TidalLock → rotationalPeriodTidallyLocked, TerraformState → terraformingState
///   Eccentricity → orbitalEccentricity, Periapsis → argOfPeriapsis
fn eddn_scan_to_body(scan: &serde_json::Value) -> Option<serde_json::Value> {
    let body_id = scan.get("BodyID").and_then(|v| v.as_i64())?;
    let body_name = scan.get("BodyName").and_then(|v| v.as_str())?.to_string();

    let mut body = serde_json::Map::new();
    body.insert("bodyId".into(), serde_json::json!(body_id));
    body.insert("name".into(), serde_json::json!(body_name));

    let mut copy = |src: &str, dst: &str| {
        if let Some(v) = scan.get(src).filter(|v| !v.is_null()) {
            body.insert(dst.into(), v.clone());
        }
    };
    copy("DistanceFromArrivalLS", "distanceToArrival");
    copy("SurfaceTemperature",    "surfaceTemperature");
    copy("SurfacePressure",       "surfacePressure");
    copy("OrbitalPeriod",         "orbitalPeriod");
    copy("SemiMajorAxis",         "semiMajorAxis");
    copy("Eccentricity",          "orbitalEccentricity");
    copy("OrbitalInclination",    "orbitalInclination");
    copy("Periapsis",             "argOfPeriapsis");
    copy("RotationPeriod",        "rotationalPeriod");
    copy("TidalLock",             "rotationalPeriodTidallyLocked");
    copy("AxialTilt",             "axialTilt");
    copy("AscendingNode",         "ascendingNode");
    copy("MeanAnomaly",           "meanAnomaly");
    copy("WasDiscovered",         "wasDiscovered");
    copy("WasMapped",             "wasMapped");
    copy("Volcanism",             "volcanismType");
    copy("TerraformState",        "terraformingState");
    copy("AtmosphereComposition", "atmosphereComposition");
    copy("Composition",           "solidComposition");
    copy("Materials",             "materials");
    copy("Rings",                 "rings");
    copy("Parents",               "parents");
    copy("StellarMass",           "solarMasses");
    copy("AbsoluteMagnitude",     "absoluteMagnitude");
    copy("Age_MY",                "age");
    copy("Luminosity",            "luminosity");
    copy("Subclass",              "subclass");
    copy("ReserveLevel",          "reserveLevel");
    copy("MassEM",                "earthMasses");
    copy("Landable",              "isLandable");

    // Atmosphere can come as either "Atmosphere" or "AtmosphereType".
    if let Some(v) = scan.get("AtmosphereType").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
        body.insert("atmosphereType".into(), serde_json::json!(v));
    } else if let Some(v) = scan.get("Atmosphere").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
        body.insert("atmosphereType".into(), serde_json::json!(v));
    }

    // Type / subType derivation
    if let Some(planet_class) = scan.get("PlanetClass").and_then(|v| v.as_str()) {
        body.insert("type".into(), serde_json::json!("Planet"));
        body.insert("subType".into(), serde_json::json!(planet_class));
    } else if let Some(star_type) = scan.get("StarType").and_then(|v| v.as_str()) {
        body.insert("type".into(), serde_json::json!("Star"));
        body.insert("subType".into(), serde_json::json!(star_type_to_subtype(star_type)));
        body.insert("spectralClass".into(), serde_json::json!(star_type));
        if body_id == 0 {
            body.insert("mainStar".into(), serde_json::json!(true));
        }
    }
    // (Belt clusters / barycentres come through other event types.)

    // Unit conversions
    if let Some(g) = scan.get("SurfaceGravity").and_then(|v| v.as_f64()) {
        // Journal: m/s²  →  Spansh: g (≈ 9.80665 m/s²)
        body.insert("gravity".into(), serde_json::json!(g / 9.80665));
    }
    if let Some(r) = scan.get("Radius").and_then(|v| v.as_f64()) {
        // Journal: metres  →  Spansh: kilometres
        body.insert("radius".into(), serde_json::json!(r / 1000.0));
    }

    if let Some(v) = scan.get("timestamp") {
        body.insert("updateTime".into(), v.clone());
    }

    Some(serde_json::Value::Object(body))
}

/// Translate a `Docked` event into a Spansh-shaped station JSON object. We
/// use MarketID as the station id (Spansh's `id` is an EDDB internal id we
/// can't synthesise; market IDs are unique 64-bit values that won't collide
/// with EDDB ids in practice).
fn eddn_docked_to_station(d: &serde_json::Value) -> Option<serde_json::Value> {
    let market_id = d.get("MarketID").and_then(|v| v.as_i64())?;
    let station_name = d.get("StationName").and_then(|v| v.as_str())?.to_string();

    let mut st = serde_json::Map::new();
    st.insert("id".into(), serde_json::json!(market_id));
    st.insert("marketId".into(), serde_json::json!(market_id));
    st.insert("name".into(), serde_json::json!(station_name));

    if let Some(v) = d.get("StationType").and_then(|v| v.as_str()) {
        st.insert("type".into(), serde_json::json!(v));
    }
    if let Some(v) = d.get("DistFromStarLS").filter(|v| !v.is_null()) {
        st.insert("distanceToArrival".into(), v.clone());
    }
    if let Some(v) = d.get("StationAllegiance").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
        st.insert("allegiance".into(), serde_json::json!(v));
    }
    if let Some(g) = loc_str(d, "StationGovernment", "StationGovernment_Localised") {
        st.insert("government".into(), serde_json::json!(g));
    }
    if let Some(e) = loc_str(d, "StationEconomy", "StationEconomy_Localised") {
        st.insert("primaryEconomy".into(), serde_json::json!(e));
    }
    if let Some(v) = d.get("StationServices").filter(|v| !v.is_null()) {
        st.insert("services".into(), v.clone());
    }
    if let Some(v) = d.get("StationFaction").and_then(|f| f.get("Name")).and_then(|n| n.as_str()) {
        st.insert("controllingFaction".into(), serde_json::json!(v));
    }
    if let Some(v) = d.get("StationFaction").and_then(|f| f.get("FactionState")).and_then(|n| n.as_str()) {
        st.insert("controllingFactionState".into(), serde_json::json!(v));
    }
    if let Some(v) = d.get("LandingPads").filter(|v| !v.is_null()) {
        st.insert("landingPads".into(), v.clone());
    }
    if let Some(v) = d.get("StationEconomies").filter(|v| !v.is_null()) {
        st.insert("economies".into(), v.clone());
    }
    if let Some(v) = d.get("timestamp") {
        st.insert("updateTime".into(), v.clone());
    }

    Some(serde_json::Value::Object(st))
}

/// Translate an SAASignalsFound event to a body patch with just signals data.
/// The journal sends `Signals: [{Type, Type_Localised, Count}, ...]` plus
/// optional `Genuses`. We pass the array through; the existing reader code
/// (e.g. coloslots.py) already handles both array and dict formats.
fn eddn_signals_to_body(s: &serde_json::Value) -> Option<serde_json::Value> {
    let body_id = s.get("BodyID").and_then(|v| v.as_i64())?;
    let body_name = s.get("BodyName").and_then(|v| v.as_str()).map(|s| s.to_string());
    let signals = s.get("Signals").cloned().filter(|v| !v.is_null());
    signals.as_ref()?; // bail if no signals

    let mut body = serde_json::Map::new();
    body.insert("bodyId".into(), serde_json::json!(body_id));
    if let Some(n) = body_name {
        body.insert("name".into(), serde_json::json!(n));
    }
    if let Some(sig) = signals {
        body.insert("signals".into(), sig);
    }
    if let Some(v) = s.get("timestamp") {
        body.insert("updateTime".into(), v.clone());
    }
    Some(serde_json::Value::Object(body))
}

/// Dispatch a journal-schema EDDN message to the right translator. Returns
/// None for events we don't ingest (StartUp, Music, ShipyardSell, …) — there
/// are far more journal events than we need for the galaxy DB.
fn eddn_handle_journal(msg: &serde_json::Value) -> Option<SpanshSystem> {
    let event = msg.get("event")?.as_str()?;
    let id64 = msg.get("SystemAddress").and_then(|v| v.as_i64())?;
    let name = msg.get("StarSystem").and_then(|v| v.as_str())?.to_string();
    if name.is_empty() {
        return None;
    }

    match event {
        "FSDJump" | "Location" | "CarrierJump" => {
            Some(eddn_jump_to_system(id64, name, msg))
        }
        "Scan" => {
            let body = eddn_scan_to_body(msg)?;
            let mut sys = eddn_carrier_system(id64, name);
            sys.bodies = Some(vec![body]);
            Some(sys)
        }
        "Docked" => {
            let station = eddn_docked_to_station(msg)?;
            let mut sys = eddn_carrier_system(id64, name);
            sys.stations = Some(vec![station]);
            Some(sys)
        }
        "SAASignalsFound" => {
            let body = eddn_signals_to_body(msg)?;
            let mut sys = eddn_carrier_system(id64, name);
            sys.bodies = Some(vec![body]);
            Some(sys)
        }
        _ => None,
    }
}

/// Top-level dispatcher. For now we only consume the journal schema — other
/// schemas (commodity/3, outfitting/2, shipyard/2, navroute/1, etc.) carry
/// useful info but populate fields the current DB doesn't model; adding them
/// later is a matter of a new arm here.
fn eddn_handle_envelope(envelope: &serde_json::Value) -> Option<SpanshSystem> {
    let schema = envelope.get("$schemaRef").and_then(|v| v.as_str())?;
    let message = envelope.get("message")?;

    if schema.starts_with("https://eddn.edcd.io/schemas/journal/") {
        return eddn_handle_journal(message);
    }
    None
}

/// EDDN listener thread. Runs forever, reconnecting with exponential backoff
/// on errors. Buffers translated SpanshSystem records for up to 1 second or
/// 200 systems (whichever first), then flushes a single batch to the writer
/// so we don't open a new SQLite transaction per ZMQ frame.
fn eddn_listener_thread(
    relay_url: String,
    sender: Sender<Vec<SpanshSystem>>,
    stats: Arc<EddnStats>,
    heatmap: Arc<Heatmap>,
) {
    let mut reconnect_delay_ms = EDDN_RECONNECT_BASE_MS;

    'outer: loop {
        info!("EDDN: connecting to {}...", relay_url);
        let ctx = zmq::Context::new();
        let sock = match ctx.socket(zmq::SUB) {
            Ok(s) => s,
            Err(e) => {
                error!("EDDN: failed to create SUB socket: {}", e);
                std::thread::sleep(Duration::from_millis(reconnect_delay_ms));
                reconnect_delay_ms = (reconnect_delay_ms.saturating_mul(2)).min(EDDN_RECONNECT_MAX_MS);
                continue;
            }
        };

        // Sensible defaults for a long-lived subscriber on a Pi.
        let _ = sock.set_rcvtimeo(EDDN_RECV_TIMEOUT_MS);
        let _ = sock.set_linger(0);
        let _ = sock.set_reconnect_ivl(2_000);
        let _ = sock.set_reconnect_ivl_max(60_000);
        // Empty subscription = receive every published frame.
        if let Err(e) = sock.set_subscribe(b"") {
            error!("EDDN: set_subscribe failed: {}", e);
            std::thread::sleep(Duration::from_millis(reconnect_delay_ms));
            reconnect_delay_ms = (reconnect_delay_ms.saturating_mul(2)).min(EDDN_RECONNECT_MAX_MS);
            continue;
        }

        if let Err(e) = sock.connect(&relay_url) {
            error!("EDDN: connect failed: {}", e);
            std::thread::sleep(Duration::from_millis(reconnect_delay_ms));
            reconnect_delay_ms = (reconnect_delay_ms.saturating_mul(2)).min(EDDN_RECONNECT_MAX_MS);
            continue;
        }

        info!("EDDN: connected, subscribed to all topics");
        stats.connected.store(1, AtomicOrdering::Relaxed);
        reconnect_delay_ms = EDDN_RECONNECT_BASE_MS;

        let mut buffer: Vec<SpanshSystem> = Vec::with_capacity(EDDN_FLUSH_BATCH_SIZE);
        let mut last_flush = Instant::now();

        loop {
            // Recv with timeout so we wake up periodically to flush even when
            // the relay is quiet.
            let msg = match sock.recv_bytes(0) {
                Ok(m) => Some(m),
                Err(zmq::Error::EAGAIN) => {
                    // Timeout — no message, but still let us flush on the
                    // interval below.
                    None
                }
                Err(e) => {
                    error!("EDDN: recv error, will reconnect: {}", e);
                    stats.connected.store(0, AtomicOrdering::Relaxed);
                    break;
                }
            };

            if let Some(raw) = msg {
                stats.messages_received.fetch_add(1, AtomicOrdering::Relaxed);

                let json_bytes = match eddn_decompress(&raw) {
                    Ok(b) => b,
                    Err(e) => {
                        stats.messages_dropped.fetch_add(1, AtomicOrdering::Relaxed);
                        warn!("EDDN: zlib decompress failed ({} bytes): {}", raw.len(), e);
                        continue;
                    }
                };

                let envelope: serde_json::Value = match serde_json::from_slice(&json_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        stats.messages_dropped.fetch_add(1, AtomicOrdering::Relaxed);
                        warn!("EDDN: JSON parse failed: {}", e);
                        continue;
                    }
                };

                match eddn_handle_envelope(&envelope) {
                    Some(sys) => {
                        // Heatmap: only count true arrival events (FSDJump/
                        // Location/CarrierJump have StarPos and end up with
                        // sys.coords = Some). Scan/Docked/SAA events use the
                        // carrier-cached system (coords = None) and are
                        // skipped, so we don't multi-count one commander
                        // pinging the same system 20 times in a session.
                        if let Some(c) = sys.coords.as_ref() {
                            heatmap.bump(c.x, c.z);
                        }
                        let body_count = sys.bodies.as_ref().map(|b| b.len() as u64).unwrap_or(0);
                        let station_count = sys.stations.as_ref().map(|s| s.len() as u64).unwrap_or(0);
                        stats.messages_processed.fetch_add(1, AtomicOrdering::Relaxed);
                        stats.systems_emitted.fetch_add(1, AtomicOrdering::Relaxed);
                        stats.bodies_emitted.fetch_add(body_count, AtomicOrdering::Relaxed);
                        stats.stations_emitted.fetch_add(station_count, AtomicOrdering::Relaxed);
                        stats.last_message_time.store(current_time_secs(), AtomicOrdering::Relaxed);
                        buffer.push(sys);
                    }
                    None => {
                        // Schema we don't ingest, or unparseable journal
                        // event — silently skip. (Counted as "received" but
                        // not "processed".)
                    }
                }
            }

            // Flush conditions: buffer big enough, or interval elapsed and
            // we have something queued.
            let should_flush = buffer.len() >= EDDN_FLUSH_BATCH_SIZE
                || (!buffer.is_empty()
                    && last_flush.elapsed() >= Duration::from_millis(EDDN_FLUSH_INTERVAL_MS));

            if should_flush {
                let batch = std::mem::take(&mut buffer);
                let n = batch.len();
                match sender.try_send(batch) {
                    Ok(()) => {
                        last_flush = Instant::now();
                    }
                    Err(crossbeam_channel::TrySendError::Full(returned)) => {
                        // Writer is overloaded — block briefly. If the
                        // writer is genuinely stuck we'll back-pressure
                        // here, which is preferable to dropping data.
                        warn!("EDDN: writer queue full, blocking on send ({} systems)", n);
                        if sender.send(returned).is_err() {
                            error!("EDDN: writer channel dead, exiting listener");
                            stats.connected.store(0, AtomicOrdering::Relaxed);
                            return;
                        }
                        last_flush = Instant::now();
                    }
                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                        error!("EDDN: writer channel dead, exiting listener");
                        stats.connected.store(0, AtomicOrdering::Relaxed);
                        return;
                    }
                }
            }
        }

        // Disconnected — flush whatever's left, drop the socket, back off,
        // reconnect.
        if !buffer.is_empty() {
            let _ = sender.send(std::mem::take(&mut buffer));
        }
        stats.reconnects.fetch_add(1, AtomicOrdering::Relaxed);
        info!("EDDN: reconnecting in {}ms...", reconnect_delay_ms);
        std::thread::sleep(Duration::from_millis(reconnect_delay_ms));
        reconnect_delay_ms = (reconnect_delay_ms.saturating_mul(2)).min(EDDN_RECONNECT_MAX_MS);
        // explicit reconnect via 'outer continue
        continue 'outer;
    }
}

// =============================================================================
// COMMANDER HOTSPOT HEATMAP — visualize where commanders are active in the galaxy
// =============================================================================
//
// GET /api/heatmap.png  — 1024×1024 RGBA PNG with transparent background
// GET /heatmap          — HTML page that overlays the PNG on a galaxy backdrop
//
// Data: every system-arrival event from EDDN (FSDJump/Location/CarrierJump)
// and every EDMC submission with system coords increments one cell. Scans and
// dock events at a system already-jumped-to are NOT double-counted (no coords
// → carrier-cached system → skipped). The Spansh dump path also bypasses the
// heatmap (it's batch reference data, not commander activity).
//
// CPU/memory:
//   - Bump:    one Relaxed atomic add (~1ns). Free.
//   - Decay:   1M load+store every 5 min (~5ms). Negligible.
//   - Render:  1M cells → log+colormap → PNG encode (~50–150ms), cached 30s.
//   - Memory:  1024×1024 × 8B = 8MB grid + a small cached PNG.
// =============================================================================

/// Live activity counter grid. Cells are addressed by (gridX, gridZ) where
/// gridX maps galactic X (left/right) and gridZ maps galactic Z (along the
/// galactic disc). Galactic Y is ignored — this is a top-down map.
struct Heatmap {
    cells: Vec<AtomicU64>,
    cached_png: std::sync::Mutex<Option<(Instant, Arc<Vec<u8>>)>>,
    total_bumps: AtomicU64,
}

impl Heatmap {
    fn new() -> Self {
        let mut cells = Vec::with_capacity(HEATMAP_W * HEATMAP_H);
        for _ in 0..(HEATMAP_W * HEATMAP_H) {
            cells.push(AtomicU64::new(0));
        }
        Self {
            cells,
            cached_png: std::sync::Mutex::new(None),
            total_bumps: AtomicU64::new(0),
        }
    }

    /// Record one system-arrival event at galactic coordinates (x, z).
    /// Out-of-bounds or non-finite coords are silently dropped.
    fn bump(&self, x: f64, z: f64) {
        if !x.is_finite() || !z.is_finite() { return; }
        if x < HEATMAP_X_MIN || x >= HEATMAP_X_MAX { return; }
        if z < HEATMAP_Z_MIN || z >= HEATMAP_Z_MAX { return; }
        let gx = ((x - HEATMAP_X_MIN) / (HEATMAP_X_MAX - HEATMAP_X_MIN) * HEATMAP_W as f64) as usize;
        let gz = ((z - HEATMAP_Z_MIN) / (HEATMAP_Z_MAX - HEATMAP_Z_MIN) * HEATMAP_H as f64) as usize;
        let idx = gz.min(HEATMAP_H - 1) * HEATMAP_W + gx.min(HEATMAP_W - 1);
        self.cells[idx].fetch_add(1, AtomicOrdering::Relaxed);
        self.total_bumps.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Multiply every nonzero cell by `factor` (typically 0.95) so old
    /// hotspots fade out gradually. Not transactional — a concurrent bump can
    /// race with the multiply; that's fine for a visual heatmap.
    fn decay(&self, factor: f64) {
        for cell in &self.cells {
            let v = cell.load(AtomicOrdering::Relaxed);
            if v == 0 { continue; }
            let new = ((v as f64) * factor) as u64;
            cell.store(new, AtomicOrdering::Relaxed);
        }
    }

    /// Render the current grid to a PNG. Costs ~50-150ms for a 1024×1024
    /// image. Callers should use `get_or_render` instead, which caches.
    fn render_png(&self) -> Vec<u8> {
        // Snapshot all cells, then find max for log scaling.
        let snap: Vec<u64> = self.cells.iter()
            .map(|c| c.load(AtomicOrdering::Relaxed)).collect();
        let max = *snap.iter().max().unwrap_or(&1);
        let log_max = ((max + 1) as f64).ln().max(1.0);

        // Build RGBA buffer. Image origin is top-left; we want galactic Z+
        // (Sgr A*, Beagle Point) at the top, so flip Y on the way out.
        let mut rgba = vec![0u8; HEATMAP_W * HEATMAP_H * 4];
        for (i, &count) in snap.iter().enumerate() {
            if count == 0 { continue; }
            let intensity = ((count + 1) as f64).ln() / log_max;
            let (r, g, b, a) = colormap_inferno(intensity);
            let gx = i % HEATMAP_W;
            let gz = i / HEATMAP_W;
            let py = HEATMAP_H - 1 - gz;
            let pi = (py * HEATMAP_W + gx) * 4;
            rgba[pi]     = r;
            rgba[pi + 1] = g;
            rgba[pi + 2] = b;
            rgba[pi + 3] = a;
        }

        let mut out = Vec::with_capacity(64 * 1024);
        {
            let mut encoder = png::Encoder::new(&mut out, HEATMAP_W as u32, HEATMAP_H as u32);
            encoder.set_color(png::ColorType::Rgba);
            encoder.set_depth(png::BitDepth::Eight);
            encoder.set_compression(png::Compression::Fast);
            let mut writer = encoder.write_header().expect("png header write");
            writer.write_image_data(&rgba).expect("png image write");
        }
        out
    }

    /// Return a recent PNG. Re-renders only if the cache is older than
    /// HEATMAP_RENDER_CACHE_SECS, so even if /api/heatmap.png is hammered the
    /// expensive render path runs at most twice a minute.
    fn get_or_render(&self) -> Arc<Vec<u8>> {
        {
            let guard = self.cached_png.lock().unwrap();
            if let Some((t, ref bytes)) = *guard {
                if t.elapsed() < Duration::from_secs(HEATMAP_RENDER_CACHE_SECS) {
                    return bytes.clone();
                }
            }
        }
        // Render outside the lock so concurrent readers can still see the
        // stale cached version while we work.
        let bytes = Arc::new(self.render_png());
        let mut guard = self.cached_png.lock().unwrap();
        *guard = Some((Instant::now(), bytes.clone()));
        bytes
    }
}

/// Perceptual heat colormap, dark indigo → magenta → orange → bright yellow.
/// Alpha ramps up with intensity so cold cells fade into the backdrop on the
/// HTML overlay page.
fn colormap_inferno(t: f64) -> (u8, u8, u8, u8) {
    let t = t.clamp(0.0, 1.0);
    // (position, R, G, B) — interpolated linearly.
    const STOPS: &[(f64, f64, f64, f64)] = &[
        (0.00,   0.0,   0.0,   0.0),
        (0.10,  20.0,   8.0,  60.0),
        (0.25,  60.0,  10.0, 120.0),
        (0.45, 160.0,  30.0, 110.0),
        (0.65, 230.0,  80.0,  50.0),
        (0.85, 255.0, 200.0,  60.0),
        (1.00, 255.0, 255.0, 220.0),
    ];

    let (mut r, mut g, mut b) = (0.0, 0.0, 0.0);
    for w in STOPS.windows(2) {
        let (t0, r0, g0, b0) = w[0];
        let (t1, r1, g1, b1) = w[1];
        if t >= t0 && t <= t1 {
            let f = (t - t0) / (t1 - t0).max(1e-9);
            r = r0 + (r1 - r0) * f;
            g = g0 + (g1 - g0) * f;
            b = b0 + (b1 - b0) * f;
            break;
        }
    }
    let alpha = (60.0 + t * 195.0).clamp(0.0, 255.0);
    (r as u8, g as u8, b as u8, alpha as u8)
}

/// Background decay loop. Runs forever, sleeping for HEATMAP_DECAY_INTERVAL_SECS
/// then multiplying every nonzero cell by HEATMAP_DECAY_FACTOR.
fn heatmap_decay_thread(heatmap: Arc<Heatmap>) {
    loop {
        std::thread::sleep(Duration::from_secs(HEATMAP_DECAY_INTERVAL_SECS));
        heatmap.decay(HEATMAP_DECAY_FACTOR);
    }
}

/// GET /api/heatmap.png — serve the cached heatmap PNG.
async fn heatmap_png_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let heatmap = state.heatmap.clone();
    let png = tokio::task::spawn_blocking(move || heatmap.get_or_render())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("render task failed: {}", e)));
    match png {
        Ok(bytes) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE,  "image/png"),
                (header::CACHE_CONTROL, "public, max-age=30"),
            ],
            (*bytes).clone(),
        ).into_response(),
        Err((code, msg)) => (code, msg).into_response(),
    }
}

/// GET /heatmap — HTML page that overlays the heatmap on a CSS-rendered
/// galactic backdrop with markers for Sol, Sgr A*, Colonia, Beagle Point.
async fn heatmap_html_handler() -> impl IntoResponse {
    Html(HEATMAP_HTML)
}

/// HTML overlay. Coordinates are mapped from galactic (X, Z) to image (X%, Y%):
///   X% = (X + 50000) / 1000              → Sol(0,0)=50, Sgr A*(25)=50.025
///   Y% = (75000 - Z) / 1000               → Sol(0,0)=75, Sgr A*(25900)=49.1
const HEATMAP_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ananke · Commander Hotspots</title>
<style>
  html,body{margin:0;padding:0;background:#04050a;color:#dde3ee;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;overflow:hidden}
  .wrap{position:relative;width:100vmin;height:100vmin;margin:0 auto}
  .bg{position:absolute;inset:0;
    background:
      radial-gradient(ellipse 60% 35% at 50% 49%, rgba(80,60,140,.18) 0%, rgba(40,20,70,.05) 60%, transparent 100%),
      radial-gradient(ellipse 80% 60% at 50% 49%, rgba(40,30,80,.10) 0%, transparent 70%),
      #04050a;
  }
  .stars{position:absolute;inset:0;
    background-image:
      radial-gradient(1px 1px at 13% 22%,#fffa 0,transparent 100%),
      radial-gradient(1px 1px at 81% 14%,#fff8 0,transparent 100%),
      radial-gradient(1px 1px at 47% 62%,#fff7 0,transparent 100%),
      radial-gradient(1px 1px at 28% 78%,#fff6 0,transparent 100%),
      radial-gradient(1px 1px at 71% 91%,#fff5 0,transparent 100%),
      radial-gradient(1px 1px at 92% 51%,#fff4 0,transparent 100%),
      radial-gradient(1px 1px at 6% 53%,#fff3 0,transparent 100%);
    background-size:240px 240px,200px 200px,260px 260px,180px 180px,220px 220px,300px 300px,280px 280px;
    opacity:.55
  }
  .heat{position:absolute;inset:0;width:100%;height:100%;mix-blend-mode:screen;image-rendering:auto}
  .marker{position:absolute;font-size:11px;color:#9fdcc0;letter-spacing:.6px;transform:translate(-50%,-50%);pointer-events:none;text-shadow:0 0 6px #000,0 0 2px #000;white-space:nowrap}
  .marker::before{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:#9fdcc0;margin-right:5px;box-shadow:0 0 8px #9fdcc0;vertical-align:1px}
  h1{position:fixed;top:14px;left:18px;margin:0;font-size:13px;font-weight:500;color:#9fdcc0;letter-spacing:1.5px}
  .meta{position:fixed;top:34px;left:18px;font-size:10px;color:#5a6675;letter-spacing:.5px}
  .legend{position:fixed;bottom:14px;left:14px;background:rgba(8,10,16,.65);padding:10px 14px;border-radius:4px;font-size:11px;line-height:1.5;border:1px solid #1a2030}
  .bar{width:200px;height:8px;background:linear-gradient(to right,rgba(20,8,60,.3),#3c1e78,#a01e6e,#e65a32,#ffc83c,#fff8dc);margin:6px 0;border-radius:1px}
  .bar-l{display:flex;justify-content:space-between;color:#7a8696;font-size:9px;letter-spacing:.5px}
  .links{position:fixed;bottom:14px;right:14px;font-size:10px;color:#5a6675}
  .links a{color:#7a8696;text-decoration:none;margin-left:10px}
  .links a:hover{color:#9fdcc0}
</style>
</head>
<body>
<h1>ANANKE · COMMANDER HOTSPOTS</h1>
<div class="meta">live activity · log scale · ~70 min half-life</div>
<div class="wrap">
  <div class="bg"></div>
  <div class="stars"></div>
  <img class="heat" id="heat" src="/api/heatmap.png" alt="commander activity heatmap">
  <!-- galactic landmarks -->
  <div class="marker" style="left:50%;top:75%">Sol</div>
  <div class="marker" style="left:50.0%;top:49.1%">Sgr A*</div>
  <div class="marker" style="left:40.5%;top:55.2%">Colonia</div>
  <div class="marker" style="left:48.9%;top:9.7%">Beagle Point</div>
</div>
<div class="legend">
  <div style="color:#9fdcc0;letter-spacing:.5px">ACTIVITY</div>
  <div class="bar"></div>
  <div class="bar-l"><span>cold</span><span>hot</span></div>
  <div style="margin-top:6px;color:#5a6675">100,000 LY across · refresh 30s</div>
</div>
<div class="links">
  <a href="/api/heatmap.png">PNG</a>
  <a href="/api/edmc/stats">stats</a>
</div>
<script>
  function refresh(){
    var img = document.getElementById('heat');
    img.src = '/api/heatmap.png?t=' + Date.now();
  }
  setInterval(refresh, 30000);
</script>
</body>
</html>
"#;

// =============================================================================
// EDMC INGEST API — Live system data push from EDMC plugin
// =============================================================================
//
// POST /api/edmc/journal  — single system (EDMC plugin sends per-jump)
// POST /api/edmc/batch    — array of systems (bulk backfill)
// GET  /api/edmc/stats    — ingest counters
//
// Auth: X-Api-Key header must match ANANKE_EDMC_KEY env var (if set).
// If env var is unset or empty, auth is disabled (open ingest).
//
// Payload format matches SpanshSystem: { id64, name, coords, population,
// bodies: [...], stations: [...] }. Fields beyond id64/name/coords are optional.
// =============================================================================

/// Validate the EDMC API key. Returns Err with 401 if invalid.
fn check_edmc_auth(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if let Some(ref expected_key) = state.edmc_api_key {
        let provided = headers
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if provided != expected_key {
            warn!("EDMC auth failed: provided key does not match");
            return Err((StatusCode::UNAUTHORIZED, "Invalid or missing X-Api-Key".into()));
        }
    }
    Ok(())
}

/// POST /api/edmc/journal — ingest a single system from EDMC
async fn edmc_journal(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    info!("EDMC /journal: received {} bytes", body.len());

    check_edmc_auth(&state, &headers)?;

    // Log raw body (truncated) for debugging
    let body_str = String::from_utf8_lossy(&body);
    if body.len() < 2000 {
        info!("EDMC /journal body: {}", body_str);
    } else {
        info!("EDMC /journal body (truncated): {}...", &body_str[..500]);
    }

    // Manual deserialization so we get a useful error message
    let system: SpanshSystem = serde_json::from_slice(&body).map_err(|e| {
        let err_msg = format!("JSON parse error: {} — body starts with: {}", e, &body_str[..body_str.len().min(300)]);
        error!("EDMC /journal REJECTED: {}", err_msg);
        (StatusCode::BAD_REQUEST, err_msg)
    })?;

    let body_count = system.bodies.as_ref().map(|b| b.len()).unwrap_or(0);
    let station_count = system.stations.as_ref().map(|s| s.len()).unwrap_or(0);
    let sys_name = system.name.clone();
    let sys_id64 = system.id64;

    info!("EDMC /journal ACCEPTED: '{}' (id64={}, bodies={}, stations={})", sys_name, sys_id64, body_count, station_count);

    // Heatmap bump (commander hotspots) — single submission, safe to bump here.
    if let Some(c) = system.coords.as_ref() {
        state.heatmap.bump(c.x, c.z);
    }

    state.edmc_sender.send(vec![system])
        .map_err(|_| {
            error!("EDMC /journal: writer channel dead or full!");
            (StatusCode::INTERNAL_SERVER_ERROR, "Ingest queue full or writer dead".into())
        })?;

    // Update stats
    state.edmc_stats.systems_ingested.fetch_add(1, AtomicOrdering::Relaxed);
    state.edmc_stats.bodies_ingested.fetch_add(body_count as u64, AtomicOrdering::Relaxed);
    state.edmc_stats.stations_ingested.fetch_add(station_count as u64, AtomicOrdering::Relaxed);
    state.edmc_stats.last_ingest_time.store(current_time_secs(), AtomicOrdering::Relaxed);

    info!("EDMC /journal: queued '{}' for DB write.", sys_name);

    Ok(Json(serde_json::json!({
        "status": "accepted",
        "system": sys_name,
        "bodies": body_count,
        "stations": station_count
    })))
}

/// POST /api/edmc/batch — ingest multiple systems at once
async fn edmc_batch(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    info!("EDMC /batch: received {} bytes", body.len());

    check_edmc_auth(&state, &headers)?;

    let body_str = String::from_utf8_lossy(&body);

    let systems: Vec<SpanshSystem> = serde_json::from_slice(&body).map_err(|e| {
        let err_msg = format!("JSON parse error: {} — body starts with: {}", e, &body_str[..body_str.len().min(300)]);
        error!("EDMC /batch REJECTED: {}", err_msg);
        (StatusCode::BAD_REQUEST, err_msg)
    })?;

    if systems.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Empty batch".into()));
    }
    if systems.len() > 10000 {
        return Err((StatusCode::BAD_REQUEST, "Batch too large (max 10000)".into()));
    }

    let count = systems.len();
    let body_total: usize = systems.iter().map(|s| s.bodies.as_ref().map(|b| b.len()).unwrap_or(0)).sum();
    let station_total: usize = systems.iter().map(|s| s.stations.as_ref().map(|b| b.len()).unwrap_or(0)).sum();

    info!("EDMC /batch ACCEPTED: {} systems, {} bodies, {} stations", count, body_total, station_total);

    // Heatmap bump per system that carries coords. Batches up to 10k systems
    // at 1 atomic add each = ~10µs total — completely free.
    for s in systems.iter() {
        if let Some(c) = s.coords.as_ref() {
            state.heatmap.bump(c.x, c.z);
        }
    }

    for chunk in systems.chunks(5000).map(|c| c.to_vec()) {
        state.edmc_sender.send(chunk)
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Ingest queue full or writer dead".into()))?;
    }

    state.edmc_stats.systems_ingested.fetch_add(count as u64, AtomicOrdering::Relaxed);
    state.edmc_stats.bodies_ingested.fetch_add(body_total as u64, AtomicOrdering::Relaxed);
    state.edmc_stats.stations_ingested.fetch_add(station_total as u64, AtomicOrdering::Relaxed);
    state.edmc_stats.last_ingest_time.store(current_time_secs(), AtomicOrdering::Relaxed);

    Ok(Json(serde_json::json!({
        "status": "accepted",
        "systems": count,
        "bodies": body_total,
        "stations": station_total
    })))
}

/// GET /api/edmc/stats — ingest counters and DB health
async fn edmc_stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let stats = &state.edmc_stats;
    let eddn = &state.eddn_stats;
    let heatmap = &state.heatmap;
    let pool = state.db_pool.clone();

    let db_stats = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let last_sync: String = conn.query_row(
            "SELECT value FROM meta WHERE key='last_sync_time'", [], |r| r.get(0)
        ).unwrap_or_else(|_| "0".to_string());

        let import_complete: String = conn.query_row(
            "SELECT value FROM meta WHERE key='import_complete'", [], |r| r.get(0)
        ).unwrap_or_else(|_| "false".to_string());

        Ok(serde_json::json!({
            "last_spansh_sync": last_sync.parse::<u64>().unwrap_or(0),
            "import_complete": import_complete,
        }))
    }).await.unwrap().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let last_ingest = stats.last_ingest_time.load(AtomicOrdering::Relaxed);

    Ok(Json(serde_json::json!({
        "edmc_ingest": {
            "systems_ingested": stats.systems_ingested.load(AtomicOrdering::Relaxed),
            "bodies_ingested": stats.bodies_ingested.load(AtomicOrdering::Relaxed),
            "stations_ingested": stats.stations_ingested.load(AtomicOrdering::Relaxed),
            "last_ingest_time": last_ingest,
        },
        "eddn_ingest": {
            "connected":           eddn.connected.load(AtomicOrdering::Relaxed) == 1,
            "messages_received":   eddn.messages_received.load(AtomicOrdering::Relaxed),
            "messages_processed":  eddn.messages_processed.load(AtomicOrdering::Relaxed),
            "messages_dropped":    eddn.messages_dropped.load(AtomicOrdering::Relaxed),
            "systems_emitted":     eddn.systems_emitted.load(AtomicOrdering::Relaxed),
            "bodies_emitted":      eddn.bodies_emitted.load(AtomicOrdering::Relaxed),
            "stations_emitted":    eddn.stations_emitted.load(AtomicOrdering::Relaxed),
            "last_message_time":   eddn.last_message_time.load(AtomicOrdering::Relaxed),
            "reconnects":          eddn.reconnects.load(AtomicOrdering::Relaxed),
        },
        "heatmap": {
            "total_bumps": heatmap.total_bumps.load(AtomicOrdering::Relaxed),
            "grid":        format!("{}x{}", HEATMAP_W, HEATMAP_H),
            "bounds_x":    [HEATMAP_X_MIN, HEATMAP_X_MAX],
            "bounds_z":    [HEATMAP_Z_MIN, HEATMAP_Z_MAX],
            "decay_factor_per_5min": HEATMAP_DECAY_FACTOR,
        },
        "database": db_stats,
    })))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    info!("Starting EDSM-Cube-RS on Pi 5...");

    let pool = setup_db_pool();
    let conn = pool.get().unwrap();
    init_db(&conn).unwrap();
    drop(conn);

    tokio::spawn(async { sync_manager().await; });

    // EDMC live-ingest writer — same db_writer_worker as the Spansh dump import,
    // running in its own thread with a bounded channel.
    let (edmc_sender, edmc_receiver) = bounded::<Vec<SpanshSystem>>(100);
    let _edmc_writer = std::thread::spawn(move || db_writer_worker(edmc_receiver));

    // EDMC API key from environment
    let edmc_api_key = std::env::var(EDMC_KEY_ENV).ok().filter(|k| !k.is_empty());
    if edmc_api_key.is_some() {
        info!("EDMC API key authentication enabled.");
    } else {
        warn!("EDMC API key not set ({}). Ingest endpoints are OPEN — set the env var to require auth.", EDMC_KEY_ENV);
    }

    // EDDN live-feed listener — connects to the public ZMQ relay and feeds
    // translated journal events into the same writer channel as EDMC and the
    // Spansh dump. Disable with ANANKE_EDDN_DISABLE=1, override the relay
    // with ANANKE_EDDN_RELAY=tcp://host:port.
    let eddn_stats = Arc::new(EddnStats {
        messages_received:  AtomicU64::new(0),
        messages_processed: AtomicU64::new(0),
        messages_dropped:   AtomicU64::new(0),
        systems_emitted:    AtomicU64::new(0),
        bodies_emitted:     AtomicU64::new(0),
        stations_emitted:   AtomicU64::new(0),
        last_message_time:  AtomicU64::new(0),
        reconnects:         AtomicU64::new(0),
        connected:          AtomicU64::new(0),
    });

    // Commander hotspot heatmap — built before the EDDN listener so we can
    // hand it the Arc. A background thread decays the grid every 5 minutes
    // so old activity fades; render is on-demand and cached.
    let heatmap = Arc::new(Heatmap::new());
    {
        let heatmap_for_decay = heatmap.clone();
        let _decay = std::thread::Builder::new()
            .name("heatmap-decay".into())
            .spawn(move || heatmap_decay_thread(heatmap_for_decay))
            .expect("failed to spawn heatmap decay thread");
        info!("Heatmap decay thread started.");
    }

    let eddn_disabled = std::env::var(EDDN_DISABLE_ENV).ok()
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if eddn_disabled {
        warn!("EDDN listener DISABLED via {}.", EDDN_DISABLE_ENV);
    } else {
        let relay_url = std::env::var(EDDN_RELAY_ENV)
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| EDDN_RELAY_URL.to_string());
        let eddn_sender = edmc_sender.clone();
        let eddn_stats_for_thread = eddn_stats.clone();
        let eddn_heatmap = heatmap.clone();
        let _eddn_listener = std::thread::Builder::new()
            .name("eddn-listener".into())
            .spawn(move || eddn_listener_thread(relay_url, eddn_sender, eddn_stats_for_thread, eddn_heatmap))
            .expect("failed to spawn EDDN listener thread");
        info!("EDDN listener thread started.");
    }

    let app_state = Arc::new(AppState {
        db_pool: pool,
        query_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_QUERIES)),
        astar_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_ASTAR)),
        carrier_cache: Mutex::new(CarrierCache { data: None, expires_at: 0 }),
        edmc_sender,
        edmc_api_key,
        edmc_stats: Arc::new(EdmcStats {
            systems_ingested: AtomicU64::new(0),
            bodies_ingested: AtomicU64::new(0),
            stations_ingested: AtomicU64::new(0),
            last_ingest_time: AtomicU64::new(0),
        }),
        eddn_stats,
        heatmap,
    });

    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/api/system", get(get_system))
        .route("/api/system/bodies", get(get_system_bodies))
        .route("/api/bodies", get(get_system_bodies))
        .route("/api/system/stations", get(get_system_stations))
        .route("/api/stations", get(get_system_stations))
        .route("/api/nearest-station", get(nearest_station))
        .route("/api/cube-search", get(cube_search_get).post(cube_search_post))
        .route("/api/route", get(ship_route_get).post(ship_route_post))
        .route("/api/carrier-route", post(carrier_route_post))
        .route("/api/neutron-route", post(neutron_route_post))
        .route("/api/galtea-progression", get(get_carrier_progression))
        // EDMC ingest endpoints
        .route("/api/edmc/journal", post(edmc_journal))
        .route("/api/edmc/batch", post(edmc_batch))
        .route("/api/edmc/stats", get(edmc_stats))
        // Commander hotspot heatmap
        .route("/api/heatmap.png", get(heatmap_png_handler))
        .route("/heatmap", get(heatmap_html_handler))
        .layer(cors)
        .with_state(app_state);
		.route("/api/distance", get(get_distance))

    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).await.unwrap();
    info!("Server listening on port {}", PORT);
    axum::serve(listener, app).await.unwrap();
}
