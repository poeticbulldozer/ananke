use crossbeam_channel::Receiver;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, Result as SqliteResult};
use std::time::Duration;
use tracing::{error, info, warn};

use crate::config::DB_FILE;
use crate::models::SpanshSystem;

// --- JSON extraction helpers ---

pub fn get_i64(v: &serde_json::Value, k: &str) -> Option<i64> {
    v.get(k).and_then(|x| x.as_i64())
}

pub fn get_f64(v: &serde_json::Value, k: &str) -> Option<f64> {
    v.get(k).and_then(|x| x.as_f64().or_else(|| x.as_i64().map(|i| i as f64)))
}

pub fn get_str<'a>(v: &'a serde_json::Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(|x| x.as_str())
}

pub fn get_bool(v: &serde_json::Value, k: &str) -> i32 {
    v.get(k).and_then(|x| x.as_bool()).map(|x| if x { 1 } else { 0 }).unwrap_or(0)
}

pub fn get_bool_opt(v: &serde_json::Value, k: &str) -> Option<i32> {
    v.get(k).and_then(|x| x.as_bool().map(|b| if b { 1 } else { 0 }).or_else(|| x.as_i64().map(|i| i as i32)))
}

// --- Utility ---

pub fn current_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// --- Pool setup ---

pub fn setup_db_pool() -> Pool<SqliteConnectionManager> {
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

// --- Schema init ---

pub fn init_db(conn: &Connection) -> SqliteResult<()> {
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

// --- DB writer worker ---

pub fn db_writer_worker(receiver: Receiver<Vec<SpanshSystem>>) {
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
                                let surface_temp: Option<i64> = get_i64(b, "surfaceTemperature")
                                    .or_else(|| get_f64(b, "surfaceTemperature").map(|f| f.round() as i64));
                                let atmo_comp = b.get("atmosphereComposition").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let composition = b.get("solidComposition")
                                    .or_else(|| b.get("composition"))
                                    .filter(|v| !v.is_null()).map(|v| v.to_string());
                                let rings = b.get("rings").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let parents = b.get("parents").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let signals = b.get("signals").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let stellar_mass = get_f64(b, "solarMasses").or_else(|| get_f64(b, "stellarMass"));
                                let belts = b.get("belts").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let materials = b.get("materials").filter(|v| !v.is_null()).map(|v| v.to_string());

                                stmt_bodies.execute(params![
                                    sys.id64, get_i64(b, "bodyId"), get_str(b, "name"), get_str(b, "type"), get_str(b, "subType"),
                                    get_f64(b, "distanceToArrival"), get_bool(b, "isLandable"), get_f64(b, "gravity"), get_f64(b, "earthMasses"),
                                    get_f64(b, "radius"), surface_temp, get_f64(b, "orbitalPeriod"), get_f64(b, "semiMajorAxis"),
                                    get_f64(b, "orbitalEccentricity"), get_f64(b, "orbitalInclination"), get_f64(b, "argOfPeriapsis"), get_f64(b, "rotationalPeriod"),
                                    get_bool(b, "rotationalPeriodTidallyLocked"), get_f64(b, "axialTilt"), get_str(b, "volcanismType"), get_str(b, "atmosphereType"),
                                    get_str(b, "terraformingState"),
                                    stellar_mass, get_f64(b, "absoluteMagnitude"), get_i64(b, "age"),
                                    get_str(b, "luminosity"), get_i64(b, "subclass"), get_f64(b, "surfacePressure"),
                                    atmo_comp, composition, rings, parents,
                                    get_i64(b, "wasDiscovered").or_else(|| get_bool_opt(b, "wasDiscovered").map(|v| v as i64)),
                                    get_i64(b, "wasMapped").or_else(|| get_bool_opt(b, "wasMapped").map(|v| v as i64)),
                                    get_f64(b, "ascendingNode"), get_f64(b, "meanAnomaly"),
                                    signals,
                                    get_i64(b, "id64"),
                                    get_bool_opt(b, "mainStar").map(|v| v as i64),
                                    get_str(b, "spectralClass"),
                                    get_f64(b, "solarRadius"),
                                    materials,
                                    get_str(b, "reserveLevel"),
                                    belts,
                                    get_str(b, "updateTime")
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

                                let landing_pads = st.get("landingPads").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let economies = st.get("economies").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let market = st.get("market").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let shipyard = st.get("shipyard").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let outfitting = st.get("outfitting").filter(|v| !v.is_null()).map(|v| v.to_string());
                                let ctrl_faction = get_str(st, "controllingFaction");
                                let ctrl_faction_state = get_str(st, "controllingFactionState");

                                stmt_stations.execute(params![
                                    get_i64(st, "id"), get_i64(st, "marketId"), sys.id64, get_str(st, "name"), get_str(st, "type"), get_f64(st, "distanceToArrival"),
                                    get_str(st, "allegiance"), get_str(st, "government"), get_str(st, "primaryEconomy"), get_str(st, "secondaryEconomy"),
                                    has_market, has_shipyard, has_outfitting, other_svcs_json, get_str(st, "updateTime"),
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