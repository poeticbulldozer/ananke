use crossbeam_channel::Sender;
use flate2::read::ZlibDecoder;
use std::{
    io::Read,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

use crate::config::*;
use crate::db::current_time_secs;
use crate::heatmap::Heatmap;
use crate::models::{SpanshCoords, SpanshSystem};
use crate::state::EddnStats;

// --- Decompression ---

fn eddn_decompress(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 4);
    ZlibDecoder::new(input).read_to_end(&mut out)?;
    Ok(out)
}

// --- String helpers ---

fn strip_dollar_token(s: &str) -> String {
    let s = s.trim_start_matches('$').trim_end_matches(';');
    if let Some(idx) = s.find('_') {
        s[idx + 1..].to_string()
    } else {
        s.to_string()
    }
}

fn loc_str(msg: &serde_json::Value, raw_key: &str, loc_key: &str) -> Option<String> {
    if let Some(v) = msg.get(loc_key).and_then(|v| v.as_str()) {
        return Some(v.to_string());
    }
    msg.get(raw_key)
        .and_then(|v| v.as_str())
        .map(strip_dollar_token)
}

fn star_type_to_subtype(t: &str) -> String {
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

// --- Translators ---

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

    let allegiance = msg.get("SystemAllegiance").and_then(|v| v.as_str())
        .filter(|s| !s.is_empty()).map(|s| s.to_string());
    let government       = loc_str(msg, "SystemGovernment",     "SystemGovernment_Localised");
    let primary_economy  = loc_str(msg, "SystemEconomy",        "SystemEconomy_Localised");
    let secondary_econ   = loc_str(msg, "SystemSecondEconomy",  "SystemSecondEconomy_Localised");
    let security         = loc_str(msg, "SystemSecurity",       "SystemSecurity_Localised");
    let population       = msg.get("Population").and_then(|v| v.as_i64());
    let controlling_faction = msg.get("SystemFaction").cloned().filter(|v| !v.is_null());
    let factions = msg.get("Factions").cloned().filter(|v| !v.is_null());
    let powers = msg.get("Powers").cloned().filter(|v| !v.is_null());
    let power_state = msg.get("PowerplayState").and_then(|v| v.as_str()).map(|s| s.to_string());
    let controlling_power = msg.get("ControllingPower").and_then(|v| v.as_str()).map(|s| s.to_string());
    let p_ctrl   = msg.get("PowerplayStateControlProgress").and_then(|v| v.as_f64());
    let p_reinf  = msg.get("PowerplayStateReinforcement").and_then(|v| v.as_f64());
    let p_under  = msg.get("PowerplayStateUndermining").and_then(|v| v.as_f64());
    let p_conflict = msg.get("PowerplayConflictProgress").cloned().filter(|v| !v.is_null());
    let thargoid = msg.get("ThargoidWar").cloned().filter(|v| !v.is_null());
    let date = msg.get("timestamp").and_then(|v| v.as_str()).map(|s| s.to_string());

    SpanshSystem {
        id64, name, population, coords,
        bodies: None, stations: None,
        allegiance, government, primary_economy,
        secondary_economy: secondary_econ, security,
        body_count: None, date,
        controlling_faction, factions, power_state, powers, controlling_power,
        power_state_control_progress: p_ctrl,
        power_state_reinforcement: p_reinf,
        power_state_undermining: p_under,
        power_conflict_progress: p_conflict,
        thargoid_war: thargoid,
    }
}

fn eddn_carrier_system(id64: i64, name: String) -> SpanshSystem {
    SpanshSystem {
        id64, name,
        population: None, coords: None, bodies: None, stations: None,
        allegiance: None, government: None, primary_economy: None,
        secondary_economy: None, security: None, body_count: None,
        date: None, controlling_faction: None, factions: None,
        power_state: None, powers: None, controlling_power: None,
        power_state_control_progress: None, power_state_reinforcement: None,
        power_state_undermining: None, power_conflict_progress: None,
        thargoid_war: None,
    }
}

fn eddn_scan_to_body(scan: &serde_json::Value) -> Option<serde_json::Value> {
    let body_id = scan.get("BodyID").and_then(|v| v.as_i64())?;
    let body_name = scan.get("BodyName").and_then(|v| v.as_str())?.to_string();

    let mut body = serde_json::Map::new();
    body.insert("bodyId".into(), serde_json::json!(body_id));
    body.insert("name".into(), serde_json::json!(body_name));

    // Use a block so the `copy` closure (which mutably borrows `body`) is dropped
    // before the unit-converted inserts below, avoiding E0499.
    {
        let mut copy = |src: &str, dst: &str| {
            if let Some(v) = scan.get(src).filter(|v| !v.is_null()) {
                body.insert(dst.into(), v.clone());
            }
        };
        copy("DistanceFromArrivalLS", "distanceToArrival");
        copy("SurfaceTemperature",    "surfaceTemperature");
        copy("Eccentricity",          "orbitalEccentricity");
        copy("OrbitalInclination",    "orbitalInclination");
        copy("Periapsis",             "argOfPeriapsis");
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
    } // `copy` dropped here, mutable borrow on `body` released

    // Unit conversions: EDDN uses SI, Spansh uses game units
    // SurfacePressure: Pascals -> atmospheres
    if let Some(pa) = scan.get("SurfacePressure").and_then(|v| v.as_f64()) {
        body.insert("surfacePressure".into(), serde_json::json!(pa / 101325.0));
    }
    // OrbitalPeriod: seconds -> days
    if let Some(s) = scan.get("OrbitalPeriod").and_then(|v| v.as_f64()) {
        body.insert("orbitalPeriod".into(), serde_json::json!(s / 86400.0));
    }
    // SemiMajorAxis: metres -> AU
    if let Some(m) = scan.get("SemiMajorAxis").and_then(|v| v.as_f64()) {
        body.insert("semiMajorAxis".into(), serde_json::json!(m / 149_597_870_700.0));
    }
    // RotationPeriod: seconds -> days (negative = retrograde, preserved)
    if let Some(s) = scan.get("RotationPeriod").and_then(|v| v.as_f64()) {
        body.insert("rotationalPeriod".into(), serde_json::json!(s / 86400.0));
    }

    if let Some(v) = scan.get("AtmosphereType").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
        body.insert("atmosphereType".into(), serde_json::json!(v));
    } else if let Some(v) = scan.get("Atmosphere").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
        body.insert("atmosphereType".into(), serde_json::json!(v));
    }

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
        // db.rs uses "solarRadius" for stars; mirror the radius value under that key too
        if let Some(r) = scan.get("Radius").and_then(|v| v.as_f64()) {
            body.insert("solarRadius".into(), serde_json::json!(r / 695700000.0));
        }
    }

    if let Some(g) = scan.get("SurfaceGravity").and_then(|v| v.as_f64()) {
        body.insert("gravity".into(), serde_json::json!(g / 9.80665));
    }
    if let Some(r) = scan.get("Radius").and_then(|v| v.as_f64()) {
        body.insert("radius".into(), serde_json::json!(r / 1000.0));
    }
    if let Some(v) = scan.get("timestamp") {
        body.insert("updateTime".into(), v.clone());
    }

    Some(serde_json::Value::Object(body))
}

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

fn eddn_signals_to_body(s: &serde_json::Value) -> Option<serde_json::Value> {
    let body_id = s.get("BodyID").and_then(|v| v.as_i64())?;
    let body_name = s.get("BodyName").and_then(|v| v.as_str()).map(|s| s.to_string());
    let signals = s.get("Signals").cloned().filter(|v| !v.is_null());
    signals.as_ref()?;

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

// --- Dispatch ---

fn extract_starpos(msg: &serde_json::Value) -> Option<SpanshCoords> {
    msg.get("StarPos")
        .and_then(|v| v.as_array())
        .filter(|arr| arr.len() == 3)
        .and_then(|arr| {
            Some(SpanshCoords {
                x: arr[0].as_f64()?,
                y: arr[1].as_f64()?,
                z: arr[2].as_f64()?,
            })
        })
}

fn eddn_handle_journal(msg: &serde_json::Value) -> Option<SpanshSystem> {
    let event = msg.get("event")?.as_str()?;
    let id64 = msg.get("SystemAddress").and_then(|v| v.as_i64())?;
    let name = msg.get("StarSystem").and_then(|v| v.as_str())?.to_string();
    if name.is_empty() { return None; }

    match event {
        "FSDJump" | "Location" | "CarrierJump" => Some(eddn_jump_to_system(id64, name, msg)),
        "Scan" => {
            let body = eddn_scan_to_body(msg)?;
            let mut sys = eddn_carrier_system(id64, name);
            sys.coords = extract_starpos(msg);
            sys.bodies = Some(vec![body]);
            Some(sys)
        }
        "Docked" => {
            let station = eddn_docked_to_station(msg)?;
            let mut sys = eddn_carrier_system(id64, name);
            sys.coords = extract_starpos(msg);
            sys.stations = Some(vec![station]);
            Some(sys)
        }
        "SAASignalsFound" => {
            let body = eddn_signals_to_body(msg)?;
            let mut sys = eddn_carrier_system(id64, name);
            sys.coords = extract_starpos(msg);
            sys.bodies = Some(vec![body]);
            Some(sys)
        }
        _ => None,
    }
}

fn eddn_handle_envelope(envelope: &serde_json::Value) -> Option<SpanshSystem> {
    let schema = envelope.get("$schemaRef").and_then(|v| v.as_str())?;
    let message = envelope.get("message")?;
    if schema.starts_with("https://eddn.edcd.io/schemas/journal/") {
        return eddn_handle_journal(message);
    }
    None
}

// --- Listener thread ---

pub fn eddn_listener_thread(
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

        let _ = sock.set_rcvtimeo(EDDN_RECV_TIMEOUT_MS);
        let _ = sock.set_linger(0);
        let _ = sock.set_reconnect_ivl(2_000);
        let _ = sock.set_reconnect_ivl_max(60_000);
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
        stats.connected.store(1, Ordering::Relaxed);
        reconnect_delay_ms = EDDN_RECONNECT_BASE_MS;

        let mut buffer: Vec<SpanshSystem> = Vec::with_capacity(EDDN_FLUSH_BATCH_SIZE);
        let mut last_flush = Instant::now();

        loop {
            let msg = match sock.recv_bytes(0) {
                Ok(m) => Some(m),
                Err(zmq::Error::EAGAIN) => None,
                Err(e) => {
                    error!("EDDN: recv error, will reconnect: {}", e);
                    stats.connected.store(0, Ordering::Relaxed);
                    break;
                }
            };

            if let Some(raw) = msg {
                stats.messages_received.fetch_add(1, Ordering::Relaxed);

                let json_bytes = match eddn_decompress(&raw) {
                    Ok(b) => b,
                    Err(e) => {
                        stats.messages_dropped.fetch_add(1, Ordering::Relaxed);
                        warn!("EDDN: zlib decompress failed ({} bytes): {}", raw.len(), e);
                        continue;
                    }
                };

                let envelope: serde_json::Value = match serde_json::from_slice(&json_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        stats.messages_dropped.fetch_add(1, Ordering::Relaxed);
                        warn!("EDDN: JSON parse failed: {}", e);
                        continue;
                    }
                };

                match eddn_handle_envelope(&envelope) {
                    Some(sys) => {
                        if let Some(c) = sys.coords.as_ref() {
                            heatmap.bump(c.x, c.z);
                        }
                        let body_count = sys.bodies.as_ref().map(|b| b.len() as u64).unwrap_or(0);
                        let station_count = sys.stations.as_ref().map(|s| s.len() as u64).unwrap_or(0);
                        stats.messages_processed.fetch_add(1, Ordering::Relaxed);
                        stats.systems_emitted.fetch_add(1, Ordering::Relaxed);
                        stats.bodies_emitted.fetch_add(body_count, Ordering::Relaxed);
                        stats.stations_emitted.fetch_add(station_count, Ordering::Relaxed);
                        stats.last_message_time.store(current_time_secs(), Ordering::Relaxed);
                        buffer.push(sys);
                    }
                    None => {}
                }
            }

            let should_flush = buffer.len() >= EDDN_FLUSH_BATCH_SIZE
                || (!buffer.is_empty()
                    && last_flush.elapsed() >= Duration::from_millis(EDDN_FLUSH_INTERVAL_MS));

            if should_flush {
                let batch = std::mem::take(&mut buffer);
                let n = batch.len();
                match sender.try_send(batch) {
                    Ok(()) => { last_flush = Instant::now(); }
                    Err(crossbeam_channel::TrySendError::Full(returned)) => {
                        warn!("EDDN: writer queue full, blocking on send ({} systems)", n);
                        if sender.send(returned).is_err() {
                            error!("EDDN: writer channel dead, exiting listener");
                            stats.connected.store(0, Ordering::Relaxed);
                            return;
                        }
                        last_flush = Instant::now();
                    }
                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                        error!("EDDN: writer channel dead, exiting listener");
                        stats.connected.store(0, Ordering::Relaxed);
                        return;
                    }
                }
            }
        }

        if !buffer.is_empty() {
            let _ = sender.send(std::mem::take(&mut buffer));
        }
        stats.reconnects.fetch_add(1, Ordering::Relaxed);
        info!("EDDN: reconnecting in {}ms...", reconnect_delay_ms);
        std::thread::sleep(Duration::from_millis(reconnect_delay_ms));
        reconnect_delay_ms = (reconnect_delay_ms.saturating_mul(2)).min(EDDN_RECONNECT_MAX_MS);
        continue 'outer;
    }
}
