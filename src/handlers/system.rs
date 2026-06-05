use axum::{extract::{Query, State}, http::StatusCode, Json};
use rusqlite::params;
use std::sync::Arc;

use crate::models::SystemQuery;
use crate::state::AppState;

pub async fn get_system(
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

        let select_sql = "
            SELECT s.id64, s.name, s.population, i.minX, i.minY, i.minZ,
                   s.controllingFaction, s.factions, s.powers, s.powerConflictProgress, s.thargoidWar,
                   s.allegiance, s.government, s.primaryEconomy, s.secondaryEconomy, s.security,
                   s.bodyCount, s.date, s.powerState, s.controllingPower,
                   s.powerStateControlProgress, s.powerStateReinforcement, s.powerStateUndermining
            FROM systems s JOIN systems_index i ON s.id64 = i.id";

        if let Some(id) = by_id64 {
            let mut stmt = conn.prepare(&format!("{} WHERE s.id64 = ? LIMIT 1", select_sql)).unwrap();
            stmt.query_row(params![id], make_row).map_err(|_| "System not found".to_string())
        } else {
            let name = by_name.unwrap();
            let mut stmt = conn.prepare(&format!("{} WHERE s.name = ? COLLATE NOCASE LIMIT 1", select_sql)).unwrap();
            stmt.query_row(params![name], make_row).map_err(|_| "System not found".to_string())
        }
    }).await.unwrap();

    match result {
        Ok(json) => Ok(Json(json)),
        Err(_) => Err((StatusCode::NOT_FOUND, "System not found".into())),
    }
}

pub async fn get_system_bodies(
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
            conn.query_row("SELECT id64, name FROM systems WHERE id64 = ? LIMIT 1", params![id], |row| Ok((row.get(0)?, row.get(1)?)))
        } else {
            conn.query_row("SELECT id64, name FROM systems WHERE name = ? COLLATE NOCASE LIMIT 1", params![by_name.unwrap()], |row| Ok((row.get(0)?, row.get(1)?)))
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

            let atmo_comp: Option<serde_json::Value> = b.get::<_, Option<String>>("atmosphereComposition").unwrap_or(None).and_then(|s| serde_json::from_str(&s).ok());
            let composition: Option<serde_json::Value> = b.get::<_, Option<String>>("composition").unwrap_or(None).and_then(|s| serde_json::from_str(&s).ok());
            let rings: Option<serde_json::Value> = b.get::<_, Option<String>>("rings").unwrap_or(None).and_then(|s| serde_json::from_str(&s).ok());
            let parents: Option<serde_json::Value> = b.get::<_, Option<String>>("parents").unwrap_or(None).and_then(|s| serde_json::from_str(&s).ok());
            let signals: Option<serde_json::Value> = b.get::<_, Option<String>>("signals").unwrap_or(None).and_then(|s| serde_json::from_str(&s).ok());
            let materials: Option<serde_json::Value> = b.get::<_, Option<String>>("materials").unwrap_or(None).and_then(|s| serde_json::from_str(&s).ok());
            let belts: Option<serde_json::Value> = b.get::<_, Option<String>>("belts").unwrap_or(None).and_then(|s| serde_json::from_str(&s).ok());

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

pub async fn get_system_stations(
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
            params![name], |row| Ok((row.get(0)?, row.get(1)?))
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
            let landing_pads: Option<serde_json::Value> = s.get::<_, Option<String>>("landingPads").unwrap_or(None).and_then(|v| serde_json::from_str(&v).ok());
            let economies: Option<serde_json::Value> = s.get::<_, Option<String>>("economies").unwrap_or(None).and_then(|v| serde_json::from_str(&v).ok());
            let market: Option<serde_json::Value> = s.get::<_, Option<String>>("market").unwrap_or(None).and_then(|v| serde_json::from_str(&v).ok());
            let shipyard: Option<serde_json::Value> = s.get::<_, Option<String>>("shipyard").unwrap_or(None).and_then(|v| serde_json::from_str(&v).ok());
            let outfitting: Option<serde_json::Value> = s.get::<_, Option<String>>("outfitting").unwrap_or(None).and_then(|v| serde_json::from_str(&v).ok());

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
