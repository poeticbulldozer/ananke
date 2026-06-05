use axum::{extract::{Query, State}, http::StatusCode, Json};
use std::sync::Arc;

use crate::models::NearestStationQuery;
use crate::state::AppState;

pub async fn nearest_station(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NearestStationQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.query_semaphore.acquire().await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded".into()))?;

    let pool = state.db_pool.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        // 1. Resolve reference system coordinates
        let ref_name = params.ref_system.clone();
        let (ref_x, ref_y, ref_z): (f64, f64, f64) = conn.query_row(
            "SELECT i.minX, i.minY, i.minZ
             FROM systems s JOIN systems_index i ON s.id64 = i.id
             WHERE s.name = ? COLLATE NOCASE LIMIT 1",
            rusqlite::params![ref_name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).map_err(|_| format!("Reference system '{}' not found", ref_name))?;

        let limit = params.limit.unwrap_or(25).clamp(1, 100);

        // 2. Normalise filter values
        let ignore_carriers    = params.ignore_fleet_carriers.unwrap_or(true);
        let use_surface        = params.use_surface_stations;
        let allegiance_filter  = params.allegiance.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty()).map(|s| s.to_string());
        let government_filter  = params.government.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty()).map(|s| s.to_string());
        let economy_filter     = params.economy.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty()).map(|s| s.to_string());
        let station_type_filter = params.station_type.as_deref()
            .filter(|s| !s.eq_ignore_ascii_case("any") && !s.is_empty()).map(|s| s.to_string());
        let max_dist_filter    = params.max_station_distance;
        let pad = params.min_landing_pad.as_deref().map(|s| s.to_lowercase()).unwrap_or_default();
        let require_large  = pad == "large";
        let require_medium = pad == "medium" || require_large;

        // 3. Build SQL
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

        // Helper: run the query
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
            if let Some(ref v) = economy_filter      { raw_params.push(Box::new(v.clone())); raw_params.push(Box::new(v.clone())); }
            if let Some(ref v) = station_type_filter { raw_params.push(Box::new(v.clone())); }
            if let Some(v)     = max_dist_filter     { raw_params.push(Box::new(v)); }

            let param_refs: Vec<&dyn rusqlite::ToSql> = raw_params.iter().map(|b| b.as_ref()).collect();

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

        // 4. Run search
        let search_steps: &[f64] = &[100.0, 500.0, 1_500.0, 4_000.0, 8_000.0, 15_000.0, 30_000.0, 70_000.0];
        let override_radius = params.radius;
        let mut results;
        let mut radius = 100.0_f64;

        if is_prison {
            results = run_query(&mut stmt, None)?;
        } else if let Some(r) = override_radius {
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

        let reported_radius = if is_prison {
            results.last().and_then(|r| r["systemDistanceLy"].as_f64()).unwrap_or(0.0)
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
