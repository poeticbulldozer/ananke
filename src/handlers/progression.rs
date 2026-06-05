use axum::{extract::State, http::StatusCode, Json};
use rusqlite::params;
use std::sync::Arc;

use crate::db::current_time_secs;
use crate::state::AppState;

pub async fn get_carrier_progression(
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
                params![id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))
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
