use axum::{extract::State, http::{StatusCode, HeaderMap}, Json};
use std::sync::{Arc, atomic::Ordering};
use tracing::{error, info, warn};

use crate::config::*;
use crate::db::current_time_secs;
use crate::models::SpanshSystem;
use crate::state::AppState;

fn check_edmc_auth(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if let Some(ref expected_key) = state.edmc_api_key {
        let provided = headers.get("x-api-key").and_then(|v| v.to_str().ok()).unwrap_or("");
        if provided != expected_key {
            warn!("EDMC auth failed: provided key does not match");
            return Err((StatusCode::UNAUTHORIZED, "Invalid or missing X-Api-Key".into()));
        }
    }
    Ok(())
}

pub async fn edmc_journal(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    info!("EDMC /journal: received {} bytes", body.len());
    check_edmc_auth(&state, &headers)?;

    let body_str = String::from_utf8_lossy(&body);
    if body.len() < 2000 {
        info!("EDMC /journal body: {}", body_str);
    } else {
        info!("EDMC /journal body (truncated): {}...", &body_str[..500]);
    }

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

    if let Some(c) = system.coords.as_ref() {
        state.heatmap.bump(c.x, c.z);
    }

    state.edmc_sender.send(vec![system])
        .map_err(|_| {
            error!("EDMC /journal: writer channel dead or full!");
            (StatusCode::INTERNAL_SERVER_ERROR, "Ingest queue full or writer dead".into())
        })?;

    state.edmc_stats.systems_ingested.fetch_add(1, Ordering::Relaxed);
    state.edmc_stats.bodies_ingested.fetch_add(body_count as u64, Ordering::Relaxed);
    state.edmc_stats.stations_ingested.fetch_add(station_count as u64, Ordering::Relaxed);
    state.edmc_stats.last_ingest_time.store(current_time_secs(), Ordering::Relaxed);

    info!("EDMC /journal: queued '{}' for DB write.", sys_name);

    Ok(Json(serde_json::json!({
        "status": "accepted",
        "system": sys_name,
        "bodies": body_count,
        "stations": station_count
    })))
}

pub async fn edmc_batch(
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

    if systems.is_empty() { return Err((StatusCode::BAD_REQUEST, "Empty batch".into())); }
    if systems.len() > 10000 { return Err((StatusCode::BAD_REQUEST, "Batch too large (max 10000)".into())); }

    let count = systems.len();
    let body_total: usize = systems.iter().map(|s| s.bodies.as_ref().map(|b| b.len()).unwrap_or(0)).sum();
    let station_total: usize = systems.iter().map(|s| s.stations.as_ref().map(|b| b.len()).unwrap_or(0)).sum();

    info!("EDMC /batch ACCEPTED: {} systems, {} bodies, {} stations", count, body_total, station_total);

    for s in systems.iter() {
        if let Some(c) = s.coords.as_ref() {
            state.heatmap.bump(c.x, c.z);
        }
    }

    for chunk in systems.chunks(5000).map(|c| c.to_vec()) {
        state.edmc_sender.send(chunk)
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Ingest queue full or writer dead".into()))?;
    }

    state.edmc_stats.systems_ingested.fetch_add(count as u64, Ordering::Relaxed);
    state.edmc_stats.bodies_ingested.fetch_add(body_total as u64, Ordering::Relaxed);
    state.edmc_stats.stations_ingested.fetch_add(station_total as u64, Ordering::Relaxed);
    state.edmc_stats.last_ingest_time.store(current_time_secs(), Ordering::Relaxed);

    Ok(Json(serde_json::json!({
        "status": "accepted",
        "systems": count,
        "bodies": body_total,
        "stations": station_total
    })))
}

pub async fn edmc_stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let stats = &state.edmc_stats;
    let eddn = &state.eddn_stats;
    let heatmap = &state.heatmap;
    let pool = state.db_pool.clone();

    let db_stats = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let last_sync: String = conn.query_row("SELECT value FROM meta WHERE key='last_sync_time'", [], |r| r.get(0)).unwrap_or_else(|_| "0".to_string());
        let import_complete: String = conn.query_row("SELECT value FROM meta WHERE key='import_complete'", [], |r| r.get(0)).unwrap_or_else(|_| "false".to_string());
        Ok(serde_json::json!({
            "last_spansh_sync": last_sync.parse::<u64>().unwrap_or(0),
            "import_complete": import_complete,
        }))
    }).await.unwrap().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let last_ingest = stats.last_ingest_time.load(Ordering::Relaxed);

    Ok(Json(serde_json::json!({
        "edmc_ingest": {
            "systems_ingested": stats.systems_ingested.load(Ordering::Relaxed),
            "bodies_ingested": stats.bodies_ingested.load(Ordering::Relaxed),
            "stations_ingested": stats.stations_ingested.load(Ordering::Relaxed),
            "last_ingest_time": last_ingest,
        },
        "eddn_ingest": {
            "connected":           eddn.connected.load(Ordering::Relaxed) == 1,
            "messages_received":   eddn.messages_received.load(Ordering::Relaxed),
            "messages_processed":  eddn.messages_processed.load(Ordering::Relaxed),
            "messages_dropped":    eddn.messages_dropped.load(Ordering::Relaxed),
            "systems_emitted":     eddn.systems_emitted.load(Ordering::Relaxed),
            "bodies_emitted":      eddn.bodies_emitted.load(Ordering::Relaxed),
            "stations_emitted":    eddn.stations_emitted.load(Ordering::Relaxed),
            "last_message_time":   eddn.last_message_time.load(Ordering::Relaxed),
            "reconnects":          eddn.reconnects.load(Ordering::Relaxed),
        },
        "heatmap": {
            "total_bumps": heatmap.total_bumps.load(Ordering::Relaxed),
            "grid":        format!("{}x{}", HEATMAP_W, HEATMAP_H),
            "bounds_x":    [HEATMAP_X_MIN, HEATMAP_X_MAX],
            "bounds_z":    [HEATMAP_Z_MIN, HEATMAP_Z_MAX],
            "decay_factor_per_5min": HEATMAP_DECAY_FACTOR,
        },
        "database": db_stats,
    })))
}
