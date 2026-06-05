use axum::{extract::{Query, State}, http::StatusCode, Json};
use std::{
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::Instant,
};

use crate::config::SHIP_ROUTE_BUDGET_MS;
use crate::models::{RouteNode, RouteQuery};
use crate::state::AppState;

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
            g_score: 0, f_score: start_h,
            id64: src_id, x: src_x, y: src_y, z: src_z,
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

                    let dist_from_prev = if step_idx == 0 { 0.0 }
                    else { ((n_x - prev_x).powi(2) + (n_y - prev_y).powi(2) + (n_z - prev_z).powi(2)).sqrt() };

                    route_json.push(serde_json::json!({
                        "system": n_name, "id64": node_id.to_string(),
                        "coords": {"x": n_x, "y": n_y, "z": n_z},
                        "distance_from_prev": (dist_from_prev * 100.0).round() / 100.0,
                        "jumps": step_idx
                    }));

                    prev_x = n_x; prev_y = n_y; prev_z = n_z;
                }

                return Ok(serde_json::json!({
                    "source": source_name, "destination": dest_name,
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
                Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?, row.get::<_, f64>(2)?, row.get::<_, f64>(3)?))
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
                            open_set.push(RouteNode {
                                g_score: tentative_g, f_score: tentative_g as f64 + h_score,
                                id64: n_id, x: n_x, y: n_y, z: n_z,
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

pub async fn ship_route_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<RouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_ship_route(state, params).await
}

pub async fn ship_route_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<RouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_ship_route(state, params).await
}
