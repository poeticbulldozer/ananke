use axum::{extract::State, http::StatusCode, Json};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use tracing::info;

use crate::config::{NEUTRON_REFINE_BUDGET_MS as REFINE_BUDGET_MS, NEUTRON_SEG_LY as SEG_LY};
use crate::models::NeutronRouteQuery;
use crate::state::AppState;

async fn do_neutron_route(
    state: Arc<AppState>,
    params: NeutronRouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.astar_semaphore.acquire().await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded — A* queue full".into()))?;
    let pool = state.db_pool.clone();
    let vk   = state.vulkan_astar.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let t_start = Instant::now();

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

        // ── Waypoint generation ───────────────────────────────────────────────
        let num_segs = (total_distance / SEG_LY).ceil() as usize;
        let dv = (x2-x1, y2-y1, z2-z1);

        let nearest_neutron = |tx: f64, ty: f64, tz: f64| -> Option<(i64, String, f64, f64, f64)> {
            for radius in [300.0f64, 800.0, 1500.0, 3000.0] {
                let result: rusqlite::Result<(i64, String, f64, f64, f64)> = conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                     FROM neutron_systems ns
                     JOIN systems_index i ON ns.systemId64 = i.id
                     JOIN systems s ON ns.systemId64 = s.id64
                     WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
                     ORDER BY (i.minX-?)*(i.minX-?) + (i.minY-?)*(i.minY-?) + (i.minZ-?)*(i.minZ-?)
                     LIMIT 1",
                    rusqlite::params![tx-radius, tx+radius, ty-radius, ty+radius, tz-radius, tz+radius, tx, tx, ty, ty, tz, tz],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                );
                if let Ok(row) = result { return Some(row); }
            }
            None
        };

        let mut waypoints: Vec<(i64, String, f64, f64, f64)> = Vec::new();
        if num_segs > 1 {
            for i in 1..num_segs {
                let t = (i as f64 * SEG_LY) / total_distance;
                let (wx, wy, wz) = (x1 + t*dv.0, y1 + t*dv.1, z1 + t*dv.2);
                if let Some(wp) = nearest_neutron(wx, wy, wz) {
                    if waypoints.last().map(|w: &(i64,_,_,_,_)| w.0) != Some(wp.0) {
                        waypoints.push(wp);
                    }
                }
            }
        }
        waypoints.push((dst_id, dst_name.clone(), x2, y2, z2));

        info!("Segmented into {} waypoints for {:.0} LY route", waypoints.len(), total_distance);

        // ── Segment neutron loader ────────────────────────────────────────────
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
                WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
            ").map_err(|e| e.to_string())?;

            let rows: Vec<(i64, String, f64, f64, f64)> = stmt.query_map(
                rusqlite::params![ax.min(bx)-buf, ax.max(bx)+buf, ay.min(by)-buf, ay.max(by)+buf, az.min(bz)-buf, az.max(bz)+buf],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            ).map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .filter(|&(_, _, x, y, z)| in_seg_corridor(x, y, z))
            .collect();
            Ok(rows)
        };

        let mut normal_stmt = conn.prepare("
            SELECT s.id64, s.name, i.minX, i.minY, i.minZ
            FROM systems_index i JOIN systems s ON i.id = s.id64
            WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
        ").map_err(|e| e.to_string())?;

        let mut normal_data: HashMap<i64, (String, f64, f64, f64)> = HashMap::new();
        let mut all_node_pos: HashMap<i64, (f64, f64, f64)> = HashMap::new();
        let mut all_node_name: HashMap<i64, String> = HashMap::new();
        let mut all_node_is_neutron: HashMap<i64, bool> = HashMap::new();
        all_node_pos.insert(src_id, (x1, y1, z1));
        all_node_name.insert(src_id, src_name.clone());
        all_node_pos.insert(dst_id, (x2, y2, z2));
        all_node_name.insert(dst_id, dst_name.clone());

        // ── Greedy through waypoints ──────────────────────────────────────────
        let mut full_path: Vec<i64> = vec![src_id];
        let mut current_pos = (x1, y1, z1);
        let mut current_id = src_id;

        for (wp_id, wp_name, wx, wy, wz) in &waypoints {
            let (wp_id, wx, wy, wz) = (*wp_id, *wx, *wy, *wz);
            let seg_neutrons = load_segment_neutrons(current_pos.0, current_pos.1, current_pos.2, wx, wy, wz)?;

            let seg_id_to_idx: HashMap<i64, usize> = seg_neutrons.iter().enumerate().map(|(i, (id, ..))| (*id, i)).collect();

            for (nid, nname, nx, ny, nz) in &seg_neutrons {
                all_node_pos.insert(*nid, (*nx, *ny, *nz));
                all_node_name.insert(*nid, nname.clone());
                all_node_is_neutron.insert(*nid, true);
            }
            all_node_pos.insert(wp_id, (wx, wy, wz));
            all_node_name.insert(wp_id, wp_name.clone());

            let cell_size = (boosted_range * 0.9).max(50.0);
            let mut seg_grid: HashMap<(i32,i32,i32), Vec<usize>> = HashMap::new();
            for (i, &(_, _, nx, ny, nz)) in seg_neutrons.iter().enumerate() {
                let cell = ((nx/cell_size) as i32, (ny/cell_size) as i32, (nz/cell_size) as i32);
                seg_grid.entry(cell).or_default().push(i);
            }

            let neutrons_near_seg = |cx: f64, cy: f64, cz: f64, range: f64, excl: i64| -> Vec<usize> {
                let (bx, by, bz) = ((cx/cell_size) as i32, (cy/cell_size) as i32, (cz/cell_size) as i32);
                let rsq = range * range;
                let mut out = Vec::new();
                for dx in -2i32..=2 { for dy in -2i32..=2 { for dz in -2i32..=2 {
                    if let Some(v) = seg_grid.get(&(bx+dx, by+dy, bz+dz)) {
                        for &i in v {
                            if seg_neutrons[i].0 == excl { continue; }
                            let d2 = (seg_neutrons[i].2-cx).powi(2)+(seg_neutrons[i].3-cy).powi(2)+(seg_neutrons[i].4-cz).powi(2);
                            if d2 <= rsq && d2 > 0.0 { out.push(i); }
                        }
                    }
                }}}
                out
            };

            let mut visited: HashSet<i64> = HashSet::new();
            visited.insert(current_id);

            'seg_greedy: for _ in 0..1000usize {
                let is_n = seg_id_to_idx.contains_key(&current_id)
                    || all_node_is_neutron.get(&current_id).copied().unwrap_or(false);
                let jump_range = if is_n { boosted_range } else { params.range };

                let d_wp_sq = (current_pos.0-wx).powi(2)+(current_pos.1-wy).powi(2)+(current_pos.2-wz).powi(2);
                if d_wp_sq <= jump_range * jump_range {
                    full_path.push(wp_id);
                    current_pos = (wx, wy, wz);
                    current_id = wp_id;
                    break 'seg_greedy;
                }

                let candidates = neutrons_near_seg(current_pos.0, current_pos.1, current_pos.2, jump_range, current_id);
                let best_n = candidates.iter()
                    .filter(|&&i| !visited.contains(&seg_neutrons[i].0))
                    .min_by(|&&a, &&b| {
                        let da = (seg_neutrons[a].2-wx).powi(2)+(seg_neutrons[a].3-wy).powi(2)+(seg_neutrons[a].4-wz).powi(2);
                        let db = (seg_neutrons[b].2-wx).powi(2)+(seg_neutrons[b].3-wy).powi(2)+(seg_neutrons[b].4-wz).powi(2);
                        da.partial_cmp(&db).unwrap_or(Ordering::Equal)
                    });

                if let Some(&idx) = best_n {
                    let n = &seg_neutrons[idx];
                    let d_new_sq = (n.2-wx).powi(2)+(n.3-wy).powi(2)+(n.4-wz).powi(2);
                    if d_new_sq < d_wp_sq {
                        visited.insert(n.0);
                        full_path.push(n.0);
                        current_pos = (n.2, n.3, n.4);
                        current_id = n.0;
                    } else {
                        let made_move = attempt_normal_move(
                            &mut normal_stmt, &mut normal_data, &mut all_node_pos, &mut all_node_name, &mut all_node_is_neutron,
                            &mut visited, &mut full_path, &mut current_pos, &mut current_id,
                            wx, wy, wz, jump_range, d_wp_sq
                        )?;
                        if !made_move { break 'seg_greedy; }
                    }
                } else {
                    let made_move = attempt_normal_move(
                        &mut normal_stmt, &mut normal_data, &mut all_node_pos, &mut all_node_name, &mut all_node_is_neutron,
                        &mut visited, &mut full_path, &mut current_pos, &mut current_id,
                        wx, wy, wz, jump_range, d_wp_sq
                    )?;
                    if !made_move { break 'seg_greedy; }
                }
            }
        }

        fn attempt_normal_move(
            stmt: &mut rusqlite::Statement,
            normal_data: &mut HashMap<i64, (String, f64, f64, f64)>,
            all_node_pos: &mut HashMap<i64, (f64, f64, f64)>,
            all_node_name: &mut HashMap<i64, String>,
            all_node_is_neutron: &mut HashMap<i64, bool>,
            visited: &mut HashSet<i64>,
            full_path: &mut Vec<i64>,
            current_pos: &mut (f64, f64, f64),
            current_id: &mut i64,
            wx: f64, wy: f64, wz: f64,
            jump_range: f64,
            d_wp_sq: f64
        ) -> Result<bool, String> {
            let search_radii = [jump_range, jump_range * 1.05];
            for &r in &search_radii {
                let db_rows: Vec<(i64, String, f64, f64, f64)> = stmt.query_map(
                    rusqlite::params![
                        current_pos.0-r, current_pos.0+r,
                        current_pos.1-r, current_pos.1+r,
                        current_pos.2-r, current_pos.2+r,
                    ],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, f64>(2)?, row.get::<_, f64>(3)?, row.get::<_, f64>(4)?))
                ).map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .filter(|&(nid, _, nx, ny, nz)| {
                    if nid == *current_id || visited.contains(&nid) { return false; }
                    let d2 = (nx-current_pos.0).powi(2)+(ny-current_pos.1).powi(2)+(nz-current_pos.2).powi(2);
                    d2 <= r*r && d2 > 0.0
                })
                .collect();

                let best = db_rows.iter()
                    .filter(|&(_, _, nx, ny, nz)| {
                        let d_new_sq = (nx-wx).powi(2)+(ny-wy).powi(2)+(nz-wz).powi(2);
                        d_new_sq < d_wp_sq
                    })
                    .min_by(|a, b| {
                        let da = (a.2-wx).powi(2)+(a.3-wy).powi(2)+(a.4-wz).powi(2);
                        let db = (b.2-wx).powi(2)+(b.3-wy).powi(2)+(b.4-wz).powi(2);
                        da.partial_cmp(&db).unwrap_or(Ordering::Equal)
                    });

                if let Some((nid, nname, nx, ny, nz)) = best {
                    let (nid, nname) = (*nid, nname.clone());
                    let (nx, ny, nz) = (*nx, *ny, *nz);
                    normal_data.entry(nid).or_insert((nname.clone(), nx, ny, nz));
                    all_node_pos.insert(nid, (nx, ny, nz));
                    all_node_name.insert(nid, nname);
                    all_node_is_neutron.insert(nid, false);
                    visited.insert(nid);
                    full_path.push(nid);
                    *current_pos = (nx, ny, nz);
                    *current_id = nid;
                    return Ok(true);
                }
            }
            Ok(false)
        }

        // Last-ditch final hop
        if full_path.last() != Some(&dst_id) {
            let (lx, ly, lz) = all_node_pos.get(full_path.last().unwrap_or(&src_id)).copied().unwrap_or((x1, y1, z1));
            let last_is_n = all_node_is_neutron.get(full_path.last().unwrap_or(&src_id)).copied().unwrap_or(false);
            let last_range = if last_is_n { boosted_range } else { params.range };
            let d_final = ((lx-x2).powi(2)+(ly-y2).powi(2)+(lz-z2).powi(2)).sqrt();
            if d_final <= last_range * 1.5 {
                full_path.push(dst_id);
            } else {
                return Err(format!(
                    "Partial route only: reached {:.0} LY from destination but could not bridge the final gap. Base {:.2} LY / boosted {:.2} LY. Try a ship with longer jump range.",
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
        if use_astar {
        {
            let corridor_half = (boosted_range * 6.0).max(3000.0);
            let corridor_sq   = corridor_half * corridor_half;
            let buf           = corridor_half;
            let dv            = (x2 - x1, y2 - y1, z2 - z1);
            let dv_len_sq     = dv.0*dv.0 + dv.1*dv.1 + dv.2*dv.2;

            let mut stmt = conn.prepare("
                SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                FROM neutron_systems ns
                JOIN systems_index i ON ns.systemId64 = i.id
                JOIN systems s ON ns.systemId64 = s.id64
                WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
            ").map_err(|e| e.to_string())?;

            let rows: Vec<(i64, String, f64, f64, f64)> = stmt.query_map(
                rusqlite::params![x1.min(x2)-buf, x1.max(x2)+buf, y1.min(y2)-buf, y1.max(y2)+buf, z1.min(z2)-buf, z1.max(z2)+buf],
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
        }

        let cell_size = (boosted_range * 0.9).max(50.0);

        // Build GPU node list: all neutrons in corridor + src + dst
        let neutron_nodes_f32: Vec<(i64, f32, f32, f32)> = all_node_pos.iter()
            .filter(|(&id, _)| {
                all_node_is_neutron.get(&id).copied().unwrap_or(false)
                    || id == src_id || id == dst_id
            })
            .map(|(&id, &(nx, ny, nz))| (id, nx as f32, ny as f32, nz as f32))
            .collect();

        let t_astar_start = Instant::now();
        let h_fn = |x: f64, y: f64, z: f64| -> f64 {
            (((x-x2).powi(2)+(y-y2).powi(2)+(z-z2).powi(2)).sqrt() / boosted_range).ceil()
        };

        astar_result = if total_distance > 10_000.0 {
            info!("Using bidirectional A* for {:.0} LY route", total_distance);

            // Seeds — same logic as CPU: start from first/last neutron in greedy path
            let fwd_seed: (i64, u32) = full_path.iter().enumerate()
                .find(|(_, &id)| all_node_is_neutron.get(&id).copied().unwrap_or(false))
                .map(|(g, &id)| (id, g as u32)).unwrap_or((src_id, 0));
            let bwd_seed: (i64, u32) = full_path.iter().enumerate().rev()
                .find(|(_, &id)| all_node_is_neutron.get(&id).copied().unwrap_or(false))
                .map(|(g, &id)| (id, (full_path.len() - 1 - g) as u32)).unwrap_or((dst_id, 0));

            let fwd_bridge_end = full_path.iter().position(|&id| id == fwd_seed.0).unwrap_or(0);
            let bwd_bridge_end = full_path.iter().rposition(|&id| id == bwd_seed.0).unwrap_or(full_path.len()-1);

            // ── Try GPU bidirectional ─────────────────────────────────────────
            let gpu_middle: Option<Vec<i64>> = (|| {
                let vk = vk.as_ref()?;
                let graph = vk.build_graph(&neutron_nodes_f32, cell_size as f32)?;
                vk.run_bidirectional(
                    &graph,
                    src_id, dst_id,
                    fwd_seed.0, bwd_seed.0,
                    fwd_seed.1, bwd_seed.1,
                    boosted_range as f32,
                    greedy_jumps as u32,
                    REFINE_BUDGET_MS,
                )
            })();

            if let Some(middle) = gpu_middle {
                // Stitch: bridge_fwd + gpu_middle + bridge_bwd
                let mut path = full_path[..fwd_bridge_end].to_vec();
                path.extend_from_slice(&middle);
                path.extend_from_slice(&full_path[bwd_bridge_end+1..]);
                info!("Neutron GPU bidir A* found {} jumps (greedy {}), {}ms",
                    path.len()-1, greedy_jumps, t_astar_start.elapsed().as_millis());
                Some(path)
            } else {
                // ── CPU bidirectional fallback ────────────────────────────────
                if vk.is_some() { info!("GPU bidir A* returned no improvement, running CPU fallback"); }

                let h_bwd = |x: f64, y: f64, z: f64| -> f64 {
                    (((x-x1).powi(2)+(y-y1).powi(2)+(z-z1).powi(2)).sqrt() / boosted_range).ceil()
                };

                let mut astar_grid: HashMap<(i32,i32,i32), Vec<i64>> = HashMap::new();
                for (&nid, &(nx, ny, nz)) in &all_node_pos {
                    if all_node_is_neutron.get(&nid).copied().unwrap_or(false) {
                        let cell = ((nx/cell_size) as i32, (ny/cell_size) as i32, (nz/cell_size) as i32);
                        astar_grid.entry(cell).or_default().push(nid);
                    }
                }

                (|| {
                    let mut fwd_cf:     HashMap<i64, i64> = HashMap::new();
                    let mut bwd_cf:     HashMap<i64, i64> = HashMap::new();
                    let mut fwd_g:      HashMap<i64, u32> = HashMap::new();
                    let mut bwd_g:      HashMap<i64, u32> = HashMap::new();
                    let mut fwd_closed: HashSet<i64>      = HashSet::new();
                    let mut bwd_closed: HashSet<i64>      = HashSet::new();
                    let mut fwd_open:   BinaryHeap<HNode> = BinaryHeap::new();
                    let mut bwd_open:   BinaryHeap<HNode> = BinaryHeap::new();

                    let (fwd_seed_id, fwd_seed_g) = fwd_seed;
                    let (bwd_seed_id, bwd_seed_g) = bwd_seed;
                    let (fsx, fsy, fsz) = all_node_pos.get(&fwd_seed_id).copied().unwrap_or((x1, y1, z1));
                    let (bsx, bsy, bsz) = all_node_pos.get(&bwd_seed_id).copied().unwrap_or((x2, y2, z2));

                    fwd_g.insert(fwd_seed_id, fwd_seed_g);
                    bwd_g.insert(bwd_seed_id, bwd_seed_g);
                    fwd_open.push(HNode { g: fwd_seed_g, f: fwd_seed_g as f64 + h_fn(fsx, fsy, fsz), id: fwd_seed_id });
                    bwd_open.push(HNode { g: bwd_seed_g, f: bwd_seed_g as f64 + h_bwd(bsx, bsy, bsz), id: bwd_seed_id });

                    for &id in &full_path[..fwd_bridge_end] { fwd_closed.insert(id); }
                    for &id in &full_path[bwd_bridge_end+1..] { bwd_closed.insert(id); }
                    for i in 0..fwd_bridge_end {
                        if i + 1 < full_path.len() { fwd_cf.insert(full_path[i+1], full_path[i]); }
                        fwd_g.insert(full_path[i], i as u32);
                    }
                    for i in (bwd_bridge_end+1..full_path.len()).rev() {
                        if i > 0 { bwd_cf.insert(full_path[i-1], full_path[i]); }
                        bwd_g.insert(full_path[i], (full_path.len()-1-i) as u32);
                    }

                    let mut mu: u32 = greedy_jumps as u32;
                    let mut best_meeting: Option<i64> = None;

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

                    loop {
                        if t_astar_start.elapsed().as_millis() > REFINE_BUDGET_MS { break; }
                        if fwd_open.is_empty() && bwd_open.is_empty() { break; }
                        let fwd_min_g = fwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                        let bwd_min_g = bwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                        if fwd_min_g.saturating_add(bwd_min_g) >= mu { break; }

                        let fwd_min_f = fwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                        let bwd_min_f = bwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                        let expand_fwd = fwd_min_f <= bwd_min_f;

                        if expand_fwd {
                            let Some(HNode { g, id, .. }) = fwd_open.pop() else { continue; };
                            if g >= mu || fwd_closed.contains(&id) { continue; }
                            fwd_closed.insert(id);
                            if let Some(&bg) = bwd_g.get(&id) {
                                let total = g + bg;
                                if total < mu { mu = total; best_meeting = Some(id); }
                            }
                            let (cx, cy, cz) = match all_node_pos.get(&id) { Some(&p) => p, None => continue };
                            expand_neighbors!(cx, cy, cz, boosted_range, id, g, fwd_g, fwd_cf, fwd_open, fwd_closed, bwd_g, h_fn);
                        } else {
                            let Some(HNode { g, id, .. }) = bwd_open.pop() else { continue; };
                            if g >= mu || bwd_closed.contains(&id) { continue; }
                            bwd_closed.insert(id);
                            if let Some(&fg) = fwd_g.get(&id) {
                                let total = fg + g;
                                if total < mu { mu = total; best_meeting = Some(id); }
                            }
                            let (cx, cy, cz) = match all_node_pos.get(&id) { Some(&p) => p, None => continue };
                            expand_neighbors!(cx, cy, cz, boosted_range, id, g, bwd_g, bwd_cf, bwd_open, bwd_closed, fwd_g, h_bwd);
                        }
                    }

                    let m = best_meeting?;
                    let mut fwd_path: Vec<i64> = vec![m];
                    let mut cur = m;
                    while cur != fwd_seed_id { match fwd_cf.get(&cur) { Some(&p) => { cur = p; fwd_path.push(cur); } None => return None, } }
                    fwd_path.reverse();
                    let mut bwd_path: Vec<i64> = Vec::new();
                    let mut cur = m;
                    while cur != dst_id { match bwd_cf.get(&cur) { Some(&p) => { cur = p; bwd_path.push(cur); } None => break, } }
                    if *bwd_path.last().unwrap_or(&m) != dst_id { bwd_path.push(dst_id); }
                    fwd_path.extend(bwd_path);

                    // Stitch bridges back on
                    let mut final_path = full_path[..fwd_bridge_end].to_vec();
                    final_path.extend_from_slice(&fwd_path);
                    final_path.extend_from_slice(&full_path[bwd_bridge_end+1..]);
                    Some(final_path)
                })()
            }
        } else {
            // ── Unidirectional ────────────────────────────────────────────────
            let uni_seed: (i64, u32) = full_path.iter().enumerate()
                .find(|(_, &id)| all_node_is_neutron.get(&id).copied().unwrap_or(false))
                .map(|(g, &id)| (id, g as u32)).unwrap_or((src_id, 0));
            let (uni_seed_id, uni_seed_g) = uni_seed;
            let uni_bridge_end = full_path.iter().position(|&id| id == uni_seed_id).unwrap_or(0);

            // ── Try GPU unidirectional ────────────────────────────────────────
            let gpu_tail: Option<Vec<i64>> = (|| {
                let vk = vk.as_ref()?;
                let graph = vk.build_graph(&neutron_nodes_f32, cell_size as f32)?;
                let remaining_greedy = (greedy_jumps as u32).saturating_sub(uni_seed_g);
                vk.run_unidirectional(
                    &graph,
                    uni_seed_id, dst_id,
                    boosted_range as f32,
                    remaining_greedy,
                    REFINE_BUDGET_MS,
                )
            })();

            if let Some(tail) = gpu_tail {
                let mut path = full_path[..uni_bridge_end].to_vec();
                path.extend_from_slice(&tail);
                info!("Neutron GPU uni A* found {} jumps (greedy {}), {}ms",
                    path.len()-1, greedy_jumps, t_astar_start.elapsed().as_millis());
                Some(path)
            } else {
                // ── CPU unidirectional fallback ───────────────────────────────
                if vk.is_some() { info!("GPU uni A* returned no improvement, running CPU fallback"); }

                let mut astar_grid: HashMap<(i32,i32,i32), Vec<i64>> = HashMap::new();
                for (&nid, &(nx, ny, nz)) in &all_node_pos {
                    if all_node_is_neutron.get(&nid).copied().unwrap_or(false) {
                        let cell = ((nx/cell_size) as i32, (ny/cell_size) as i32, (nz/cell_size) as i32);
                        astar_grid.entry(cell).or_default().push(nid);
                    }
                }

                (|| {
                    let mut came_from: HashMap<i64, i64> = HashMap::new();
                    let mut g_score:   HashMap<i64, u32> = HashMap::new();
                    let mut closed:    HashSet<i64>      = HashSet::new();
                    let mut open:      BinaryHeap<HNode> = BinaryHeap::new();

                    let (usx, usy, usz) = all_node_pos.get(&uni_seed_id).copied().unwrap_or((x1, y1, z1));
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
                            while cur != src_id { match came_from.get(&cur) { Some(&p) => { cur = p; path.push(cur); } None => return None, } }
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
                                g_score.insert(dst_id, tg); came_from.insert(dst_id, id);
                                open.push(HNode { g: tg, f: tg as f64, id: dst_id });
                            }
                        }
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
                                        g_score.insert(n_id, tg); came_from.insert(n_id, id);
                                        open.push(HNode { g: tg, f: tg as f64 + h_fn(nx, ny, nz), id: n_id });
                                    }
                                }
                            }
                        }}}
                    }
                    None
                })()
            }
        };

        } // end if use_astar

        // ── Path selection ────────────────────────────────────────────────────
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
                let nm  = all_node_name.get(&nid).cloned().unwrap_or_default();
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
                } else { (nx, ny, nz) };
                let dist = ((nx_next-nx).powi(2)+(ny_next-ny).powi(2)+(nz_next-nz).powi(2)).sqrt();
                (dist, is_n)
            } else { (0.0, false) };

            route_json.push(serde_json::json!({
                "system": nname, "id64": nid.to_string(),
                "distance_from_start": (dist_from_start*100.0).round()/100.0,
                "distance_to_destination": (d_dest*100.0).round()/100.0,
                "jump_distance": (jdist*100.0).round()/100.0,
                "used_neutron_boost": used_boost,
                "is_neutron": is_n,
            }));
            dist_from_start += jdist;
        }

        Ok(serde_json::json!({
            "source": params.source, "destination": params.destination,
            "total_distance_ly": (total_distance*100.0).round()/100.0,
            "totalJumps": route_json.len().saturating_sub(1),
            "optimised": is_optimal,
            "route": route_json,
        }))
    }).await.unwrap().map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    Ok(Json(result))
}

pub async fn neutron_route_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<NeutronRouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_neutron_route(state, params).await
}