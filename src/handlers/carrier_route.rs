use axum::{extract::State, http::StatusCode, Json};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use tracing::info;

use crate::config::{CARRIER_REFINE_BUDGET_MS, CARRIER_JUMP_RANGE};
use crate::models::CarrierRouteQuery;
use crate::state::AppState;

async fn do_carrier_route(
    state: Arc<AppState>,
    params: CarrierRouteQuery,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let _permit = state.astar_semaphore.acquire().await.map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "Server overloaded — A* queue full".into()))?;
    let pool = state.db_pool.clone();
    let vk   = state.vulkan_astar.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        let t_start = Instant::now();

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
                    rusqlite::params![id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                ).map_err(|_| format!("System ID '{}' not found", id))
            } else {
                conn.query_row(
                    "SELECT s.id64, s.name, i.minX, i.minY, i.minZ FROM systems s JOIN systems_index i ON s.id64 = i.id WHERE s.name = ? COLLATE NOCASE LIMIT 1",
                    rusqlite::params![sys_input], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
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

        // ── Greedy baseline ───────────────────────────────────────────────────
        let mut greedy_path: Vec<(i64, String, f64, f64, f64)> = vec![(src_id, src_name.clone(), x1, y1, z1)];
        {
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let mut current_pos = (x1, y1, z1);
            let mut _current_id = src_id;
            let mut current_sys_name = params.current_system.clone();
            let mut distance_remaining = total_distance;

            for _ in 0..500usize {
                if distance_remaining <= CARRIER_JUMP_RANGE { break; }

                let v_x = x2 - current_pos.0; let v_y = y2 - current_pos.1; let v_z = z2 - current_pos.2;
                let v_mag = (v_x.powi(2) + v_y.powi(2) + v_z.powi(2)).sqrt();
                let u_x = v_x / v_mag; let u_y = v_y / v_mag; let u_z = v_z / v_mag;

                let target_dist = 498.5;
                let t_x = current_pos.0 + u_x * target_dist;
                let t_y = current_pos.1 + u_y * target_dist;
                let t_z = current_pos.2 + u_z * target_dist;

                let mut best: Option<(i64, String, f64, f64, f64, f64)> = None;
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
            greedy_path.push((dest_id, dest_name.clone(), x2, y2, z2));
        }

        let greedy_jumps = greedy_path.len() - 1;
        info!("Carrier greedy: {} jumps in {}ms", greedy_jumps, t_start.elapsed().as_millis());

        let use_astar = params.engine.as_deref().unwrap_or("greedy").to_lowercase() != "greedy";
        let mut astar_path: Option<Vec<(i64, String, f64, f64, f64)>> = None;

        // ── A* refinement ─────────────────────────────────────────────────────
        if use_astar {
            let corridor_half = 1500.0f64;
            let corridor_sq   = corridor_half * corridor_half;
            let dv            = (x2 - x1, y2 - y1, z2 - z1);
            let dv_len_sq     = dv.0*dv.0 + dv.1*dv.1 + dv.2*dv.2;

            let mut preload_stmt = conn.prepare("
                SELECT s.id64, s.name, i.minX, i.minY, i.minZ
                FROM systems_index i JOIN systems s ON i.id = s.id64
                WHERE i.minX BETWEEN ? AND ? AND i.minY BETWEEN ? AND ? AND i.minZ BETWEEN ? AND ?
            ").map_err(|e| e.to_string())?;

            let buf = corridor_half;
            let all_systems: Vec<(i64, String, f64, f64, f64)> = preload_stmt.query_map(
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

            info!("Carrier A* corridor preload: {} systems", all_systems.len());

            // Build CPU node maps (needed for path→response conversion regardless of GPU/CPU path)
            let mut node_pos:  HashMap<i64, (f64, f64, f64)> = HashMap::with_capacity(all_systems.len());
            let mut node_name: HashMap<i64, String>          = HashMap::with_capacity(all_systems.len());
            let cell_size = (CARRIER_JUMP_RANGE * 0.9).max(50.0);

            for &(id, ref name, nx, ny, nz) in &all_systems {
                node_pos.insert(id, (nx, ny, nz));
                node_name.insert(id, name.clone());
            }
            node_pos.entry(src_id).or_insert((x1, y1, z1));
            node_name.entry(src_id).or_insert(src_name.clone());
            node_pos.entry(dest_id).or_insert((x2, y2, z2));
            node_name.entry(dest_id).or_insert(dest_name.clone());

            let t_astar_start = Instant::now();
            let h_fn = |x: f64, y: f64, z: f64| -> f64 {
                (((x-x2).powi(2)+(y-y2).powi(2)+(z-z2).powi(2)).sqrt() / CARRIER_JUMP_RANGE).ceil()
            };

            // Helper: convert id64 path to the tuple vec the rest of the function expects
            let ids_to_path = |ids: Vec<i64>| -> Vec<(i64, String, f64, f64, f64)> {
                ids.iter().map(|&id| {
                    let name = node_name.get(&id).cloned().unwrap_or_default();
                    let (nx, ny, nz) = node_pos.get(&id).copied().unwrap_or((0., 0., 0.));
                    (id, name, nx, ny, nz)
                }).collect()
            };

            // ── Try GPU A* ────────────────────────────────────────────────────
            let gpu_ids: Option<Vec<i64>> = (|| {
                let vk = vk.as_ref()?;
                let nodes_f32: Vec<(i64, f32, f32, f32)> = node_pos.iter()
                    .map(|(&id, &(nx, ny, nz))| (id, nx as f32, ny as f32, nz as f32))
                    .collect();
                let graph = vk.build_graph(&nodes_f32, cell_size as f32)?;
                if total_distance > 5_000.0 {
                    vk.run_bidirectional(
                        &graph,
                        src_id, dest_id,
                        src_id, dest_id, 0, 0,   // no bridge seeding needed for carrier
                        CARRIER_JUMP_RANGE as f32,
                        greedy_jumps as u32,
                        CARRIER_REFINE_BUDGET_MS,
                    )
                } else {
                    vk.run_unidirectional(
                        &graph,
                        src_id, dest_id,
                        CARRIER_JUMP_RANGE as f32,
                        greedy_jumps as u32,
                        CARRIER_REFINE_BUDGET_MS,
                    )
                }
            })();

            if let Some(ids) = gpu_ids {
                info!("Carrier GPU A* found {} jumps (greedy {}), {}ms",
                    ids.len() - 1, greedy_jumps, t_astar_start.elapsed().as_millis());
                astar_path = Some(ids_to_path(ids));
            } else {
                // ── CPU A* fallback ───────────────────────────────────────────
                if vk.is_some() {
                    info!("GPU A* returned no improvement, running CPU fallback");
                }

                // Build spatial grid (only needed for CPU path)
                let mut grid: HashMap<(i32, i32, i32), Vec<i64>> = HashMap::new();
                for (&id, &(nx, ny, nz)) in &node_pos {
                    let cell = ((nx/cell_size) as i32, (ny/cell_size) as i32, (nz/cell_size) as i32);
                    grid.entry(cell).or_default().push(id);
                }

                let cpu_ids: Option<Vec<i64>> = if total_distance > 5_000.0 {
                    let h_bwd = |x: f64, y: f64, z: f64| -> f64 {
                        (((x-x1).powi(2)+(y-y1).powi(2)+(z-z1).powi(2)).sqrt() / CARRIER_JUMP_RANGE).ceil()
                    };
                    (|| {
                        let mut fwd_cf:     HashMap<i64, i64> = HashMap::new();
                        let mut bwd_cf:     HashMap<i64, i64> = HashMap::new();
                        let mut fwd_g:      HashMap<i64, u32> = HashMap::new();
                        let mut bwd_g:      HashMap<i64, u32> = HashMap::new();
                        let mut fwd_closed: HashSet<i64>      = HashSet::new();
                        let mut bwd_closed: HashSet<i64>      = HashSet::new();
                        let mut fwd_open:   BinaryHeap<CNode> = BinaryHeap::new();
                        let mut bwd_open:   BinaryHeap<CNode> = BinaryHeap::new();

                        fwd_g.insert(src_id, 0);
                        bwd_g.insert(dest_id, 0);
                        fwd_open.push(CNode { g: 0, f: h_fn(x1, y1, z1), id: src_id });
                        bwd_open.push(CNode { g: 0, f: h_bwd(x2, y2, z2), id: dest_id });

                        let mut mu: u32          = greedy_jumps as u32;
                        let mut best_meeting: Option<i64> = None;
                        let rsq = CARRIER_JUMP_RANGE * CARRIER_JUMP_RANGE;

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

                        loop {
                            if t_astar_start.elapsed().as_millis() > CARRIER_REFINE_BUDGET_MS { break; }
                            if fwd_open.is_empty() && bwd_open.is_empty() { break; }
                            let fwd_min_g = fwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                            let bwd_min_g = bwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
                            if fwd_min_g.saturating_add(bwd_min_g) >= mu { break; }

                            let fwd_min_f = fwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                            let bwd_min_f = bwd_open.peek().map(|n| n.f).unwrap_or(f64::MAX);
                            let expand_fwd = fwd_min_f <= bwd_min_f;

                            if expand_fwd {
                                let Some(CNode { g, id, .. }) = fwd_open.pop() else { continue; };
                                if g >= mu || fwd_closed.contains(&id) { continue; }
                                fwd_closed.insert(id);
                                if let Some(&bg) = bwd_g.get(&id) {
                                    let total = g + bg;
                                    if total < mu { mu = total; best_meeting = Some(id); }
                                }
                                let (cx, cy, cz) = match node_pos.get(&id) { Some(&p) => p, None => continue };
                                let d_dst = ((cx-x2).powi(2)+(cy-y2).powi(2)+(cz-z2).powi(2)).sqrt();
                                if d_dst <= CARRIER_JUMP_RANGE {
                                    let tg = g + 1;
                                    if tg < *fwd_g.get(&dest_id).unwrap_or(&u32::MAX) {
                                        fwd_g.insert(dest_id, tg); fwd_cf.insert(dest_id, id);
                                        fwd_open.push(CNode { g: tg, f: tg as f64, id: dest_id });
                                        if tg < mu { mu = tg; best_meeting = Some(dest_id); }
                                    }
                                }
                                expand_carrier!(cx, cy, cz, id, g, fwd_g, fwd_cf, fwd_open, fwd_closed, bwd_g, h_fn);
                            } else {
                                let Some(CNode { g, id, .. }) = bwd_open.pop() else { continue; };
                                if g >= mu || bwd_closed.contains(&id) { continue; }
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
                                        bwd_g.insert(src_id, tg); bwd_cf.insert(src_id, id);
                                        bwd_open.push(CNode { g: tg, f: tg as f64, id: src_id });
                                        if tg < mu { mu = tg; best_meeting = Some(src_id); }
                                    }
                                }
                                expand_carrier!(cx, cy, cz, id, g, bwd_g, bwd_cf, bwd_open, bwd_closed, fwd_g, h_bwd);
                            }
                        }

                        let m = best_meeting?;
                        let mut fwd_path: Vec<i64> = vec![m];
                        let mut cur = m;
                        while cur != src_id { match fwd_cf.get(&cur) { Some(&p) => { cur = p; fwd_path.push(cur); } None => return None, } }
                        fwd_path.reverse();
                        let mut bwd_path: Vec<i64> = Vec::new();
                        let mut cur = m;
                        while cur != dest_id { match bwd_cf.get(&cur) { Some(&p) => { cur = p; bwd_path.push(cur); } None => break, } }
                        if *bwd_path.last().unwrap_or(&m) != dest_id { bwd_path.push(dest_id); }
                        fwd_path.extend(bwd_path);
                        Some(fwd_path)
                    })()
                } else {
                    // Unidirectional
                    (|| {
                        let mut came_from: HashMap<i64, i64> = HashMap::new();
                        let mut g_score:   HashMap<i64, u32> = HashMap::new();
                        let mut closed:    HashSet<i64>      = HashSet::new();
                        let mut open:      BinaryHeap<CNode> = BinaryHeap::new();
                        let rsq = CARRIER_JUMP_RANGE * CARRIER_JUMP_RANGE;

                        g_score.insert(src_id, 0);
                        open.push(CNode { g: 0, f: h_fn(x1, y1, z1), id: src_id });

                        while let Some(CNode { g, id, .. }) = open.pop() {
                            if t_astar_start.elapsed().as_millis() > CARRIER_REFINE_BUDGET_MS { return None; }
                            if g as usize >= greedy_jumps { continue; }
                            if id == dest_id {
                                let mut path = vec![dest_id];
                                let mut cur = dest_id;
                                while cur != src_id { match came_from.get(&cur) { Some(&p) => { cur = p; path.push(cur); } None => return None, } }
                                path.reverse();
                                return Some(path);
                            }
                            if closed.contains(&id) { continue; }
                            closed.insert(id);
                            let (cx, cy, cz) = match node_pos.get(&id) { Some(&p) => p, None => continue };
                            let d_dst = ((cx-x2).powi(2)+(cy-y2).powi(2)+(cz-z2).powi(2)).sqrt();
                            if d_dst <= CARRIER_JUMP_RANGE && !closed.contains(&dest_id) {
                                let tg = g + 1;
                                if tg < *g_score.get(&dest_id).unwrap_or(&u32::MAX) {
                                    g_score.insert(dest_id, tg); came_from.insert(dest_id, id);
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
                                            g_score.insert(n_id, tg); came_from.insert(n_id, id);
                                            open.push(CNode { g: tg, f: tg as f64 + h_fn(nx, ny, nz), id: n_id });
                                        }
                                    }
                                }
                            }}}
                        }
                        None
                    })()
                };

                if let Some(ids) = cpu_ids {
                    info!("Carrier CPU A* found {} jumps (greedy {}), {}ms",
                        ids.len() - 1, greedy_jumps, t_astar_start.elapsed().as_millis());
                    astar_path = Some(ids_to_path(ids));
                } else {
                    info!("Carrier A* did not improve on greedy ({} jumps), {}ms",
                        greedy_jumps, t_start.elapsed().as_millis());
                }
            }
        } else {
            info!("Engine: greedy only, skipping A* refinement ({} jumps)", greedy_jumps);
        }

        // ── Select final path ─────────────────────────────────────────────────
        let final_path = if use_astar { astar_path.unwrap_or(greedy_path) } else { greedy_path };
        let is_optimal = final_path.len() - 1 < greedy_jumps;

        // ── Fuel simulation ───────────────────────────────────────────────────
        let mut tank = params.tank_fuel;
        let mut market = params.stored_tritium;
        let mut total_fuel_used = 0.0;
        let mut jumps_json: Vec<serde_json::Value> = Vec::with_capacity(final_path.len());

        for step in 0..final_path.len() {
            let (nid, ref nname, nx, ny, nz) = final_path[step];
            let dist_from_start = ((nx - x1).powi(2) + (ny - y1).powi(2) + (nz - z1).powi(2)).sqrt();
            let dist_to_dest    = ((nx - x2).powi(2) + (ny - y2).powi(2) + (nz - z2).powi(2)).sqrt();

            if step == 0 {
                jumps_json.push(serde_json::json!({
                    "system": nname, "id64": nid.to_string(),
                    "distance_from_start": 0.0,
                    "distance_to_destination": (dist_to_dest * 100.0).round() / 100.0,
                    "jump_distance": 0.0, "fuel_used": 0.0,
                    "fuel_left_tank": tank, "tritium_in_market": market,
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
                    "system": nname, "id64": nid.to_string(),
                    "distance_from_start": (dist_from_start * 100.0).round() / 100.0,
                    "distance_to_destination": (dist_to_dest * 100.0).round() / 100.0,
                    "jump_distance": (jdist * 100.0).round() / 100.0,
                    "fuel_used": jump_fuel, "fuel_left_tank": tank,
                    "tritium_in_market": market, "has_enough_fuel": has_enough_fuel
                }));
            }
        }

        Ok(serde_json::json!({
            "source": params.current_system, "destination": params.destination,
            "is_squadron": is_squadron, "optimised": is_optimal,
            "total_distance_ly": (total_distance * 100.0).round() / 100.0,
            "base_cargo_capacity_used": base_cargo,
            "initial_fuel_tank": params.tank_fuel,
            "initial_market_tritium": params.stored_tritium,
            "total_fuel_used": total_fuel_used,
            "final_fuel_tank": tank, "final_market_tritium": market,
            "totalJumps": jumps_json.len().saturating_sub(1),
            "route": jumps_json
        }))
    }).await.unwrap().map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    Ok(Json(result))
}

pub async fn carrier_route_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<CarrierRouteQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_carrier_route(state, params).await
}