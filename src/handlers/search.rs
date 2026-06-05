use axum::{extract::{Query, State}, http::StatusCode, Json};
use std::sync::Arc;

use crate::models::CubeSearchQuery;
use crate::state::AppState;

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
        let custom_filter = params.custom_filter.unwrap_or_default();

        // custom_filter takes precedence over body_type
        let effective_filter = if !custom_filter.is_empty() { custom_filter.to_lowercase() } else { body_filter.to_lowercase() };
        let eff = effective_filter.as_str();

        let mut target_subtypes: Vec<&str> = Vec::new();
        let mut is_terraformable = false;
        let mut ring_type_filter: Option<String> = None;
        let mut require_bio = false;
        let mut require_geo = false;
        let mut require_landable = false;
        let mut include_white_dwarfs = false;
        let mut include_wolf_rayet = false;
        let mut white_dwarf_specific: Vec<&str> = Vec::new();
        let mut wolf_rayet_specific: Vec<&str> = Vec::new();
        let mut star_class_subtypes: Vec<&str> = Vec::new();

        // -- Terrestrial planets --
        if eff.contains("earth-like") { target_subtypes.push("Earth-like world"); }
        if eff.contains("water world") { target_subtypes.push("Water world"); }
        if eff.contains("ammonia world") { target_subtypes.push("Ammonia world"); }
        if eff.contains("high metal content") { target_subtypes.push("High metal content world"); }
        if eff.contains("metal-rich body") { target_subtypes.push("Metal-rich body"); }
        if eff.contains("rocky body") { target_subtypes.push("Rocky body"); }
        if eff.contains("rocky ice world") { target_subtypes.push("Rocky Ice world"); }
        if eff.contains("icy body") { target_subtypes.push("Icy body"); }

        // -- Gas giants --
        if eff.contains("class i gas giant") { target_subtypes.push("Class I gas giant"); }
        if eff.contains("class ii gas giant") { target_subtypes.push("Class II gas giant"); }
        if eff.contains("class iii gas giant") { target_subtypes.push("Class III gas giant"); }
        if eff.contains("class iv gas giant") { target_subtypes.push("Class IV gas giant"); }
        if eff.contains("class v gas giant") { target_subtypes.push("Class V gas giant"); }
        if eff.contains("water-based life giant") { target_subtypes.push("Gas giant with water-based life"); }
        if eff.contains("ammonia-based life giant") { target_subtypes.push("Gas giant with ammonia-based life"); }
        if eff.contains("helium-rich gas giant") { target_subtypes.push("Helium-rich gas giant"); }
        else if eff.contains("helium gas giant") { target_subtypes.push("Helium gas giant"); }
        if eff.contains("water giant") { target_subtypes.push("Water giant"); }

        // -- Compact / exotic objects --
        if eff.contains("neutron star") { target_subtypes.push("Neutron Star"); }
        if eff.contains("supermassive black hole") { target_subtypes.push("Supermassive Black Hole"); }
        else if eff.contains("black hole") { target_subtypes.push("Black Hole"); }

        // White dwarfs — specific subtypes first, then catch-all
        if eff.contains("white dwarf dab") { white_dwarf_specific.push("White Dwarf (DAB) Star"); }
        if eff.contains("white dwarf dav") { white_dwarf_specific.push("White Dwarf (DAV) Star"); }
        if eff.contains("white dwarf daz") { white_dwarf_specific.push("White Dwarf (DAZ) Star"); }
        if eff.contains("white dwarf da") && !eff.contains("white dwarf dab") && !eff.contains("white dwarf dav") && !eff.contains("white dwarf daz") {
            white_dwarf_specific.push("White Dwarf (DA) Star");
        }
        if eff.contains("white dwarf dbv") { white_dwarf_specific.push("White Dwarf (DBV) Star"); }
        if eff.contains("white dwarf dbz") { white_dwarf_specific.push("White Dwarf (DBZ) Star"); }
        if eff.contains("white dwarf db") && !eff.contains("white dwarf dbv") && !eff.contains("white dwarf dbz") {
            white_dwarf_specific.push("White Dwarf (DB) Star");
        }
        if eff.contains("white dwarf dcv") { white_dwarf_specific.push("White Dwarf (DCV) Star"); }
        if eff.contains("white dwarf dc") && !eff.contains("white dwarf dcv") {
            white_dwarf_specific.push("White Dwarf (DC) Star");
        }
        if eff.contains("white dwarf dq") { white_dwarf_specific.push("White Dwarf (DQ) Star"); }
        // "white dwarf d" (generic D, not DA/DB/DC/DQ)
        if eff.contains("white dwarf d") && !eff.contains("white dwarf da") && !eff.contains("white dwarf db") && !eff.contains("white dwarf dc") && !eff.contains("white dwarf dq") {
            white_dwarf_specific.push("White Dwarf (D) Star");
        }
        // Catch-all: "white dwarf" without any specific subtype selects all
        if eff.contains("white dwarf") && white_dwarf_specific.is_empty() {
            include_white_dwarfs = true;
        }

        // Wolf-Rayet stars — specific variants first
        if eff.contains("wolf-rayet c") && !eff.contains("wolf-rayet nc") {
            wolf_rayet_specific.push("Wolf-Rayet C Star");
        }
        if eff.contains("wolf-rayet n") && !eff.contains("wolf-rayet nc") {
            wolf_rayet_specific.push("Wolf-Rayet N Star");
        }
        if eff.contains("wolf-rayet nc") { wolf_rayet_specific.push("Wolf-Rayet NC Star"); }
        if eff.contains("wolf-rayet o") { wolf_rayet_specific.push("Wolf-Rayet O Star"); }
        if eff.contains("wolf-rayet") && wolf_rayet_specific.is_empty() {
            include_wolf_rayet = true;
        }

        // -- Main-sequence star classes --
        if eff.contains("o star") {
            star_class_subtypes.push("O (Blue-White) Star");
        }
        if eff.contains("b star") {
            star_class_subtypes.push("B (Blue-White) Star");
            star_class_subtypes.push("B (Blue-White super giant) Star");
        }
        if eff.contains("a star") {
            star_class_subtypes.push("A (Blue-White) Star");
            star_class_subtypes.push("A (Blue-White super giant) Star");
        }
        if eff.contains("f star") {
            star_class_subtypes.push("F (White) Star");
            star_class_subtypes.push("F (White super giant) Star");
        }
        if eff.contains("g star") {
            star_class_subtypes.push("G (White-Yellow) Star");
            star_class_subtypes.push("G (White-Yellow super giant) Star");
        }
        if eff.contains("k star") {
            star_class_subtypes.push("K (Yellow-Orange) Star");
            star_class_subtypes.push("K (Yellow-Orange giant) Star");
        }
        if eff.contains("m star") {
            star_class_subtypes.push("M (Red dwarf) Star");
            star_class_subtypes.push("M (Red giant) Star");
            star_class_subtypes.push("M (Red super giant) Star");
        }
        if eff.contains("l dwarf") { star_class_subtypes.push("L (Brown dwarf) Star"); }
        if eff.contains("t dwarf") { star_class_subtypes.push("T (Brown dwarf) Star"); }
        if eff.contains("y dwarf") { star_class_subtypes.push("Y (Brown dwarf) Star"); }
        if eff.contains("t tauri") { star_class_subtypes.push("T Tauri Star"); }
        if eff.contains("herbig") { star_class_subtypes.push("Herbig Ae/Be Star"); }
        if eff.contains("c star type") { star_class_subtypes.push("C Star"); }
        if eff.contains("cj star type") { star_class_subtypes.push("CJ Star"); }
        if eff.contains("cn star type") { star_class_subtypes.push("CN Star"); }
        if eff.contains("ms star type") { star_class_subtypes.push("MS-type Star"); }
        if eff.contains("s star type") { star_class_subtypes.push("S-type Star"); }

        // -- Properties --
        if eff.contains("terraformable") { is_terraformable = true; }
        if eff.contains("bio") || eff.contains("biological") { require_bio = true; }
        if eff.contains("geo") || eff.contains("geological") { require_geo = true; }
        if eff.contains("landable") { require_landable = true; }

        // -- Rings --
        if eff.contains("icy ring") { ring_type_filter = Some("Icy".to_string()); }
        else if eff.contains("metallic ring") { ring_type_filter = Some("Metallic".to_string()); }
        else if eff.contains("metal rich ring") { ring_type_filter = Some("Metal Rich".to_string()); }
        else if eff.contains("rocky ring") { ring_type_filter = Some("Rocky".to_string()); }
        else if eff.contains("rings") || eff.contains("ring") { ring_type_filter = Some("%".to_string()); }

        let mut results: Vec<serde_json::Value>;

        let has_subtypes = !target_subtypes.is_empty();
        let has_star_classes = !star_class_subtypes.is_empty();
        let has_wd_specific = !white_dwarf_specific.is_empty();
        let has_wr_specific = !wolf_rayet_specific.is_empty();
        let has_any_filter = is_terraformable || require_bio || require_geo || require_landable
            || ring_type_filter.is_some() || has_subtypes || include_white_dwarfs
            || has_star_classes || has_wd_specific || has_wr_specific || include_wolf_rayet;

        if has_any_filter {
            let mut sql = String::from("
                SELECT s.id64, s.name as systemName, s.population, i.minX, i.minY, i.minZ,
                       b.bodyId, b.name as bodyName, b.subType, b.distanceToArrival
                FROM systems_index i JOIN systems s ON i.id = s.id64
                JOIN bodies b ON s.id64 = b.systemId64
                WHERE i.minX >= ? AND i.maxX <= ? AND i.minY >= ? AND i.maxY <= ? AND i.minZ >= ? AND i.maxZ <= ?");

            let mut ring_param: Option<String> = None;

            if is_terraformable {
                sql.push_str(" AND (b.terraformingState = 'Terraformable' OR b.terraformingState = 'Terraforming completed')");
            }
            if require_bio {
                sql.push_str(" AND b.signals LIKE '%Biological%'");
            }
            if require_geo {
                sql.push_str(" AND b.signals LIKE '%Geological%'");
            }
            if require_landable {
                sql.push_str(" AND b.isLandable = 1");
            }
            if let Some(ref rt) = ring_type_filter {
                if rt == "%" {
                    sql.push_str(" AND b.rings IS NOT NULL AND b.rings != '[]'");
                } else {
                    ring_param = Some(format!("%\"{}%", rt));
                    sql.push_str(" AND b.rings LIKE ?");
                }
            }

            // Build the combined subtype OR clause
            let needs_subtype_clause = has_subtypes || include_white_dwarfs || has_star_classes
                || has_wd_specific || has_wr_specific || include_wolf_rayet;
            if needs_subtype_clause {
                sql.push_str(" AND (");
                let mut subtype_conds = Vec::new();
                if has_subtypes {
                    let placeholders = target_subtypes.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    subtype_conds.push(format!("b.subType IN ({})", placeholders));
                }
                if has_star_classes {
                    let placeholders = star_class_subtypes.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    subtype_conds.push(format!("b.subType IN ({})", placeholders));
                }
                if has_wd_specific {
                    let placeholders = white_dwarf_specific.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    subtype_conds.push(format!("b.subType IN ({})", placeholders));
                }
                if include_white_dwarfs {
                    subtype_conds.push("b.subType LIKE 'White Dwarf%'".to_string());
                }
                if has_wr_specific {
                    let placeholders = wolf_rayet_specific.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    subtype_conds.push(format!("b.subType IN ({})", placeholders));
                }
                if include_wolf_rayet {
                    subtype_conds.push("b.subType LIKE 'Wolf-Rayet%'".to_string());
                }
                sql.push_str(&subtype_conds.join(" OR "));
                sql.push_str(")");
            }
            sql.push_str(" LIMIT 5000");

            let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
            let mut query_params: Vec<&dyn rusqlite::ToSql> = vec![&min_x, &max_x, &min_y, &max_y, &min_z, &max_z];
            
            if let Some(ref rp) = ring_param {
                query_params.push(rp);
            }
            for st in &target_subtypes {
                query_params.push(st);
            }
            for st in &star_class_subtypes {
                query_params.push(st);
            }
            for st in &white_dwarf_specific {
                query_params.push(st);
            }
            for st in &wolf_rayet_specific {
                query_params.push(st);
            }

            let rows = stmt.query_map(&*query_params, |row| {
                let id64: i64 = row.get(0)?;
                let sys_name: String = row.get(1)?;
                let pop: i64 = row.get(2)?;
                let x: f64 = row.get(3)?; let y: f64 = row.get(4)?; let z: f64 = row.get(5)?;
                let body_id: i64 = row.get(6)?;
                let body_name: String = row.get(7)?;
                let sub_type: String = row.get(8)?;
                let dist_arrival: f64 = row.get(9)?;
                let dist_ly = ((x - cx).powi(2) + (y - cy).powi(2) + (z - cz).powi(2)).sqrt();
                let body_short = body_name.replace(&sys_name, "").trim().to_string();
                Ok(serde_json::json!({
                    "systemId64": id64.to_string(), "bodyId": body_id,
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
            }).map_err(|e| e.to_string())?;
            results = rows.filter_map(Result::ok).collect();
        } else {
            let mut stmt = conn.prepare("
                SELECT s.id64, s.name, s.population, i.minX, i.minY, i.minZ
                FROM systems_index i JOIN systems s ON i.id = s.id64
                WHERE i.minX >= ? AND i.maxX <= ? AND i.minY >= ? AND i.maxY <= ? AND i.minZ >= ? AND i.maxZ <= ? LIMIT 5000
            ").map_err(|e| e.to_string())?;
            let rows = stmt.query_map(rusqlite::params![min_x, max_x, min_y, max_y, min_z, max_z], |row| {
                let id64: i64 = row.get(0)?;
                let sys_name: String = row.get(1)?;
                let pop: i64 = row.get(2)?;
                let (x, y, z): (f64, f64, f64) = (row.get(3)?, row.get(4)?, row.get(5)?);
                let dist = ((x-cx).powi(2) + (y-cy).powi(2) + (z-cz).powi(2)).sqrt();
                Ok(serde_json::json!({
                    "systemId64": id64.to_string(), "uniqueId": id64.to_string(),
                    "system": sys_name, "body": "-", "systemDistLy": (dist * 100.0).round() / 100.0,
                    "arrivalDistLs": 0, "inhabited": if pop > 0 { "Yes" } else { "No" },
                    "coords": {"x": x, "y": y, "z": z}
                }))
            }).map_err(|e| e.to_string())?;
            results = rows.filter_map(Result::ok).collect();
        }

        results.sort_by(|a, b| a["systemDistLy"].as_f64().unwrap().partial_cmp(&b["systemDistLy"].as_f64().unwrap()).unwrap());
        Ok(serde_json::json!({"cubeSize": h*2.0, "count": results.len(), "results": results}))
    }).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(result))
}

pub async fn cube_search_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CubeSearchQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_cube_search(state, params).await
}

pub async fn cube_search_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<CubeSearchQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    do_cube_search(state, params).await
}
