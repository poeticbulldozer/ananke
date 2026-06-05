use serde::Deserialize;

// --- Core data types ---

#[derive(Deserialize, Debug, Clone)]
pub struct SpanshCoords {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
pub struct SpanshSystem {
    pub id64: i64,
    pub name: String,
    #[serde(default)]
    pub population: Option<i64>,
    #[serde(default)]
    pub coords: Option<SpanshCoords>,
    #[serde(default)]
    pub bodies: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub stations: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub allegiance: Option<String>,
    #[serde(default)]
    pub government: Option<String>,
    #[serde(default, rename = "primaryEconomy")]
    pub primary_economy: Option<String>,
    #[serde(default, rename = "secondaryEconomy")]
    pub secondary_economy: Option<String>,
    #[serde(default)]
    pub security: Option<String>,
    #[serde(default, rename = "bodyCount")]
    pub body_count: Option<i64>,
    #[serde(default)]
    pub date: Option<String>,
    #[serde(default, rename = "controllingFaction")]
    pub controlling_faction: Option<serde_json::Value>,
    #[serde(default)]
    pub factions: Option<serde_json::Value>,
    #[serde(default, rename = "powerState")]
    pub power_state: Option<String>,
    #[serde(default)]
    pub powers: Option<serde_json::Value>,
    #[serde(default, rename = "controllingPower")]
    pub controlling_power: Option<String>,
    #[serde(default, rename = "powerStateControlProgress")]
    pub power_state_control_progress: Option<f64>,
    #[serde(default, rename = "powerStateReinforcement")]
    pub power_state_reinforcement: Option<f64>,
    #[serde(default, rename = "powerStateUndermining")]
    pub power_state_undermining: Option<f64>,
    #[serde(default, rename = "powerConflictProgress")]
    pub power_conflict_progress: Option<serde_json::Value>,
    #[serde(default, rename = "thargoidWar")]
    pub thargoid_war: Option<serde_json::Value>,
}

// --- API query parameter structs ---

#[derive(Deserialize)]
pub struct CubeSearchQuery {
    pub ref_system: Option<String>,
    pub center: Option<String>,
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub z: Option<f64>,
    pub size: Option<f64>,
    #[serde(rename = "bodyType", alias = "body_type")]
    pub body_type: Option<String>,
    #[serde(rename = "customFilter", alias = "custom_filter")]
    pub custom_filter: Option<String>,
}

#[derive(Deserialize)]
pub struct SystemQuery {
    #[serde(rename = "systemName", alias = "name")]
    pub system_name: Option<String>,
    pub id64: Option<i64>,
}

#[derive(Deserialize)]
pub struct RouteQuery {
    pub source: String,
    pub destination: String,
}

#[derive(Deserialize)]
pub struct NearestStationQuery {
    #[serde(rename = "refSystem", alias = "ref_system", alias = "system")]
    pub ref_system: String,
    pub radius: Option<f64>,
    pub limit: Option<usize>,
    pub allegiance: Option<String>,
    pub government: Option<String>,
    pub economy: Option<String>,
    #[serde(rename = "stationType", alias = "station_type")]
    pub station_type: Option<String>,
    #[serde(rename = "minLandingPad", alias = "min_landing_pad")]
    pub min_landing_pad: Option<String>,
    #[serde(rename = "maxStationDistance", alias = "max_station_distance")]
    pub max_station_distance: Option<f64>,
    #[serde(rename = "useSurfaceStations", alias = "use_surface_stations", default)]
    pub use_surface_stations: bool,
    #[serde(rename = "ignoreFleetCarriers", alias = "ignore_fleet_carriers")]
    pub ignore_fleet_carriers: Option<bool>,
}

#[derive(Deserialize)]
pub struct CarrierRouteQuery {
    pub current_system: String,
    pub destination: String,
    #[serde(alias = "cargo_capacity")]
    pub used_cargo: f64,
    #[serde(alias = "current_fuel")]
    pub tank_fuel: f64,
    #[serde(alias = "market_tritium")]
    pub stored_tritium: f64,
    pub is_squadron: Option<bool>,
    pub engine: Option<String>,
}

#[derive(Deserialize)]
pub struct NeutronRouteQuery {
    pub source: String,
    pub destination: String,
    pub range: f64,
    pub supercharge_type: String,
    pub engine: Option<String>,
}

// --- A* node for ship routing ---

#[derive(Clone)]
pub struct RouteNode {
    pub g_score: usize,
    pub f_score: f64,
    pub id64: i64,
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl PartialEq for RouteNode {
    fn eq(&self, other: &Self) -> bool { self.id64 == other.id64 }
}
impl Eq for RouteNode {}
impl PartialOrd for RouteNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for RouteNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.f_score.partial_cmp(&self.f_score).unwrap_or(std::cmp::Ordering::Equal)
    }
}
