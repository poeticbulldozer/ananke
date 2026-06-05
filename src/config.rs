// --- CONFIGURATION ---
pub const DB_FILE: &str = "edsm_cube.db";
pub const PORT: u16 = 8000;
pub const URL_SYSTEMS_1DAY: &str = "https://downloads.spansh.co.uk/galaxy_1day.json.gz";
pub const FILE_SYSTEMS_1DAY: &str = "galaxy_1day.json.gz";
pub const FILE_SYSTEMS_DOWNLOADING: &str = "galaxy_1day.json.gz.downloading";
pub const SYNC_INTERVAL_SECONDS: u64 = 21600; // 6 hours
pub const MAX_CONCURRENT_QUERIES: usize = 6;
pub const MAX_CONCURRENT_ASTAR: usize = 2;
pub const SHIP_ROUTE_BUDGET_MS: u128 = 120_000; // 2 minutes
pub const EDMC_KEY_ENV: &str = "ANANKE_EDMC_KEY";

// --- EDDN ---
pub const EDDN_RELAY_URL: &str = "tcp://eddn.edcd.io:9500";
pub const EDDN_RELAY_ENV: &str = "ANANKE_EDDN_RELAY";
pub const EDDN_DISABLE_ENV: &str = "ANANKE_EDDN_DISABLE";
pub const EDDN_RECV_TIMEOUT_MS: i32 = 60_000;
pub const EDDN_RECONNECT_BASE_MS: u64 = 1_000;
pub const EDDN_RECONNECT_MAX_MS: u64 = 60_000;
pub const EDDN_FLUSH_INTERVAL_MS: u64 = 1_000;
pub const EDDN_FLUSH_BATCH_SIZE: usize = 200;

// --- Commander hotspot heatmap ---
pub const HEATMAP_X_MIN: f64 = -50_000.0;
pub const HEATMAP_X_MAX: f64 =  50_000.0;
pub const HEATMAP_Z_MIN: f64 = -25_000.0;
pub const HEATMAP_Z_MAX: f64 =  75_000.0;
pub const HEATMAP_W: usize = 1024;
pub const HEATMAP_H: usize = 1024;
pub const HEATMAP_DECAY_INTERVAL_SECS: u64 = 300;
pub const HEATMAP_DECAY_FACTOR: f64 = 0.9928057; // ≈8 hour half-life
pub const HEATMAP_RENDER_CACHE_SECS: u64 = 30;

// --- Routing ---
pub const CARRIER_REFINE_BUDGET_MS: u128 = 1_800_000; // 30 minutes
pub const CARRIER_JUMP_RANGE: f64 = 500.0;
pub const NEUTRON_REFINE_BUDGET_MS: u128 = 1_800_000; // 30 minutes
pub const NEUTRON_SEG_LY: f64 = 2000.0;
