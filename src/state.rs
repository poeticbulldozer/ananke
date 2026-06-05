use crossbeam_channel::Sender;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::sync::{Arc, atomic::AtomicU64};
use tokio::sync::{Mutex, Semaphore};

use crate::heatmap::Heatmap;
use crate::models::SpanshSystem;
use crate::vulkan_astar::VulkanAstar;

#[allow(dead_code)]
pub struct AppState {
    pub db_pool: Pool<SqliteConnectionManager>,
    pub query_semaphore: Arc<Semaphore>,
    pub astar_semaphore: Arc<Semaphore>,
    pub carrier_cache: Mutex<CarrierCache>,
    pub edmc_sender: Sender<Vec<SpanshSystem>>,
    pub edmc_api_key: Option<String>,
    pub edmc_stats: Arc<EdmcStats>,
    pub eddn_stats: Arc<EddnStats>,
    pub heatmap: Arc<Heatmap>,
    pub vulkan_astar: Option<Arc<VulkanAstar>>,
}

#[allow(dead_code)]
pub struct CarrierCache {
    pub data: Option<serde_json::Value>,
    pub expires_at: u64,
}

/// Live ingest counters for EDMC
pub struct EdmcStats {
    pub systems_ingested: AtomicU64,
    pub bodies_ingested: AtomicU64,
    pub stations_ingested: AtomicU64,
    pub last_ingest_time: AtomicU64,
}

/// Live ingest counters for EDDN
pub struct EddnStats {
    pub messages_received: AtomicU64,
    pub messages_processed: AtomicU64,
    pub messages_dropped: AtomicU64,
    pub systems_emitted: AtomicU64,
    pub bodies_emitted: AtomicU64,
    pub stations_emitted: AtomicU64,
    pub last_message_time: AtomicU64,
    pub reconnects: AtomicU64,
    pub connected: AtomicU64,
}