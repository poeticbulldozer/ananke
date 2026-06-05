#![recursion_limit = "256"]

mod config;
mod db;
mod eddn;
mod heatmap;
mod handlers;
mod models;
mod state;
mod sync;
mod vulkan_astar;

use crossbeam_channel::bounded;
use std::sync::{Arc, atomic::AtomicU64};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use axum::{routing::{get, post}, Router};

use config::*;
use db::{setup_db_pool, init_db, db_writer_worker};
use eddn::eddn_listener_thread;
use heatmap::{Heatmap, heatmap_decay_thread, heatmap_png_handler, heatmap_html_handler};
use state::{AppState, CarrierCache, EdmcStats, EddnStats};
use vulkan_astar::VulkanAstar;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    info!("Starting Ananke on port {}...", PORT);

    let pool = setup_db_pool();
    let conn = pool.get().unwrap();
    init_db(&conn).unwrap();
    drop(conn);

    tokio::spawn(async { sync::sync_manager().await; });

    // EDMC live-ingest writer
    let (edmc_sender, edmc_receiver) = bounded::<Vec<models::SpanshSystem>>(100);
    let _edmc_writer = std::thread::spawn(move || db_writer_worker(edmc_receiver));

    // EDMC API key
    let edmc_api_key = std::env::var(EDMC_KEY_ENV).ok().filter(|k| !k.is_empty());
    if edmc_api_key.is_some() {
        info!("EDMC API key authentication enabled.");
    } else {
        warn!("EDMC API key not set ({}). Ingest endpoints are OPEN — set the env var to require auth.", EDMC_KEY_ENV);
    }

    // EDDN stats
    let eddn_stats = Arc::new(EddnStats {
        messages_received:  AtomicU64::new(0),
        messages_processed: AtomicU64::new(0),
        messages_dropped:   AtomicU64::new(0),
        systems_emitted:    AtomicU64::new(0),
        bodies_emitted:     AtomicU64::new(0),
        stations_emitted:   AtomicU64::new(0),
        last_message_time:  AtomicU64::new(0),
        reconnects:         AtomicU64::new(0),
        connected:          AtomicU64::new(0),
    });

    // Heatmap
    let heatmap = Arc::new(Heatmap::new());
    {
        let heatmap_for_decay = heatmap.clone();
        let _decay = std::thread::Builder::new()
            .name("heatmap-decay".into())
            .spawn(move || heatmap_decay_thread(heatmap_for_decay))
            .expect("failed to spawn heatmap decay thread");
        info!("Heatmap decay thread started.");
    }

    // EDDN listener
    let eddn_disabled = std::env::var(EDDN_DISABLE_ENV).ok()
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if eddn_disabled {
        warn!("EDDN listener DISABLED via {}.", EDDN_DISABLE_ENV);
    } else {
        let relay_url = std::env::var(EDDN_RELAY_ENV)
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| EDDN_RELAY_URL.to_string());
        let eddn_sender = edmc_sender.clone();
        let eddn_stats_for_thread = eddn_stats.clone();
        let eddn_heatmap = heatmap.clone();
        let _eddn_listener = std::thread::Builder::new()
            .name("eddn-listener".into())
            .spawn(move || eddn_listener_thread(relay_url, eddn_sender, eddn_stats_for_thread, eddn_heatmap))
            .expect("failed to spawn EDDN listener thread");
        info!("EDDN listener thread started.");
    }

    // Vulkan A* — init once, shared across all route requests via AppState
    let vulkan_astar = VulkanAstar::init();
    if vulkan_astar.is_none() {
        warn!("VulkanAstar init failed — carrier/neutron routes will use CPU A*.");
    }

    let app_state = Arc::new(AppState {
        db_pool: pool,
        query_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_QUERIES)),
        astar_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_ASTAR)),
        carrier_cache: Mutex::new(CarrierCache { data: None, expires_at: 0 }),
        edmc_sender,
        edmc_api_key,
        edmc_stats: Arc::new(EdmcStats {
            systems_ingested: AtomicU64::new(0),
            bodies_ingested: AtomicU64::new(0),
            stations_ingested: AtomicU64::new(0),
            last_ingest_time: AtomicU64::new(0),
        }),
        eddn_stats,
        heatmap,
        vulkan_astar,
    });

    let cors = CorsLayer::permissive();

    let app = Router::new()
        // System data
        .route("/api/system", get(handlers::system::get_system))
        .route("/api/system/bodies", get(handlers::system::get_system_bodies))
        .route("/api/bodies", get(handlers::system::get_system_bodies))
        .route("/api/system/stations", get(handlers::system::get_system_stations))
        .route("/api/stations", get(handlers::system::get_system_stations))
        // Station search
        .route("/api/nearest-station", get(handlers::station::nearest_station))
        // Cube search
        .route("/api/cube-search", get(handlers::search::cube_search_get).post(handlers::search::cube_search_post))
        // Routing
        .route("/api/route", get(handlers::ship_route::ship_route_get).post(handlers::ship_route::ship_route_post))
        .route("/api/carrier-route", post(handlers::carrier_route::carrier_route_post))
        .route("/api/neutron-route", post(handlers::neutron_route::neutron_route_post))
        // Progression
        .route("/api/galtea-progression", get(handlers::progression::get_carrier_progression))
        // EDMC ingest
        .route("/api/edmc/journal", post(handlers::edmc::edmc_journal))
        .route("/api/edmc/batch", post(handlers::edmc::edmc_batch))
        .route("/api/edmc/stats", get(handlers::edmc::edmc_stats))
        // Heatmap
        .route("/api/heatmap.png", get(heatmap_png_handler))
        .route("/heatmap", get(heatmap_html_handler))
        .layer(cors)
        .with_state(app_state);

    let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).await.unwrap();
    info!("Server listening on port {}", PORT);
    axum::serve(listener, app).await.unwrap();
}