# Ananke

Ananke is the high-performance, Rust-based backend API for the Galtea project (an Elite Dangerous tool suite). It provides extremely fast geospatial querying, routing, and live data ingestion using a local SQLite database (`edsm_cube.db`) and optional GPU-accelerated pathfinding.

## Features

- **Blazing Fast API**: Built with `axum` and `tokio` for handling a massive number of concurrent requests.
- **Advanced A* Pathfinding**: Supports Fleet Carrier, Neutron Star, and standard Ship routing. Features a **Vulkan-accelerated A*** implementation (`vulkano`) for massive performance gains on complex routing, with a seamless CPU fallback.
- **Live Data Ingestion**:
  - **EDDN Listener**: Automatically connects to the Elite Dangerous Data Network (EDDN) via ZeroMQ to ingest real-time universe state changes.
  - **EDMC Ingest**: Provides authenticated endpoints for custom Elite Dangerous Market Connector (EDMC) plugins to push journal updates and batch data.
- **Spatial "Cube" Searching**: High-performance spatial querying to find systems, bodies (like specific star types or ringed planets), and nearest stations within a custom cubic sector.
- **Real-time Heatmap**: In-memory decay heatmap of galaxy activity generated dynamically as a PNG (`/api/heatmap.png`).
- **Fleet Carrier Tracking**: Progression endpoints tracking specific carrier movements (`/api/galtea-progression`).

## API Endpoints Overview

- **System Data**:
  - `GET /api/system` - Get data for a specific system.
  - `GET /api/system/bodies` or `GET /api/bodies` - Get all bodies in a system.
  - `GET /api/system/stations` or `GET /api/stations` - Get all stations in a system.
- **Search**:
  - `GET /api/nearest-station` - Find the closest station to a given coordinate.
  - `GET / POST /api/cube-search` - Advanced search for systems/bodies within a spatial cube constraint.
- **Routing**:
  - `GET / POST /api/route` - Standard ship routing.
  - `POST /api/carrier-route` - Fleet Carrier routing (Vulkan A* supported).
  - `POST /api/neutron-route` - Neutron plotter (Vulkan A* supported).
- **Ingest & Stats**:
  - `POST /api/edmc/journal` & `POST /api/edmc/batch` - EDMC data ingest.
  - `GET /api/edmc/stats` - Current ingest metrics.

## Setup & Running

### Prerequisites
- **Rust (edition 2021)**: Install via `rustup`.
- **Database**: Ananke requires the `edsm_cube.db` SQLite database in the root of the project directory. *(Note: This file is large and is excluded from git via `.gitignore`)*.
- **Vulkan** *(Optional)*: Vulkan drivers for GPU-accelerated A* pathfinding.

### Environment Variables
- `EDMC_KEY_ENV`: Set an API key to require authentication on EDMC ingest endpoints. If not set, endpoints are open.
- `EDDN_DISABLE_ENV`: Set to any non-empty value to disable the live EDDN listener.
- `EDDN_RELAY_ENV`: Override the default EDDN ZeroMQ relay URL.

### Build & Run
```bash
# Build the project (Release mode is highly recommended due to pathfinding overhead)
cargo build --release

# Run the server (Defaults to port 8000 or the port specified in config)
cargo run --release
```

## Architecture

Ananke uses a highly concurrent architecture:
- `axum` routes requests asynchronously.
- Database access is pooled via `r2d2_sqlite` and requests are throttled using `tokio::sync::Semaphore` to prevent SQLite locks.
- A dedicated background thread writes ingested EDMC/EDDN data to the database using `crossbeam-channel` to prevent blocking the web server.
- The `vulkan_astar` module initializes the Vulkan device once at startup and shares it across all incoming route requests.
