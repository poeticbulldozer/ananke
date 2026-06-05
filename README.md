# Ananke

Ananke is a high-performance, concurrent Rust-based backend API for the Galtea project (an Elite Dangerous tool suite). It is designed to handle extremely fast 3D geospatial searching, advanced routing, real-time activity heatmaps, and live data ingestion using a local SQLite database and optional Vulkan-accelerated GPU pathfinding.

---

## Features

- **Blazing Fast API**: Built with `axum` and `tokio` for handling a massive number of concurrent requests.
- **Advanced A* Pathfinding**: Supports Fleet Carrier, Neutron Star, and standard Ship routing. Features a **Vulkan-accelerated A*** implementation (`vulkano`) for massive performance gains on complex routing, with a seamless CPU fallback.
- **Live Data Ingestion**:
  - **EDDN Listener**: Automatically connects to the Elite Dangerous Data Network (EDDN) via ZeroMQ to ingest real-time universe state changes.
  - **EDMC Ingest**: Provides authenticated endpoints for custom Elite Dangerous Market Connector (EDMC) plugins to push journal updates and batch data.
- **Spatial "Cube" Searching**: High-performance spatial querying using a 3D R*Tree index to find systems, bodies (like specific star types, terraformable worlds, or ringed planets), and nearest stations within a custom cubic sector.
- **Automated Data Syncing**: A background sync manager downloads Spansh galaxy 1-day system dumps (`galaxy_1day.json.gz`) every 6 hours and processes them automatically, backfilling the database.
- **Real-time Heatmap**: In-memory decay heatmap of galaxy activity generated dynamically as a PNG (`/api/heatmap.png`) or rendered as HTML (`/heatmap`).
- **Fleet Carrier Tracking**: Progression endpoints tracking specific carrier movements (`/api/galtea-progression`).

---

## Database Schema Overview

Ananke stores galaxy information in a local SQLite database (`edsm_cube.db`) configured with high-performance PRAGMAs (`journal_mode = WAL`, `temp_store = MEMORY`, `synchronous = NORMAL`). The schema consists of:

- **`systems`**: Primary star system records including coordinate offsets, populations, allegiances, governments, controlling factions, and power states.
- **`systems_index`**: A 3D spatial index utilizing SQLite's `rtree` extension to speed up coordinate bounding box lookups.
- **`bodies`**: Celestial bodies, stars, and planets, detailing landability, gravity, orbital metrics, atmospheric composition, rings, discovery state, and surface/biological signals.
- **`stations`**: In-system stations, outposts, and fleet carriers, including services, landing pad sizes, market items, shipyards, outfitting options, and docking accessibility.
- **`neutron_systems`**: An indexed sub-table tracking system IDs that contain a Neutron Star to optimize route plotting.
- **`prison_systems`**: An indexed sub-table tracking system IDs containing prison facilities/megaships.
- **`meta`**: Simple key-value store for sync history and database state tracking.

---

## Configuration & Setup

### Prerequisites
- **Rust (Edition 2021)**: Install via `rustup`.
- **SQLite**: Runtime library compatible with SQLite3.
- **ZeroMQ**: Development headers required for compiling `zmq` bindings.
- **Vulkan** *(Optional)*: Vulkan loader and compatible drivers are needed on the host to enable GPU-accelerated pathfinding. If absent, Ananke falls back to CPU A* routing seamlessly.

### Environment Variables

| Variable Name | Description | Default Value |
| :--- | :--- | :--- |
| `ANANKE_EDMC_KEY` | If set, requires this API key as authentication on EDMC ingest endpoints. If not set, endpoints are open. | (Disabled / Open) |
| `ANANKE_EDDN_DISABLE` | Set to any non-empty value (e.g. `1` or `true`) to completely disable the live EDDN ZeroMQ listener thread. | (Enabled) |
| `ANANKE_EDDN_RELAY` | Overrides the default ZeroMQ relay URL to listen to EDDN messages. | `tcp://eddn.edcd.io:9500` |

### Build & Run

```bash
# Build the project (Release mode is highly recommended due to pathfinding/search overhead)
cargo build --release

# Run the server (Defaults to port 8000)
cargo run --release
```

---

## API Endpoints Reference

### 1. System Information
- `GET /api/system` - Get detailed metadata for a specific system.
  - **Query Parameters**:
    - `systemName` / `name` *(string, optional)*: Case-insensitive system name.
    - `id64` *(integer, optional)*: Elite Dangerous unique 64-bit system ID.
- `GET /api/system/bodies` or `GET /api/bodies` - List all bodies in a system.
  - **Query Parameters**: Same as `/api/system`.
- `GET /api/system/stations` or `GET /api/stations` - List all stations and fleet carriers in a system.
  - **Query Parameters**: Same as `/api/system`.

### 2. Search Endpoints
- `GET /api/nearest-station` - Find the closest station to a reference coordinates or system.
  - **Query Parameters**:
    - `refSystem` / `ref_system` / `system` *(string, required)*: Reference system name.
    - `radius` *(float, optional)*: Maximum search distance in light-years.
    - `limit` *(integer, optional)*: Maximum number of stations to return.
    - `allegiance` / `government` / `economy` *(string, optional)*: Filter by faction attributes.
    - `stationType` / `station_type` *(string, optional)*: Filter by type (e.g., Coriolis, Outpost).
    - `minLandingPad` / `min_landing_pad` *(string, optional)*: Minimum landing pad size required (`S`, `M`, or `L`).
    - `maxStationDistance` / `max_station_distance` *(float, optional)*: Maximum distance of the station from the arrival star in light-seconds (ls).
    - `useSurfaceStations` / `use_surface_stations` *(boolean, default: false)*: Whether to include planetary surface stations.
    - `ignoreFleetCarriers` / `ignore_fleet_carriers` *(boolean, optional)*: Whether to ignore fleet carriers.

- `GET` / `POST /api/cube-search` - Find systems or bodies within a 3D spatial cube constraint.
  - **JSON/Query Parameters**:
    - `ref_system` / `center` *(string, optional)*: Use coordinates of this system as the cube's center.
    - `x` / `y` / `z` *(float, optional)*: Explicit coordinates of the cube's center.
    - `size` *(float, optional, default: 20.0)*: Dimensions of the search cube in light-years (max: 500.0).
    - `bodyType` / `body_type` / `customFilter` / `custom_filter` *(string, optional)*: Search filters.
      - *Supported Filters*: `earth-like`, `water world`, `ammonia world`, `neutron star`, `black hole`, `white dwarf`, `rocky body`, `high metal content`, `terraformable`, `bio` / `biological` (signals), `geo` / `geological` (signals), `icy ring`, `metallic ring`/`metal ring`, `rocky ring`, or generic `rings`.

### 3. Routing
- `GET` / `POST /api/route` - Plot a standard ship route (jump limit: 14.99 ly).
  - **JSON/Query Parameters**:
    - `source` *(string, required)*: Starting system name or ID.
    - `destination` *(string, required)*: Destination system name or ID.
- `POST /api/carrier-route` - Plot a Fleet Carrier route (500 ly max jump range) with detailed fuel/tritium calculations.
  - **JSON Parameters**:
    - `current_system` *(string, required)*: Start system name or ID.
    - `destination` *(string, required)*: Destination system name or ID.
    - `used_cargo` / `cargo_capacity` *(float, required)*: Current cargo weight in tons.
    - `tank_fuel` / `current_fuel` *(float, required)*: Tritium currently in the carrier's fuel tank (tons).
    - `stored_tritium` / `market_tritium` *(float, required)*: Tritium stored in the carrier's cargo bay/market (tons).
    - `is_squadron` *(boolean, optional)*: If `true`, uses squadron carrier weight parameters (60,000t instead of 25,000t base mass).
    - `engine` *(string, optional)*: Pathfinding engine selection (`greedy` or `astar`).
- `POST /api/neutron-route` - Plot a neutron-supercharged route.
  - **JSON Parameters**:
    - `source` *(string, required)*: Starting system.
    - `destination` *(string, required)*: Destination system.
    - `range` *(float, required)*: The ship's base jump range in light-years.
    - `supercharge_type` *(string, required)*: Supercharge multiplier type (e.g. `caspian` for 6x boost, or others for standard 4x boost).
    - `engine` *(string, optional)*: Pathfinding engine selection (`greedy` or `astar`).

### 4. Data Ingestion & Heatmaps
- `POST /api/edmc/journal` & `POST /api/edmc/batch` - Custom EDMC plugin endpoints to ingest player journal events. Requires matching key in `ANANKE_EDMC_KEY` if configured.
- `GET /api/edmc/stats` - Retrieve stats on ingested systems, bodies, and stations.
- `GET /api/heatmap.png` - Renders a dynamic activity PNG map (1024x1024) with spatial coordinates, applying an in-memory decay model.
- `GET /heatmap` - HTML interface for visualizing the activity heatmap.
- `GET /api/galtea-progression` - Retrieve logged Fleet Carrier movement progression coordinates.

---

## Architecture

Ananke utilizes a highly concurrent, thread-safe architecture:
1. **Async Web Server**: Powered by `axum` routing request endpoints on tokio runtime threads.
2. **Database Access & Throttling**: Managed through an `r2d2` pool of SQLite connections. Database queries are regulated using `tokio::sync::Semaphore` to prevent SQLite connection exhaustion and database locks.
3. **Non-Blocking Write Worker**: Live data from the EDMC endpoints and the ZeroMQ EDDN listener thread is sent via `crossbeam-channel` queues to a single dedicated database writer thread. This isolates writes, preventing SQLite database locks from blocking the main web server.
4. **Vulkan A* Pathfinding**: Initializes the Vulkan instance and compiles pathfinding compute shaders once at startup. When route requests come in, Vulkan buffers are built and executed on the GPU, returning the optimal node path. Seamless CPU A* fallbacks are invoked if initialization fails or compute resources are busy.

