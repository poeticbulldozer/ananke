# Ananke — A* Routing: Full Context for Vulkan Reimplementation

## Project Overview

**Ananke** is a Rust/Axum HTTP server for Elite Dangerous galaxy data. It sits on a Raspberry Pi 5
with a SQLite database (`edsm_cube.db`) containing ~70M+ systems sourced from the Spansh daily dump
and live EDDN/EDMC ingest.

The current CPU A* is production and correct. The goal is to port the inner A* graph search to
Vulkan compute shaders while keeping the Rust host code, DB layer, and HTTP API identical.

---

## Database Schema (relevant tables only)

```sql
-- Primary system record
systems (
    id64 INTEGER PRIMARY KEY,
    name TEXT,
    population INTEGER,
    last_update INTEGER,
    allegiance TEXT, government TEXT, primaryEconomy TEXT, secondaryEconomy TEXT,
    security TEXT, bodyCount INTEGER, date TEXT,
    controllingFaction TEXT,  -- JSON
    factions TEXT,            -- JSON
    powerState TEXT, powers TEXT, controllingPower TEXT,
    powerStateControlProgress REAL, powerStateReinforcement REAL,
    powerStateUndermining REAL, powerConflictProgress TEXT, thargoidWar TEXT
)

-- Spatial R-tree index — coords stored here, NOT in systems
-- minX=maxX=x, minY=maxY=y, minZ=maxZ=z (point entries)
systems_index USING rtree(id, minX, maxX, minY, maxY, minZ, maxZ)

-- Pre-filtered neutron star lookup set (~30k rows)
neutron_systems (systemId64 INTEGER PRIMARY KEY)
```

Coordinate lookup pattern used everywhere:
```sql
SELECT s.id64, s.name, i.minX, i.minY, i.minZ
FROM systems s JOIN systems_index i ON s.id64 = i.id
WHERE s.name = ? COLLATE NOCASE LIMIT 1
-- or WHERE s.id64 = ?
```

Bbox/range query pattern:
```sql
SELECT id, minX, minY, minZ
FROM systems_index
WHERE minX >= ? AND maxX <= ?
  AND minY >= ? AND maxY <= ?
  AND minZ >= ? AND maxZ <= ?
```

---

## Concurrency Model

```rust
const MAX_CONCURRENT_QUERIES: usize = 6;
const MAX_CONCURRENT_ASTAR: usize = 2;  // protects Pi

// AppState holds:
query_semaphore: Arc<Semaphore>    // all DB endpoints
astar_semaphore: Arc<Semaphore>    // ship + carrier + neutron routes only
```

All three routing functions acquire `astar_semaphore` and run
`tokio::task::spawn_blocking` for the synchronous compute work.

---

## Router 1: Ship A* (`GET|POST /api/route`)

### Purpose
Fixed 14.99 LY jump range. Every system in the DB is a valid node. Returns minimum-jump path.

### Parameters
```rust
struct RouteQuery {
    source: String,      // system name or id64
    destination: String,
}
```

### Budget
```rust
const SHIP_ROUTE_BUDGET_MS: u128 = 120_000; // 2 minutes
let mut max_iterations = 2_000_000;
```

### Node struct
```rust
struct RouteNode {
    g_score: usize,   // jumps from start
    f_score: f64,     // g + h, used for BinaryHeap ordering (min-heap via reversed cmp)
    id64: i64,
    x: f64, y: f64, z: f64,
}
// Ordering: other.f_score.partial_cmp(&self.f_score)  →  min-heap on f
```

### Heuristic
```rust
h(n) = euclidean_dist(n, dst) / 14.99
// admissible: never overestimates real jump count
```

### Neighbour expansion
One prepared statement reused across all expansions:
```sql
SELECT id, minX, minY, minZ
FROM systems_index
WHERE minX >= ? AND maxX <= ?
  AND minY >= ? AND maxY <= ?
  AND minZ >= ? AND maxZ <= ?
```
Bbox = `[current.x ± 14.99, current.y ± 14.99, current.z ± 14.99]`.
Then filter: `dist(current, neighbour) <= 14.99` (Euclidean, not bbox).

### State
- `open_set: BinaryHeap<RouteNode>` — standard Rust binary heap
- `g_score_map: HashMap<i64, usize>` — best g seen per node
- `came_from: HashMap<i64, i64>` — parent tracking for reconstruction

### Termination
- `current.id64 == dst_id` → reconstruct and return
- `max_iterations == 0 || elapsed > 120s` → return error

### Output JSON
```json
{
  "source": "Sol",
  "destination": "Colonia",
  "totalJumps": 1234,
  "route": [
    {"system": "Sol", "id64": "10477373803", "coords": {"x":0,"y":0,"z":0},
     "distance_from_prev": 0.0, "jumps": 0},
    ...
  ]
}
```

---

## Router 2: Carrier Route (`POST /api/carrier-route`)

### Purpose
Fleet carrier, always 500 LY jump range. Greedy baseline + optional A* refinement.
Fuel simulation runs after path is chosen (not during search).

### Parameters
```rust
struct CarrierRouteQuery {
    current_system: String,
    destination: String,
    used_cargo: f64,          // alias: cargo_capacity
    tank_fuel: f64,           // alias: current_fuel
    stored_tritium: f64,      // alias: market_tritium
    max_tank_capacity: Option<f64>,   // default 1000 personal / 25000 squadron
    is_squadron: Option<bool>,
    engine: Option<String>,   // "greedy" (default) | "astar"
}
```

### Constants
```rust
const CARRIER_JUMP_RANGE: f64 = 500.0;
const CARRIER_REFINE_BUDGET_MS: u128 = 1_800_000; // 30 minutes
let carrier_base_mass = if is_squadron { 60000.0 } else { 25000.0 };
```

### Phase 1: Greedy baseline
For up to 500 hops:
1. Project unit vector from current → destination
2. Target point = current + unit * 498.5
3. Search DB in expanding bbox (start 20 LY, expand by 30 LY up to 5 iterations) around target
4. Among all candidates within 500 LY of current, pick the one closest to destination
5. Repeat until `dist_remaining <= 500`
6. Append destination as final hop

### Phase 2: A* refinement (engine == "astar")

**Corridor preload:**
```rust
let corridor_half = 1500.0f64; // ≈ 3× jump range
// Load all systems in bbox [min(x1,x2)±buf .. max(x1,x2)±buf] per axis
// Then filter to corridor: perpendicular distance ≤ corridor_half
// Projection: t = dot(w, dv) / |dv|²  clamped [0,1]
//             perp² = |w - t*dv|²  ≤ corridor_sq
```

**Spatial grid:**
```rust
let cell_size = (CARRIER_JUMP_RANGE * 0.9).max(50.0); // ~450 LY
// cell = (x/cell_size as i32, y/cell_size as i32, z/cell_size as i32)
// Neighbour search: all cells in ±2 cube around current cell (5³=125 cells)
```

**A* node:**
```rust
struct CNode { g: u32, f: f64, id: i64 }
// min-heap: other.f.partial_cmp(&self.f)
```

**Heuristic:**
```rust
h_fwd(n) = ceil(dist(n, dst) / CARRIER_JUMP_RANGE)
h_bwd(n) = ceil(dist(n, src) / CARRIER_JUMP_RANGE)
```

**Search strategy:**
- `total_distance <= 5000 LY` → **unidirectional A***
  - Bounded by `g < greedy_jumps` (prune any path longer than greedy)
  - Direct dst-reach check: if `dist(current, dst) <= 500` → push dst node with `g+1`
- `total_distance > 5000 LY` → **bidirectional A***
  - `mu = greedy_jumps` (upper bound, tightened as better meeting nodes found)
  - Expand the frontier with smaller `min_f` each iteration
  - Terminate when `fwd_open.min_f + bwd_open.min_f >= mu`
  - Path reconstruction: fwd chain from src→meeting, bwd chain from meeting→dst

**Final path selection:**
```rust
let final_path = if use_astar { astar_path.unwrap_or(greedy_path) } else { greedy_path };
let is_optimal = final_path.len() - 1 < greedy_jumps;
```

### Fuel simulation (runs on chosen path)
```rust
let jump_fuel = (5.0 + (jump_dist * (base_cargo + market_tritium + carrier_base_mass)) / 200_000.0).ceil();
tank -= jump_fuel;
let has_enough_fuel = tank >= 0.0;
// Top off from market after each jump:
let top_off = (max_tank_capacity - tank).max(0.0).min(market);
tank += top_off; market -= top_off;
```

### Output JSON
```json
{
  "source": "...", "destination": "...",
  "is_squadron": false, "optimised": true,
  "total_distance_ly": 12345.67,
  "base_cargo_capacity_used": 500.0,
  "initial_fuel_tank": 800.0, "initial_market_tritium": 5000.0,
  "total_fuel_used": 3200.0, "final_fuel_tank": 600.0, "final_market_tritium": 1000.0,
  "totalJumps": 25,
  "route": [
    {"system": "...", "id64": "...",
     "distance_from_start": 0.0, "distance_to_destination": 12345.67,
     "jump_distance": 0.0, "fuel_used": 0.0,
     "fuel_left_tank": 800.0, "tritium_in_market": 5000.0, "has_enough_fuel": true},
    ...
  ]
}
```

---

## Router 3: Neutron Route (`POST /api/neutron-route`)

### Purpose
Neutron highway routing. Graph nodes = neutron stars only (+ src/dst normal stars).
Normal-star fallback hops used when out of boosted range.

### Parameters
```rust
struct NeutronRouteQuery {
    source: String,
    destination: String,
    range: f64,              // ship base jump range in LY
    supercharge_type: String, // "neutron" (4×) | "caspian" (6×)
    engine: Option<String>,  // "greedy" | "astar" (default: astar)
}
```

### Constants
```rust
const REFINE_BUDGET_MS: u128 = 1_800_000; // 30 minutes
const SEG_LY: f64 = 2000.0;              // segment length for waypoints
```

### Boost multiplier
```rust
let multiplier = if supercharge_type == "caspian" { 6.0 } else { 4.0 };
let boosted_range = params.range * multiplier;
```

### Phase 1: Waypoint generation
For routes > SEG_LY, split into ~2000 LY segments:
```rust
let num_segs = (total_distance / SEG_LY).ceil() as usize;
// For i in 1..num_segs: find nearest neutron to point at i*SEG_LY along straight line
// Final waypoint = actual destination
```

Nearest neutron finder: expanding bbox rings [300, 800, 1500, 3000 LY], ORDER BY sq-dist, LIMIT 1.
Deduplicates consecutive identical waypoints.

### Phase 2: Greedy through waypoints
Per-segment state:
- Load neutrons in corridor around segment (corridor_half = max(boosted_range×3, 1500 LY))
- Build per-segment spatial grid (cell_size = max(boosted_range×0.9, 50))
- Anti-loop: `RECENT_WINDOW = 8` (VecDeque, not full visited set — prevents permanent stuck)

Per-hop logic:
1. Check if waypoint reachable directly (`dist ≤ jump_range`) → done
2. Find best neutron within `jump_range` not in recent window → jump to it
3. Fallback: DB query for normal stars in expanding radii [jump_range, boosted_range, boosted_range×1.5]
   → pick closest to waypoint
4. If nothing found: break segment, continue to next waypoint

Node type tracking: `all_node_is_neutron: HashMap<i64, bool>` — affects jump range at each hop.

### Phase 3: Full corridor A* refinement (engine != "greedy")

**Preload:**
```rust
let corridor_half = (boosted_range * 6.0).max(3000.0);
// Load ALL neutrons in bbox+corridor from src→dst (full route, not per-segment)
// ~100k neutrons for a 22k LY route — ~80MB
```

**A* grid:** cell_size = max(boosted_range×0.9, 50), neutrons only.

**Heuristic:**
```rust
h_fwd(n) = ceil(dist(n, dst) / boosted_range)
h_bwd(n) = ceil(dist(n, src) / boosted_range)
```

**Search strategy:**
- `total_distance <= 10_000 LY` → unidirectional A*
- `total_distance > 10_000 LY` → bidirectional A*

**Bidirectional seed trick:** Forward search seeds from the first neutron in greedy path,
backward seeds from the last neutron. This skips non-neutron bridge hops at src/dst ends
(those hops stay from greedy). Bidirectional structure mirrors the carrier case.

**Path stitching (neutron only):**
- If A* result starts ≠ src_id: prepend src_id + greedy prefix up to first A* node
- If A* result ends ≠ dst_id: append greedy suffix from last A* node + dst_id
- This handles the normal-star bridge hops at route ends that A* (neutron-only graph) can't see

### Output JSON
```json
{
  "source": "...", "destination": "...",
  "total_distance_ly": 22000.0,
  "totalJumps": 183, "optimised": true,
  "route": [
    {"system": "...", "id64": "...",
     "distance_from_start": 0.0, "distance_to_destination": 22000.0,
     "jump_distance": 0.0,
     "used_neutron_boost": false, "is_neutron": false},
    ...
  ]
}
```

---

## Shared Spatial Grid Pattern (Carrier + Neutron A*)

All in-memory neighbour lookups use the same pattern:
```rust
// Build:
let cell_size = (jump_range * 0.9).max(50.0);
let cell = (x/cell_size as i32, y/cell_size as i32, z/cell_size as i32);
grid.entry(cell).or_default().push(node_id);

// Query (expand current cell ±2 in each axis = 5³ = 125 cells):
let (bx, by, bz) = (x/cell_size as i32, y/cell_size as i32, z/cell_size as i32);
for dx in -2i32..=2 { for dy in -2i32..=2 { for dz in -2i32..=2 {
    if let Some(v) = grid.get(&(bx+dx, by+dy, bz+dz)) {
        for &n_id in v {
            let d2 = sq_dist(current, neighbour);
            if d2 <= rsq && d2 > 0.0 { /* valid edge */ }
        }
    }
}}}
```

The ship A* does NOT use this pattern — it queries the R-tree DB directly per node.

---

## Bidirectional A* Termination Condition

Used for carrier (>5k LY) and neutron (>10k LY):
```rust
let mut mu: u32 = greedy_jumps as u32; // upper bound = best path so far
let mut best_meeting: Option<i64> = None;

// Each iteration:
// 1. Pick the frontier (fwd or bwd) with lower min_f
// 2. Terminate if fwd_open.min_f + bwd_open.min_f >= mu
// 3. When a node is settled by one direction and already in the other's g-map:
//    if fwd_g[n] + bwd_g[n] < mu: update mu, best_meeting = n
// 4. Prune: skip nodes with g >= mu

// Reconstruction:
// fwd: walk came_from from meeting_node → src, reverse
// bwd: walk came_from from meeting_node → dst
// join: fwd_path + bwd_path (meeting node appears once)
```

---

## Vulkan Reimplementation Notes

The bottleneck in all three routers is the priority queue expansion + neighbour lookup.

### What to GPU-accelerate

**Target: Carrier and Neutron A*** — both use preloaded in-memory graphs.
Ship A* hits the DB per expansion, so GPU parallelism doesn't help there without
also moving the R-tree to GPU memory.

### Data layout for GPU
All nodes can be packed as structs of arrays:
```
node_id[N]:   i64
x[N], y[N], z[N]: f32 (f64 → f32 is fine for LY-scale distances)
is_neutron[N]: u32  (neutron only)
```

The spatial grid maps cleanly to a fixed-size voxel buffer or a sorted list + binary search.

### What stays on CPU
- SQLite queries (corridor preload)
- BinaryHeap / open set management (Vulkan doesn't have a concurrent priority queue)
- Path reconstruction (pointer-chasing on came_from)
- Fuel simulation (sequential, not compute-heavy)
- HTTP handling, JSON serialisation

### Suggested Vulkan compute dispatch
One dispatch per A* iteration layer (frontier expansion):
- Input: current open set (flat array of `{g, f, id}`)
- For each node in batch: find all neighbours via voxel grid lookup
- Output: candidate relaxations `{node_id, new_g, parent_id}`
- CPU collects outputs, deduplicates, updates open set / g-map

This maps well to Vulkan push constants for `mu`, `jump_range_sq`, `dst_coords`,
and storage buffers for the node arrays and voxel grid.

---

## Key Crates (current CPU impl)

```toml
axum            # HTTP
tokio           # async runtime
rusqlite        # SQLite
r2d2 / r2d2_sqlite # connection pool
serde / serde_json
crossbeam-channel  # EDMC writer channel
tracing
tower-http      # CORS
flate2          # gz decode (Spansh dump)
reqwest         # Spansh download
```

For Vulkan: add `ash` or `wgpu` (wgpu preferred for Pi 5 / portability).

---

## API Base URL
```
https://ananke.projectgaltea.org
```
Do NOT use EDSM — it is broken. All system lookups go through Ananke.
