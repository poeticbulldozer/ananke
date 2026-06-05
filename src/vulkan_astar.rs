/// vulkan_astar.rs — GPU-accelerated A* for Ananke carrier/neutron routing
///
/// Architecture
/// ============
/// The inner neighbour-expansion loop (125-voxel-cell cube lookup) is the hot path.
/// We batch the open set into a GPU dispatch; each invocation handles one frontier node,
/// walks the voxel hash table, and emits candidate relaxations into an atomic output
/// buffer. The CPU then ingests those relaxations, updates g_score / came_from, and
/// re-fills the heap. Path reconstruction and termination stay on CPU.
///
/// Works on any Vulkan 1.0 compute device. On the Steam Deck (RDNA2 / RADV) the APU
/// shares RAM so buffer uploads cost nothing — ideal.
///
/// Integration
/// ===========
/// 1. Add to AppState:   pub vulkan_astar: Option<Arc<VulkanAstar>>
/// 2. Init in main:      AppState { vulkan_astar: VulkanAstar::init(), .. }
/// 3. In carrier_route:  replace the BinaryHeap expansion loop (not the greedy
///    phase, not fuel sim) with run_unidirectional / run_bidirectional.
/// 4. In neutron_route:  same — replace only the A* refinement loop.
///    The greedy seeding + bridge stitching before/after still runs on CPU.
///
/// The functions return Option<Vec<i64>> (system id64 path). None means the GPU
/// failed or didn't improve on greedy — caller should fall back to the existing
/// CPU A* path.
///
/// Cargo.toml already has vulkano = "0.34" and vulkano-shaders = "0.34". ✓
///
/// ── Optimisation notes ─────────────────────────────────────────────────────
/// CPU-side hot-loop state (g_score, came_from, closed) uses flat Vec<u32> /
/// Vec<bool> indexed by node idx instead of HashMap / HashSet. Node indices are
/// sequential 0..n so direct indexing is O(1) with no hashing overhead and much
/// better cache locality.
///
/// The Vulkan descriptor set is built once per route and reused across every
/// dispatch_frontier call (graph + pre-allocated buffers don't change within a
/// route).
///
/// Relaxations returned from the GPU are sorted by to_idx before the CPU
/// processes them, giving sequential access into the flat g_score / came_from
/// arrays and reducing cache misses.
///
/// Batch vectors are allocated once outside the main loop and reused via
/// clear() to avoid per-iteration allocation.

use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use vulkano::{
    buffer::{Buffer, BufferContents, BufferCreateInfo, BufferUsage, Subbuffer},
    command_buffer::{
        allocator::StandardCommandBufferAllocator, AutoCommandBufferBuilder,
        CommandBufferUsage,
    },
    descriptor_set::{
        allocator::StandardDescriptorSetAllocator,
        layout::{
            DescriptorSetLayout, DescriptorSetLayoutBinding, DescriptorSetLayoutCreateInfo,
            DescriptorType,
        },
        PersistentDescriptorSet, WriteDescriptorSet,
    },
    device::{
        physical::PhysicalDeviceType, Device, DeviceCreateInfo, Queue, QueueCreateInfo,
        QueueFlags,
    },
    instance::{Instance, InstanceCreateInfo},
    memory::allocator::{AllocationCreateInfo, MemoryTypeFilter, StandardMemoryAllocator},
    pipeline::{
        compute::ComputePipelineCreateInfo,
        layout::{PipelineLayoutCreateInfo, PushConstantRange},
        ComputePipeline, Pipeline, PipelineBindPoint, PipelineLayout,
        PipelineShaderStageCreateInfo,
    },
    shader::ShaderStages,
    sync::{self, GpuFuture},
    VulkanLibrary,
};

// ─── GPU-side data types ─────────────────────────────────────────────────────
// All #[repr(C)] to match GLSL std430 layout.

/// One graph node (f32 is plenty at LY scale; f64 → f32 cast by caller).
#[derive(Clone, Copy, BufferContents)]
#[repr(C)]
struct GpuNode {
    x: f32, y: f32, z: f32,
    _pad: f32,
}

/// One slot in the open-addressing voxel hash table.
/// `count == 0xFFFF_FFFF` is the *empty* sentinel.
#[derive(Clone, Copy, BufferContents)]
#[repr(C)]
struct GpuVoxelSlot {
    cx: i32, cy: i32, cz: i32,
    offset: u32,    // index into flat voxel_node_idx[]
    count:  u32,    // number of node indices in this cell
    _pad:   u32,
}

/// One entry in the frontier passed to the GPU each iteration.
#[derive(Clone, Copy, BufferContents)]
#[repr(C)]
struct GpuFrontierNode {
    node_idx: u32,
    g:        u32,
}

/// One relaxation candidate emitted by the GPU.
#[derive(Clone, Copy, BufferContents)]
#[repr(C)]
pub struct GpuRelaxation {
    pub from_idx: u32,
    pub to_idx:   u32,
    pub new_g:    u32,
    _pad:         u32,
}

/// Atomic output counter + overflow flag.
#[derive(Clone, Copy, BufferContents)]
#[repr(C)]
struct GpuCounter {
    count:    u32,
    overflow: u32,
}

/// Push constants — one block covering all tuning knobs.
/// Must stay ≤ 128 bytes (guaranteed minimum by Vulkan spec).
#[derive(Clone, Copy, BufferContents)]
#[repr(C)]
struct PushConstants {
    frontier_size:        u32,
    mu:                   u32,   // upper bound (greedy_jumps); GPU prunes paths ≥ mu
    jump_range_sq:        f32,
    cell_size:            f32,
    dst_x:                f32,
    dst_y:                f32,
    dst_z:                f32,
    dst_node_idx:         u32,
    table_mask:           u32,   // power-of-2 table size − 1
    max_total_relaxations: u32,  // capacity of the relaxation output buffer
}

// ─── Inline GLSL compute shader ───────────────────────────────────────────────
// Compiled at build time by vulkano-shaders / shaderc.
// Each invocation = one frontier node.  Emits relaxations via atomicAdd.

mod cs {
    vulkano_shaders::shader! {
        ty: "compute",
        src: r#"
#version 450
layout(local_size_x = 64, local_size_y = 1, local_size_z = 1) in;

// ── GLSL struct must match GpuVoxelSlot (std430, 24 bytes) ──────────────────
struct VoxelSlot {
    int  cx, cy, cz;
    uint offset;
    uint count;
    uint _pad;
};

// ── Bindings ──────────────────────────────────────────────────────────────────
layout(set = 0, binding = 0) readonly buffer NodeBuf     { vec4        nodes[];          };
layout(set = 0, binding = 1) readonly buffer VoxelTable  { VoxelSlot   voxel_table[];    };
layout(set = 0, binding = 2) readonly buffer VoxelNodes  { uint        voxel_node_idx[]; };
layout(set = 0, binding = 3) readonly buffer FrontierBuf { uvec2       frontier[];       };
layout(set = 0, binding = 4) buffer         RelaxBuf     { uvec4       relaxations[];    };
layout(set = 0, binding = 5) buffer         CounterBuf   { uint relax_count; uint overflow_count; };

// ── Push constants ────────────────────────────────────────────────────────────
layout(push_constant) uniform PC {
    uint  frontier_size;
    uint  mu;
    float jump_range_sq;
    float cell_size;
    float dst_x, dst_y, dst_z;
    uint  dst_node_idx;
    uint  table_mask;
    uint  max_total_relaxations;
};

// ── Helpers ───────────────────────────────────────────────────────────────────
uint hash3(int cx, int cy, int cz) {
    return uint(cx) * 2654435761u
         ^ uint(cy) *  805459861u
         ^ uint(cz) * 3266489917u;
}

// Returns voxel_table slot index, or 0xFFFFFFFF if not found.
// Uses linear probing with a 32-step cap (table is ≥4× loaded so rare).
uint find_voxel(int cx, int cy, int cz) {
    uint probe = hash3(cx, cy, cz) & table_mask;
    for (uint i = 0u; i < 32u; i++) {
        uint s   = (probe + i) & table_mask;
        uint cnt = voxel_table[s].count;
        if (cnt == 0xFFFFFFFFu) return 0xFFFFFFFFu; // empty slot — key absent
        if (voxel_table[s].cx == cx &&
            voxel_table[s].cy == cy &&
            voxel_table[s].cz == cz) return s;
    }
    return 0xFFFFFFFFu;
}

void emit_relaxation(uint from_idx, uint to_idx, uint new_g) {
    uint slot = atomicAdd(relax_count, 1u);
    if (slot < max_total_relaxations) {
        relaxations[slot] = uvec4(from_idx, to_idx, new_g, 0u);
    } else {
        atomicAdd(overflow_count, 1u);
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────
void main() {
    uint tid = gl_GlobalInvocationID.x;
    if (tid >= frontier_size) return;

    uvec2 f      = frontier[tid];
    uint my_idx  = f.x;
    uint my_g    = f.y;
    if (my_g + 1u >= mu) return;   // prune: can't beat upper bound

    vec3 p = nodes[my_idx].xyz;

    // ── Direct reach to terminal node ─────────────────────────────────────
    if (my_idx != dst_node_idx) {
        vec3  d_dst  = p - vec3(dst_x, dst_y, dst_z);
        float d2_dst = dot(d_dst, d_dst);
        if (d2_dst <= jump_range_sq && d2_dst > 0.0) {
            emit_relaxation(my_idx, dst_node_idx, my_g + 1u);
        }
    }

    // ── Expand ±2 voxel cube (125 cells) ─────────────────────────────────
    int cx = int(floor(p.x / cell_size));
    int cy = int(floor(p.y / cell_size));
    int cz = int(floor(p.z / cell_size));

    for (int dx = -2; dx <= 2; dx++) {
    for (int dy = -2; dy <= 2; dy++) {
    for (int dz = -2; dz <= 2; dz++) {
        uint slot = find_voxel(cx + dx, cy + dy, cz + dz);
        if (slot == 0xFFFFFFFFu) continue;

        uint off = voxel_table[slot].offset;
        uint cnt = voxel_table[slot].count;

        for (uint i = off; i < off + cnt; i++) {
            uint  n_idx = voxel_node_idx[i];
            if (n_idx == my_idx) continue;

            vec3  np   = nodes[n_idx].xyz;
            vec3  diff = np - p;
            float d2   = dot(diff, diff);
            if (d2 > jump_range_sq || d2 == 0.0) continue;

            emit_relaxation(my_idx, n_idx, my_g + 1u);
        }
    }}}
}
        "#,
    }
}

// ─── GpuGraph — corridor data uploaded once per route request ────────────────

pub struct GpuGraph {
    node_buf:    Subbuffer<[GpuNode]>,
    voxel_table: Subbuffer<[GpuVoxelSlot]>,
    voxel_nodes: Subbuffer<[u32]>,

    /// CPU-side lookups (cheap on APU since there's no separate VRAM copy).
    pub id_to_idx: HashMap<i64, u32>,
    pub idx_to_id: Vec<i64>,
    /// f32 positions kept CPU-side for heuristic evaluation without GPU readback.
    pub node_pos:  Vec<(f32, f32, f32)>,

    pub cell_size:   f32,
    pub table_mask:  u32,
    pub node_count:  usize,
}

// ─── VulkanAstar ─────────────────────────────────────────────────────────────

pub struct VulkanAstar {
    device:    Arc<Device>,
    queue:     Arc<Queue>,
    pipeline:  Arc<ComputePipeline>,
    mem_alloc: Arc<StandardMemoryAllocator>,
    cmd_alloc: StandardCommandBufferAllocator,
    ds_alloc:  StandardDescriptorSetAllocator,

    // Pre-allocated buffers reused across dispatch_frontier calls to avoid
    // per-iteration allocator churn. Sized to max capacity at init time.
    frontier_buf: Subbuffer<[GpuFrontierNode]>,
    relax_buf:    Subbuffer<[GpuRelaxation]>,
    counter_buf:  Subbuffer<GpuCounter>,
}

/// Frontier nodes drained per GPU dispatch. 512 = 8 workgroups of 64 on RDNA2.
/// Each emits up to MAX_RELAX_PER_NODE candidates → output buf = 512 × 512 × 16B = 4 MB.
const BATCH_SIZE:          u32 = 512;
const MAX_RELAX_PER_NODE:  u32 = 512;

/// Sentinel for "no value" in flat g_score / came_from arrays.
const SENTINEL: u32 = u32::MAX;

impl VulkanAstar {
    // ── Initialisation ───────────────────────────────────────────────────────

    /// Returns None if Vulkan is unavailable (process falls back to CPU A*).
    pub fn init() -> Option<Arc<Self>> {
        let lib = VulkanLibrary::new()
            .map_err(|e| warn!("Vulkan library unavailable: {e}")).ok()?;

        let instance = Instance::new(lib, InstanceCreateInfo::default())
            .map_err(|e| warn!("Vulkan instance: {e}")).ok()?;

        // Pick best compute-capable device (prefer discrete > integrated).
        let (phys, qfi) = instance
            .enumerate_physical_devices()
            .map_err(|e| warn!("Enumerate physical devices: {e}")).ok()?
            .filter_map(|p| {
                let qfi = p.queue_family_properties()
                    .iter().enumerate()
                    .find(|(_, q)| q.queue_flags.contains(QueueFlags::COMPUTE))
                    .map(|(i, _)| i as u32)?;
                Some((p, qfi))
            })
            .min_by_key(|(p, _)| match p.properties().device_type {
                PhysicalDeviceType::DiscreteGpu   => 0,
                PhysicalDeviceType::IntegratedGpu => 1,
                PhysicalDeviceType::VirtualGpu    => 2,
                _                                 => 3,
            })?;

        info!("VulkanAstar: {:?} — {}",
            phys.properties().device_type,
            phys.properties().device_name);

        let (device, mut queues) = Device::new(phys, DeviceCreateInfo {
            queue_create_infos: vec![QueueCreateInfo {
                queue_family_index: qfi,
                ..Default::default()
            }],
            ..Default::default()
        }).map_err(|e| warn!("Vulkan device: {e}")).ok()?;

        let queue     = queues.next()?;
        let mem_alloc = Arc::new(StandardMemoryAllocator::new_default(device.clone()));
        let cmd_alloc = StandardCommandBufferAllocator::new(device.clone(), Default::default());
        let ds_alloc  = StandardDescriptorSetAllocator::new(device.clone(), Default::default());

        let pipeline = Self::build_pipeline(device.clone())?;

        // Pre-allocate dispatch buffers at maximum required size so
        // dispatch_frontier never touches the allocator.
        let frontier_buf: Subbuffer<[GpuFrontierNode]> = Buffer::new_slice(
            mem_alloc.clone(),
            BufferCreateInfo { usage: BufferUsage::STORAGE_BUFFER, ..Default::default() },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_HOST
                    | MemoryTypeFilter::HOST_SEQUENTIAL_WRITE,
                ..Default::default()
            },
            BATCH_SIZE as u64,
        ).map_err(|e| warn!("frontier_buf alloc: {e}")).ok()?;

        let relax_buf: Subbuffer<[GpuRelaxation]> = Buffer::new_slice(
            mem_alloc.clone(),
            BufferCreateInfo { usage: BufferUsage::STORAGE_BUFFER, ..Default::default() },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_HOST
                    | MemoryTypeFilter::HOST_RANDOM_ACCESS,
                ..Default::default()
            },
            (BATCH_SIZE * MAX_RELAX_PER_NODE) as u64,
        ).map_err(|e| warn!("relax_buf alloc: {e}")).ok()?;

        let counter_buf: Subbuffer<GpuCounter> = Buffer::from_data(
            mem_alloc.clone(),
            BufferCreateInfo { usage: BufferUsage::STORAGE_BUFFER, ..Default::default() },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_HOST
                    | MemoryTypeFilter::HOST_RANDOM_ACCESS,
                ..Default::default()
            },
            GpuCounter { count: 0, overflow: 0 },
        ).map_err(|e| warn!("counter_buf alloc: {e}")).ok()?;

        Some(Arc::new(Self {
            device, queue, pipeline, mem_alloc, cmd_alloc, ds_alloc,
            frontier_buf, relax_buf, counter_buf,
        }))
    }

    fn build_pipeline(device: Arc<Device>) -> Option<Arc<ComputePipeline>> {
        let shader = cs::load(device.clone())
            .map_err(|e| warn!("Shader compile: {e}")).ok()?;
        let entry  = shader.entry_point("main")?;
        let stage  = PipelineShaderStageCreateInfo::new(entry);

        // 6 storage buffer bindings
        let bindings = (0u32..6)
            .map(|b| (b, DescriptorSetLayoutBinding {
                stages: ShaderStages::COMPUTE,
                ..DescriptorSetLayoutBinding::descriptor_type(DescriptorType::StorageBuffer)
            }))
            .collect();

        let ds_layout = DescriptorSetLayout::new(device.clone(),
            DescriptorSetLayoutCreateInfo { bindings, ..Default::default() })
            .map_err(|e| warn!("DS layout: {e}")).ok()?;

        let layout = PipelineLayout::new(device.clone(), PipelineLayoutCreateInfo {
            set_layouts: vec![ds_layout],
            push_constant_ranges: vec![PushConstantRange {
                stages: ShaderStages::COMPUTE,
                offset: 0,
                size: std::mem::size_of::<PushConstants>() as u32,
            }],
            ..Default::default()
        }).map_err(|e| warn!("Pipeline layout: {e}")).ok()?;

        ComputePipeline::new(device, None,
            ComputePipelineCreateInfo::stage_layout(stage, layout))
            .map_err(|e| warn!("Compute pipeline: {e}")).ok()
    }

    // ── Build GpuGraph from a corridor node list ─────────────────────────────
    //
    // `nodes`: (id64, x, y, z) in f32.  cell_size must match the caller's voxel grid.
    // Builds the voxel hash table with open addressing (load factor ≤ 0.25).

    pub fn build_graph(
        &self,
        nodes:     &[(i64, f32, f32, f32)],
        cell_size: f32,
    ) -> Option<GpuGraph> {
        let n = nodes.len();
        let mut id_to_idx: HashMap<i64, u32>    = HashMap::with_capacity(n);
        let mut idx_to_id: Vec<i64>             = Vec::with_capacity(n);
        let mut node_pos:  Vec<(f32, f32, f32)> = Vec::with_capacity(n);

        for (i, &(id, x, y, z)) in nodes.iter().enumerate() {
            id_to_idx.insert(id, i as u32);
            idx_to_id.push(id);
            node_pos.push((x, y, z));
        }

        // GPU node buffer — host-sequential-write is fine on APU (unified memory)
        let node_buf = Buffer::from_iter(
            self.mem_alloc.clone(),
            BufferCreateInfo { usage: BufferUsage::STORAGE_BUFFER, ..Default::default() },
            AllocationCreateInfo {
                memory_type_filter: MemoryTypeFilter::PREFER_DEVICE
                    | MemoryTypeFilter::HOST_SEQUENTIAL_WRITE,
                ..Default::default()
            },
            node_pos.iter().map(|&(x, y, z)| GpuNode { x, y, z, _pad: 0.0 }),
        ).ok()?;

        // Build CPU voxel grid
        let mut cpu_grid: HashMap<(i32,i32,i32), Vec<u32>> = HashMap::new();
        for (i, &(x, y, z)) in node_pos.iter().enumerate() {
            let cell = (
                (x / cell_size).floor() as i32,
                (y / cell_size).floor() as i32,
                (z / cell_size).floor() as i32,
            );
            cpu_grid.entry(cell).or_default().push(i as u32);
        }

        // Flatten to CSR value array + open-addressing hash table
        let table_size = ((cpu_grid.len() * 4).next_power_of_two()).max(16) as u32;
        let table_mask = table_size - 1;
        let empty_slot = GpuVoxelSlot { cx: 0, cy: 0, cz: 0, offset: 0, count: 0xFFFF_FFFF, _pad: 0 };
        let mut table: Vec<GpuVoxelSlot> = vec![empty_slot; table_size as usize];
        let mut voxel_flat: Vec<u32>     = Vec::new();

        for ((cx, cy, cz), indices) in &cpu_grid {
            let offset = voxel_flat.len() as u32;
            let count  = indices.len() as u32;
            voxel_flat.extend_from_slice(indices);

            let mut probe = (hash3_cpu(*cx, *cy, *cz) & table_mask) as usize;
            loop {
                if table[probe].count == 0xFFFF_FFFF {
                    table[probe] = GpuVoxelSlot { cx: *cx, cy: *cy, cz: *cz, offset, count, _pad: 0 };
                    break;
                }
                probe = (probe + 1) & table_mask as usize;
            }
        }

        if voxel_flat.is_empty() { voxel_flat.push(0); } // zero-length SSBO disallowed

        fn upload<T: BufferContents>(
            alloc: Arc<StandardMemoryAllocator>,
            data: Vec<T>,
        ) -> Option<Subbuffer<[T]>> {
            Buffer::from_iter(
                alloc,
                BufferCreateInfo { usage: BufferUsage::STORAGE_BUFFER, ..Default::default() },
                AllocationCreateInfo {
                    memory_type_filter: MemoryTypeFilter::PREFER_DEVICE
                        | MemoryTypeFilter::HOST_SEQUENTIAL_WRITE,
                    ..Default::default()
                },
                data,
            ).ok()
        }

        let voxel_table = upload(self.mem_alloc.clone(), table)?;
        let voxel_nodes = upload(self.mem_alloc.clone(), voxel_flat)?;

        info!("GpuGraph: {} nodes, {} cells, table_size={}",
            n, cpu_grid.len(), table_size);

        Some(GpuGraph {
            node_buf, voxel_table, voxel_nodes,
            id_to_idx, idx_to_id, node_pos,
            cell_size, table_mask,
            node_count: n,
        })
    }

    // ── Build a descriptor set for an entire route ───────────────────────────
    //
    // Graph buffers + pre-allocated frontier/relax/counter buffers don't change
    // within a route, so we build the DS once and reuse it for every dispatch.
    // Eliminates per-dispatch DS allocation + validation overhead.

    fn build_route_ds(
        &self,
        graph: &GpuGraph,
    ) -> Option<Arc<PersistentDescriptorSet>> {
        PersistentDescriptorSet::new(
            &self.ds_alloc,
            self.pipeline.layout().set_layouts()[0].clone(),
            [
                WriteDescriptorSet::buffer(0, graph.node_buf.clone()),
                WriteDescriptorSet::buffer(1, graph.voxel_table.clone()),
                WriteDescriptorSet::buffer(2, graph.voxel_nodes.clone()),
                WriteDescriptorSet::buffer(3, self.frontier_buf.clone()),
                WriteDescriptorSet::buffer(4, self.relax_buf.clone()),
                WriteDescriptorSet::buffer(5, self.counter_buf.clone()),
            ],
            [],
        ).map_err(|e| warn!("Route DS create: {e}")).ok()
    }

    // ── GPU dispatch: expand one batch ───────────────────────────────────────
    //
    // frontier: (node_idx, g) pairs.
    // dst_idx / dst_pos: the terminal node for this direction (fwd→actual dst,
    //   bwd→actual src). GPU only emits a direct-reach relaxation for this node.
    // ds: pre-built descriptor set from build_route_ds (reused across dispatches).
    // Returns None on Vulkan error (caller falls back to CPU).

    fn dispatch_frontier(
        &self,
        graph:    &GpuGraph,
        frontier: &[(u32, u32)],
        mu:       u32,
        jump_range: f32,
        dst_idx:  u32,
        dst_pos:  (f32, f32, f32),
        ds:       &Arc<PersistentDescriptorSet>,
    ) -> Option<Vec<GpuRelaxation>> {
        if frontier.is_empty() { return Some(vec![]); }

        let fsize     = frontier.len() as u32;
        let max_relax = BATCH_SIZE * MAX_RELAX_PER_NODE;

        // ── Write frontier data into pre-allocated buffer ────────────────────
        {
            let mut guard = self.frontier_buf.write().ok()?;
            for (i, &(node_idx, g)) in frontier.iter().enumerate() {
                guard[i] = GpuFrontierNode { node_idx, g };
            }
        }

        // ── Zero the counter before each dispatch ────────────────────────────
        {
            let mut guard = self.counter_buf.write().ok()?;
            *guard = GpuCounter { count: 0, overflow: 0 };
        }

        // ── Dispatch ──────────────────────────────────────────────────────────

        let pc = PushConstants {
            frontier_size:        fsize,
            mu,
            jump_range_sq:        jump_range * jump_range,
            cell_size:            graph.cell_size,
            dst_x:                dst_pos.0,
            dst_y:                dst_pos.1,
            dst_z:                dst_pos.2,
            dst_node_idx:         dst_idx,
            table_mask:           graph.table_mask,
            max_total_relaxations: max_relax,
        };

        let wg = fsize.div_ceil(64);

        let mut builder = AutoCommandBufferBuilder::primary(
            &self.cmd_alloc,
            self.queue.queue_family_index(),
            CommandBufferUsage::OneTimeSubmit,
        ).ok()?;

        builder
            .bind_pipeline_compute(self.pipeline.clone())
            .map_err(|e| warn!("bind pipeline: {e}")).ok()?
            .bind_descriptor_sets(
                PipelineBindPoint::Compute,
                self.pipeline.layout().clone(),
                0,
                ds.clone(),     // Arc clone = refcount bump, not a deep copy
            )
            .map_err(|e| warn!("bind DS: {e}")).ok()?
            .push_constants(self.pipeline.layout().clone(), 0, pc)
            .map_err(|e| warn!("push constants: {e}")).ok()?
            .dispatch([wg, 1, 1])
            .map_err(|e| warn!("dispatch: {e}")).ok()?;

        let cmd = builder.build().ok()?;

        sync::now(self.device.clone())
            .then_execute(self.queue.clone(), cmd)
            .map_err(|e| warn!("execute: {e}")).ok()?
            .then_signal_fence_and_flush()
            .map_err(|e| warn!("flush: {e}")).ok()?
            .wait(None)
            .map_err(|e| warn!("wait: {e}")).ok()?;

        // ── Readback ──────────────────────────────────────────────────────────

        let counter  = self.counter_buf.read().ok()?;
        let n_relax  = (counter.count as usize).min(max_relax as usize);
        if counter.overflow > 0 {
            warn!("VulkanAstar: {} relaxation overflow — raise MAX_RELAX_PER_NODE", counter.overflow);
        }

        let guard = self.relax_buf.read().ok()?;
        let mut relax = guard[..n_relax].to_vec();

        // Sort by to_idx so CPU-side g_score/came_from updates hit flat arrays
        // sequentially, improving cache locality for large relaxation batches.
        relax.sort_unstable_by_key(|r| r.to_idx);

        Some(relax)
    }

    // ── Unidirectional A* ────────────────────────────────────────────────────
    //
    // Used for carrier routes ≤ 5 000 LY, neutron routes ≤ 10 000 LY.
    //
    // `src_id` / `dst_id` must exist in graph.id_to_idx (caller ensures this by
    // inserting them into the node list before calling build_graph).
    // Returns None if GPU fails or finds no improvement over greedy_jumps.

    pub fn run_unidirectional(
        &self,
        graph:        &GpuGraph,
        src_id:       i64,
        dst_id:       i64,
        jump_range:   f32,
        greedy_jumps: u32,
        budget_ms:    u128,
    ) -> Option<Vec<i64>> {
        let src_idx = *graph.id_to_idx.get(&src_id)?;
        let dst_idx = *graph.id_to_idx.get(&dst_id)?;
        let dst_pos = graph.node_pos[dst_idx as usize];
        let n       = graph.node_count;
        let t_start = Instant::now();

        // Build descriptor set once for this entire route.
        let ds = self.build_route_ds(graph)?;

        // ── Flat arrays: O(1) direct-indexed, cache-friendly ─────────────────
        // Replaces HashMap<u32,u32> g_score / came_from and HashSet<u32> closed.
        // Indices are sequential 0..n so we index directly. SENTINEL = "unvisited".
        let mut g_score:   Vec<u32>  = vec![SENTINEL; n];
        let mut came_from: Vec<u32>  = vec![SENTINEL; n];
        let mut closed:    Vec<bool> = vec![false; n];

        // min-heap keyed on f = g + h; store f as f64 bits for Ord.
        #[derive(Eq, PartialEq)]
        struct ONode { f_bits: u64, idx: u32, g: u32 }
        impl Ord        for ONode { fn cmp(&self, o: &Self) -> std::cmp::Ordering { o.f_bits.cmp(&self.f_bits) } }
        impl PartialOrd for ONode { fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(o)) } }

        let jr = jump_range as f64;
        let h = |idx: u32| -> f64 {
            let (x, y, z) = graph.node_pos[idx as usize];
            let (dx, dy, dz) = (x - dst_pos.0, y - dst_pos.1, z - dst_pos.2);
            ((dx*dx + dy*dy + dz*dz).sqrt() as f64 / jr).ceil()
        };
        let mk = |idx: u32, g: u32| ONode {
            f_bits: (g as f64 + h(idx)).to_bits(), idx, g,
        };

        let mut open = BinaryHeap::with_capacity(BATCH_SIZE as usize * 16);

        g_score[src_idx as usize] = 0;
        open.push(mk(src_idx, 0));

        let mut best_g_dst = greedy_jumps; // upper bound; tightened when dst reached

        // Reusable batch buffer — cleared each iteration instead of reallocated.
        let mut batch: Vec<(u32, u32)> = Vec::with_capacity(BATCH_SIZE as usize);

        while !open.is_empty() {
            if t_start.elapsed().as_millis() > budget_ms { break; }

            // Drain up to BATCH_SIZE nodes with lowest f-score.
            batch.clear();
            while batch.len() < BATCH_SIZE as usize {
                let Some(node) = open.pop() else { break };
                if node.g >= best_g_dst                    { continue; }
                if closed[node.idx as usize]               { continue; }
                // Stale entry check: heap may hold outdated g-values.
                if node.g > g_score[node.idx as usize]     { continue; }
                closed[node.idx as usize] = true;
                if node.idx == dst_idx {
                    best_g_dst = node.g;
                    break;
                }
                batch.push((node.idx, node.g));
            }

            if batch.is_empty() { break; }

            let relaxations = self.dispatch_frontier(
                graph, &batch, best_g_dst, jump_range, dst_idx, dst_pos, &ds,
            )?;

            for r in &relaxations {
                let ti = r.to_idx as usize;
                if r.to_idx == dst_idx {
                    if r.new_g < best_g_dst {
                        best_g_dst = r.new_g;
                        came_from[ti] = r.from_idx;
                        g_score[ti]   = r.new_g;
                    }
                    continue;
                }
                if r.new_g < g_score[ti] {
                    g_score[ti]   = r.new_g;
                    came_from[ti] = r.from_idx;
                    if !closed[ti] {
                        open.push(mk(r.to_idx, r.new_g));
                    }
                }
            }
        }

        if best_g_dst >= greedy_jumps { return None; } // didn't improve
        if came_from[dst_idx as usize] == SENTINEL && !closed[dst_idx as usize] { return None; }

        reconstruct_path_flat(&came_from, src_idx, dst_idx, &graph.idx_to_id)
    }

    // ── Bidirectional A* ─────────────────────────────────────────────────────
    //
    // Used for carrier routes > 5 000 LY, neutron routes > 10 000 LY.
    // Mirrors the CPU bidirectional structure exactly.
    //
    // Seeds (fwd_seed_id, bwd_seed_id) let the neutron router start from the first/last
    // neutron in the greedy path rather than from src/dst, exactly as the CPU version does.
    // Pass src_id/dst_id to disable seeding (carrier case).

    pub fn run_bidirectional(
        &self,
        graph:        &GpuGraph,
        src_id:       i64,
        dst_id:       i64,
        fwd_seed_id:  i64,   // starting node for forward search (== src_id for carrier)
        bwd_seed_id:  i64,   // starting node for backward search (== dst_id for carrier)
        fwd_seed_g:   u32,   // g at fwd_seed (jumps already paid for bridge hops)
        bwd_seed_g:   u32,
        jump_range:   f32,
        greedy_jumps: u32,
        budget_ms:    u128,
    ) -> Option<Vec<i64>> {
        let src_idx      = *graph.id_to_idx.get(&src_id)?;
        let dst_idx      = *graph.id_to_idx.get(&dst_id)?;
        let fwd_seed_idx = *graph.id_to_idx.get(&fwd_seed_id)?;
        let bwd_seed_idx = *graph.id_to_idx.get(&bwd_seed_id)?;
        let src_pos      = graph.node_pos[src_idx as usize];
        let dst_pos      = graph.node_pos[dst_idx as usize];
        let n            = graph.node_count;
        let t_start      = Instant::now();
        let jr           = jump_range as f64;

        // Build descriptor set once for this entire route.
        let ds = self.build_route_ds(graph)?;

        let h_fwd = |idx: u32| -> f64 {
            let (x, y, z) = graph.node_pos[idx as usize];
            let (dx, dy, dz) = (x - dst_pos.0, y - dst_pos.1, z - dst_pos.2);
            ((dx*dx + dy*dy + dz*dz).sqrt() as f64 / jr).ceil()
        };
        let h_bwd = |idx: u32| -> f64 {
            let (x, y, z) = graph.node_pos[idx as usize];
            let (dx, dy, dz) = (x - src_pos.0, y - src_pos.1, z - src_pos.2);
            ((dx*dx + dy*dy + dz*dz).sqrt() as f64 / jr).ceil()
        };

        #[derive(Eq, PartialEq)]
        struct ONode { f_bits: u64, idx: u32, g: u32 }
        impl Ord        for ONode { fn cmp(&self, o: &Self) -> std::cmp::Ordering { o.f_bits.cmp(&self.f_bits) } }
        impl PartialOrd for ONode { fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(o)) } }

        let mk_fwd = |idx: u32, g: u32| ONode { f_bits: (g as f64 + h_fwd(idx)).to_bits(), idx, g };
        let mk_bwd = |idx: u32, g: u32| ONode { f_bits: (g as f64 + h_bwd(idx)).to_bits(), idx, g };

        // ── Flat arrays for both directions ──────────────────────────────────
        let mut fwd_g:      Vec<u32>  = vec![SENTINEL; n];
        let mut bwd_g:      Vec<u32>  = vec![SENTINEL; n];
        let mut fwd_cf:     Vec<u32>  = vec![SENTINEL; n];
        let mut bwd_cf:     Vec<u32>  = vec![SENTINEL; n];
        let mut fwd_closed: Vec<bool> = vec![false; n];
        let mut bwd_closed: Vec<bool> = vec![false; n];

        let mut fwd_open = BinaryHeap::with_capacity(BATCH_SIZE as usize * 16);
        let mut bwd_open = BinaryHeap::with_capacity(BATCH_SIZE as usize * 16);

        fwd_g[fwd_seed_idx as usize] = fwd_seed_g;
        bwd_g[bwd_seed_idx as usize] = bwd_seed_g;
        fwd_open.push(mk_fwd(fwd_seed_idx, fwd_seed_g));
        bwd_open.push(mk_bwd(bwd_seed_idx, bwd_seed_g));

        let mut mu:           u32         = greedy_jumps;
        let mut best_meeting: Option<u32> = None;

        // Reusable batch buffer.
        let mut batch: Vec<(u32, u32)> = Vec::with_capacity(BATCH_SIZE as usize);

        loop {
            if t_start.elapsed().as_millis() > budget_ms { break; }
            if fwd_open.is_empty() && bwd_open.is_empty() { break; }

            let fwd_min_g = fwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
            let bwd_min_g = bwd_open.peek().map(|n| n.g).unwrap_or(u32::MAX);
            if fwd_min_g.saturating_add(bwd_min_g) >= mu { break; }

            let fwd_min_f = fwd_open.peek().map(|n| n.f_bits).unwrap_or(u64::MAX);
            let bwd_min_f = bwd_open.peek().map(|n| n.f_bits).unwrap_or(u64::MAX);
            let expand_fwd = fwd_min_f <= bwd_min_f;

            if expand_fwd {
                // ── Forward expansion ────────────────────────────────────────
                batch.clear();
                while batch.len() < BATCH_SIZE as usize {
                    let Some(node) = fwd_open.pop() else { break };
                    if node.g >= mu                        { continue; }
                    if fwd_closed[node.idx as usize]       { continue; }
                    if node.g > fwd_g[node.idx as usize]   { continue; }
                    fwd_closed[node.idx as usize] = true;
                    // Meeting check
                    let bg = bwd_g[node.idx as usize];
                    if bg != SENTINEL {
                        let total = node.g + bg;
                        if total < mu { mu = total; best_meeting = Some(node.idx); }
                    }
                    batch.push((node.idx, node.g));
                }
                if batch.is_empty() { continue; }

                let relax = self.dispatch_frontier(
                    graph, &batch, mu, jump_range, dst_idx, dst_pos, &ds,
                )?;

                for r in &relax {
                    let ti = r.to_idx as usize;
                    if r.new_g < fwd_g[ti] {
                        fwd_g[ti]  = r.new_g;
                        fwd_cf[ti] = r.from_idx;
                        if !fwd_closed[ti] {
                            fwd_open.push(mk_fwd(r.to_idx, r.new_g));
                        }
                        let bg = bwd_g[ti];
                        if bg != SENTINEL {
                            let total = r.new_g + bg;
                            if total < mu { mu = total; best_meeting = Some(r.to_idx); }
                        }
                    }
                }
            } else {
                // ── Backward expansion ───────────────────────────────────────
                batch.clear();
                while batch.len() < BATCH_SIZE as usize {
                    let Some(node) = bwd_open.pop() else { break };
                    if node.g >= mu                        { continue; }
                    if bwd_closed[node.idx as usize]       { continue; }
                    if node.g > bwd_g[node.idx as usize]   { continue; }
                    bwd_closed[node.idx as usize] = true;
                    let fg = fwd_g[node.idx as usize];
                    if fg != SENTINEL {
                        let total = fg + node.g;
                        if total < mu { mu = total; best_meeting = Some(node.idx); }
                    }
                    batch.push((node.idx, node.g));
                }
                if batch.is_empty() { continue; }

                // Backward search treats src as its terminal node for direct-reach.
                let relax = self.dispatch_frontier(
                    graph, &batch, mu, jump_range, src_idx, src_pos, &ds,
                )?;

                for r in &relax {
                    let ti = r.to_idx as usize;
                    if r.new_g < bwd_g[ti] {
                        bwd_g[ti]  = r.new_g;
                        bwd_cf[ti] = r.from_idx;
                        if !bwd_closed[ti] {
                            bwd_open.push(mk_bwd(r.to_idx, r.new_g));
                        }
                        let fg = fwd_g[ti];
                        if fg != SENTINEL {
                            let total = fg + r.new_g;
                            if total < mu { mu = total; best_meeting = Some(r.to_idx); }
                        }
                    }
                }
            }
        }

        if mu >= greedy_jumps { return None; } // didn't beat greedy

        let m = best_meeting?;

        // fwd: m → fwd_seed → (prepend greedy bridge up to src if seeded mid-route)
        let mut fwd_path = vec![m];
        let mut cur = m;
        while cur != fwd_seed_idx {
            let prev = fwd_cf[cur as usize];
            if prev == SENTINEL { return None; }
            cur = prev;
            fwd_path.push(cur);
        }
        fwd_path.reverse();

        // bwd: m → bwd_seed
        let mut bwd_path: Vec<u32> = Vec::new();
        let mut cur = m;
        while cur != bwd_seed_idx {
            let prev = bwd_cf[cur as usize];
            if prev == SENTINEL { break; }
            cur = prev;
            bwd_path.push(cur);
        }
        if bwd_path.last().copied().unwrap_or(m) != bwd_seed_idx {
            bwd_path.push(bwd_seed_idx);
        }
        fwd_path.extend(bwd_path);

        // Map back to id64
        let mid_ids: Vec<i64> = fwd_path.iter()
            .map(|&i| graph.idx_to_id[i as usize])
            .collect();

        Some(mid_ids)
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Same hash as the GLSL — must stay identical.
#[inline(always)]
pub fn hash3_cpu(cx: i32, cy: i32, cz: i32) -> u32 {
    (cx as u32).wrapping_mul(2654435761)
    ^ (cy as u32).wrapping_mul(805459861)
    ^ (cz as u32).wrapping_mul(3266489917)
}

/// Path reconstruction using flat came_from array.
fn reconstruct_path_flat(
    came_from: &[u32],
    src_idx:   u32,
    dst_idx:   u32,
    idx_to_id: &[i64],
) -> Option<Vec<i64>> {
    let mut path_idx = vec![dst_idx];
    let mut cur = dst_idx;
    while cur != src_idx {
        let prev = came_from[cur as usize];
        if prev == SENTINEL { return None; }
        cur = prev;
        path_idx.push(cur);
    }
    path_idx.reverse();
    Some(path_idx.iter().map(|&i| idx_to_id[i as usize]).collect())
}

// ─────────────────────────────────────────────────────────────────────────────
// INTEGRATION GUIDE (what to change in existing files)
// ─────────────────────────────────────────────────────────────────────────────
//
// state.rs — add field:
//   pub vulkan_astar: Option<Arc<VulkanAstar>>,
//
// main.rs / mod.rs — init:
//   let vulkan_astar = VulkanAstar::init();
//   // Warn if None — routes will fall back to CPU automatically.
//
// carrier_route.rs — inside the `if use_astar { ... }` block,
// replace the BinaryHeap expansion loop (keep the greedy phase and fuel sim):
//
//   // Build node list for GPU (same systems already in node_pos/node_name)
//   let nodes_f32: Vec<(i64, f32, f32, f32)> = all_systems.iter()
//       .map(|&(id, _, nx, ny, nz)| (id, nx as f32, ny as f32, nz as f32))
//       .collect();
//
//   if let Some(ref vk) = state.vulkan_astar {
//       if let Some(graph) = vk.build_graph(&nodes_f32, cell_size as f32) {
//           let gpu_path = if total_distance > 5_000.0 {
//               vk.run_bidirectional(
//                   &graph, src_id, dest_id,
//                   src_id, dest_id, 0, 0,  // no bridge seeding for carrier
//                   CARRIER_JUMP_RANGE as f32, greedy_jumps as u32,
//                   CARRIER_REFINE_BUDGET_MS,
//               )
//           } else {
//               vk.run_unidirectional(
//                   &graph, src_id, dest_id,
//                   CARRIER_JUMP_RANGE as f32, greedy_jumps as u32,
//                   CARRIER_REFINE_BUDGET_MS,
//               )
//           };
//           if let Some(ids) = gpu_path {
//               astar_path = Some(ids.iter().map(|&id| {
//                   let name = node_name.get(&id).cloned().unwrap_or_default();
//                   let (nx, ny, nz) = node_pos.get(&id).copied().unwrap_or((0.,0.,0.));
//                   (id, name, nx, ny, nz)
//               }).collect());
//           }
//       }
//   }
//   // If GPU returned None, the existing CPU BinaryHeap loop runs as fallback.
//
// neutron_route.rs — same pattern.  For bidirectional case pass:
//   fwd_seed_id = fwd_seed.0, fwd_seed_g = fwd_seed.1
//   bwd_seed_id = bwd_seed.0, bwd_seed_g = bwd_seed.1
// Then prepend/append the greedy bridge hops exactly as the CPU path already does.
// ─────────────────────────────────────────────────────────────────────────────
