use axum::{
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use std::{
    sync::{Arc, atomic::{AtomicU64, Ordering as AtomicOrdering}},
    time::{Duration, Instant},
};

use crate::config::*;
use crate::state::AppState;

/// Live activity counter grid.
pub struct Heatmap {
    cells: Vec<AtomicU64>,
    /// Holds the last rendered PNG and the time it was rendered.
    /// The lock is held for the entire render to prevent simultaneous renders
    /// under concurrent requests (TOCTOU would let N threads all render at once).
    cached_png: std::sync::Mutex<Option<(Instant, Arc<Vec<u8>>)>>,
    pub total_bumps: AtomicU64,
}

impl Heatmap {
    pub fn new() -> Self {
        let cells = (0..HEATMAP_W * HEATMAP_H)
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            cells,
            cached_png: std::sync::Mutex::new(None),
            total_bumps: AtomicU64::new(0),
        }
    }

    pub fn bump(&self, x: f64, z: f64) {
        if !x.is_finite() || !z.is_finite() { return; }
        if x < HEATMAP_X_MIN || x >= HEATMAP_X_MAX { return; }
        if z < HEATMAP_Z_MIN || z >= HEATMAP_Z_MAX { return; }
        let gx = ((x - HEATMAP_X_MIN) / (HEATMAP_X_MAX - HEATMAP_X_MIN) * HEATMAP_W as f64) as usize;
        let gz = ((z - HEATMAP_Z_MIN) / (HEATMAP_Z_MAX - HEATMAP_Z_MIN) * HEATMAP_H as f64) as usize;
        let idx = gz.min(HEATMAP_H - 1) * HEATMAP_W + gx.min(HEATMAP_W - 1);
        self.cells[idx].fetch_add(1, AtomicOrdering::Relaxed);
        self.total_bumps.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn decay(&self, factor: f64) {
        for cell in &self.cells {
            let v = cell.load(AtomicOrdering::Relaxed);
            if v > 0 {
                cell.store(((v as f64) * factor) as u64, AtomicOrdering::Relaxed);
            }
        }
    }

    fn render_png(&self) -> Vec<u8> {
        let snap: Vec<u64> = self.cells.iter()
            .map(|c| c.load(AtomicOrdering::Relaxed))
            .collect();
        let max = snap.iter().copied().max().unwrap_or(0);
        let log_max = ((max + 1) as f64).ln().max(1.0);

        let mut rgba = vec![0u8; HEATMAP_W * HEATMAP_H * 4];
        for (i, &count) in snap.iter().enumerate() {
            if count == 0 { continue; }
            let intensity = ((count + 1) as f64).ln() / log_max;
            let (r, g, b, a) = colormap_inferno(intensity);
            let gx = i % HEATMAP_W;
            let gz = i / HEATMAP_W;
            // Flip Y: gz=0 is the south pole in galaxy coords, but row 0 is top of image.
            let pi = ((HEATMAP_H - 1 - gz) * HEATMAP_W + gx) * 4;
            rgba[pi]     = r;
            rgba[pi + 1] = g;
            rgba[pi + 2] = b;
            rgba[pi + 3] = a;
        }

        let mut out = Vec::with_capacity(64 * 1024);
        let mut encoder = png::Encoder::new(&mut out, HEATMAP_W as u32, HEATMAP_H as u32);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_compression(png::Compression::Fast);
        encoder.write_header()
            .expect("png header write")
            .write_image_data(&rgba)
            .expect("png image write");
        out
    }

    /// Returns a cached PNG, re-rendering only when the cache has expired.
    ///
    /// The mutex is held across the render to avoid the TOCTOU window where
    /// two threads both see a stale cache entry and both kick off a render.
    pub fn get_or_render(&self) -> Arc<Vec<u8>> {
        let mut guard = self.cached_png.lock().unwrap();
        if let Some((t, ref bytes)) = *guard {
            if t.elapsed() < Duration::from_secs(HEATMAP_RENDER_CACHE_SECS) {
                return bytes.clone();
            }
        }
        let bytes = Arc::new(self.render_png());
        *guard = Some((Instant::now(), bytes.clone()));
        bytes
    }
}

/// Matplotlib "inferno" colormap, sampled from the canonical 256-entry LUT.
/// Returns (R, G, B, A) where alpha scales from ~60 (cold) to 255 (hot).
fn colormap_inferno(t: f64) -> (u8, u8, u8, u8) {
    // 9 evenly-spaced samples from the official inferno LUT (perceptually uniform).
    const STOPS: &[[f64; 4]] = &[
        [0.000,   0.0,   0.0,   4.0],
        [0.125,  40.0,  11.0,  84.0],
        [0.250,  96.0,  19.0, 110.0],
        [0.375, 159.0,  42.0,  99.0],
        [0.500, 212.0,  72.0,  66.0],
        [0.625, 245.0, 125.0,  21.0],
        [0.750, 250.0, 193.0,  39.0],
        [0.875, 252.0, 255.0, 164.0],
        [1.000, 252.0, 255.0, 164.0], // sentinel: clamp at top
    ];

    let t = t.clamp(0.0, 1.0);
    // Binary-search for the segment containing t.
    let seg = STOPS.partition_point(|s| s[0] <= t).saturating_sub(1).min(STOPS.len() - 2);
    let [t0, r0, g0, b0] = STOPS[seg];
    let [t1, r1, g1, b1] = STOPS[seg + 1];
    let f = if t1 > t0 { (t - t0) / (t1 - t0) } else { 0.0 };
    let lerp = |a: f64, b: f64| (a + (b - a) * f) as u8;
    let alpha = (60.0 + t * 195.0) as u8;
    (lerp(r0, r1), lerp(g0, g1), lerp(b0, b1), alpha)
}

pub fn heatmap_decay_thread(heatmap: Arc<Heatmap>) {
    loop {
        std::thread::sleep(Duration::from_secs(HEATMAP_DECAY_INTERVAL_SECS));
        // Catch panics so a transient error doesn't silently kill the decay loop.
        if let Err(e) = std::panic::catch_unwind(|| heatmap.decay(HEATMAP_DECAY_FACTOR)) {
            eprintln!("heatmap decay panic (continuing): {:?}", e);
        }
    }
}

pub async fn heatmap_png_handler(
    State(state): State<Arc<AppState>>,
) -> Response {
    let heatmap = state.heatmap.clone();
    match tokio::task::spawn_blocking(move || heatmap.get_or_render()).await {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "image/png")
            .header(header::CACHE_CONTROL, "public, max-age=30")
            // Serve directly from the Arc — no clone of the PNG buffer.
            .body(Body::from(bytes.as_ref().clone()))
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("render task failed: {e}")))
            .unwrap(),
    }
}

/// Compute the CSS `left` / `top` percentage for a galaxy-coordinate marker.
/// x is the galactic X axis (left→right), z is the galactic Z axis (bottom→top).
fn marker_pct(x: f64, z: f64) -> (f64, f64) {
    let left = (x - HEATMAP_X_MIN) / (HEATMAP_X_MAX - HEATMAP_X_MIN) * 100.0;
    // Image row 0 is the *top* of the canvas (high Z), so we invert.
    let top  = (1.0 - (z - HEATMAP_Z_MIN) / (HEATMAP_Z_MAX - HEATMAP_Z_MIN)) * 100.0;
    (left, top)
}

pub async fn heatmap_html_handler() -> impl IntoResponse {
    Html(build_heatmap_html())
}

/// Build the HTML page at runtime so marker positions derive from config constants.
fn build_heatmap_html() -> String {
    // Known landmark coordinates (galactic X, Z).
    let landmarks: &[(&str, f64, f64)] = &[
        ("Sol",        0.0,         0.0),
        ("Sgr A*",     25.21,    -20.90),
        ("Colonia",  -9530.5,   -910.28),
        ("Beagle Pt", 1111.56, 65269.15),
    ];

    let markers: String = landmarks.iter().map(|(name, x, z)| {
        let (l, t) = marker_pct(*x, *z);
        format!(
            r#"<div class="marker" style="left:{l:.2}%;top:{t:.2}%">{name}</div>"#,
            l = l, t = t, name = name
        )
    }).collect::<Vec<_>>().join("\n  ");

    format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ananke · Commander Hotspots</title>
<style>
  html,body{{margin:0;padding:0;background:#04050a;color:#dde3ee;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;overflow:hidden}}
  .wrap{{position:relative;width:100vmin;height:100vmin;margin:0 auto}}
  .bg{{position:absolute;inset:0;
    background:
      radial-gradient(ellipse 60% 35% at 50% 49%, rgba(80,60,140,.18) 0%, rgba(40,20,70,.05) 60%, transparent 100%),
      radial-gradient(ellipse 80% 60% at 50% 49%, rgba(40,30,80,.10) 0%, transparent 70%),
      #04050a;
  }}
  .stars{{position:absolute;inset:0;
    background-image:
      radial-gradient(1px 1px at 13% 22%,#fffa 0,transparent 100%),
      radial-gradient(1px 1px at 81% 14%,#fff8 0,transparent 100%),
      radial-gradient(1px 1px at 47% 62%,#fff7 0,transparent 100%),
      radial-gradient(1px 1px at 28% 78%,#fff6 0,transparent 100%),
      radial-gradient(1px 1px at 71% 91%,#fff5 0,transparent 100%),
      radial-gradient(1px 1px at 92% 51%,#fff4 0,transparent 100%),
      radial-gradient(1px 1px at 6% 53%,#fff3 0,transparent 100%);
    background-size:240px 240px,200px 200px,260px 260px,180px 180px,220px 220px,300px 300px,280px 280px;
    opacity:.55
  }}
  .heat{{position:absolute;inset:0;width:100%;height:100%;mix-blend-mode:screen;image-rendering:auto}}
  .marker{{position:absolute;font-size:11px;color:#9fdcc0;letter-spacing:.6px;transform:translate(-50%,-50%);pointer-events:none;text-shadow:0 0 6px #000,0 0 2px #000;white-space:nowrap}}
  .marker::before{{content:"";display:inline-block;width:5px;height:5px;border-radius:50%;background:#9fdcc0;margin-right:5px;box-shadow:0 0 8px #9fdcc0;vertical-align:1px}}
  h1{{position:fixed;top:14px;left:18px;margin:0;font-size:13px;font-weight:500;color:#9fdcc0;letter-spacing:1.5px}}
  .meta{{position:fixed;top:34px;left:18px;font-size:10px;color:#5a6675;letter-spacing:.5px}}
  .legend{{position:fixed;bottom:14px;left:14px;background:rgba(8,10,16,.65);padding:10px 14px;border-radius:4px;font-size:11px;line-height:1.5;border:1px solid #1a2030}}
  .bar{{width:200px;height:8px;background:linear-gradient(to right,rgba(20,8,60,.3),#3c1e78,#a01e6e,#e65a32,#ffc83c,#fff8dc);margin:6px 0;border-radius:1px}}
  .bar-l{{display:flex;justify-content:space-between;color:#7a8696;font-size:9px;letter-spacing:.5px}}
  .links{{position:fixed;bottom:14px;right:14px;font-size:10px;color:#5a6675}}
  .links a{{color:#7a8696;text-decoration:none;margin-left:10px}}
  .links a:hover{{color:#9fdcc0}}
</style>
</head>
<body>
<h1>ANANKE · COMMANDER HOTSPOTS</h1>
<div class="meta">live activity · log scale · ~70 min half-life</div>
<div class="wrap">
  <div class="bg"></div>
  <div class="stars"></div>
  <img class="heat" id="heat" src="/api/heatmap.png" alt="commander activity heatmap">
  {markers}
</div>
<div class="legend">
  <div style="color:#9fdcc0;letter-spacing:.5px">ACTIVITY</div>
  <div class="bar"></div>
  <div class="bar-l"><span>cold</span><span>hot</span></div>
  <div style="margin-top:6px;color:#5a6675">100,000 LY across · refresh 30s</div>
</div>
<div class="links">
  <a href="/api/heatmap.png">PNG</a>
  <a href="/api/edmc/stats">stats</a>
</div>
<script>
  const img = document.getElementById('heat');
  setInterval(() => {{ img.src = `/api/heatmap.png?t=${{Date.now()}}`; }}, 30_000);
</script>
</body>
</html>"#, markers = markers)
}
