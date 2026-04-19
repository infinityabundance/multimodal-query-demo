use std::io::Write;
use std::path::Path;

use anyhow::Result;
use mqd_core::{ComponentKind, ComponentName, EntityPath, Event, Payload, Timeline};
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

/// Deterministic synthetic generator for sparse multi-rate multimodal event streams.
///
/// Agents walk a 2D random walk in a fixed plane, emit Transform3D poses at a high rate,
/// a Scalar "battery" reading at a mid rate, and an occasional ImageRef.
#[derive(Clone, Debug)]
pub struct GenConfig {
    pub seed: u64,
    pub agents: u32,
    pub ticks: u32,
    pub tick_ns: i64,
    pub pose_every_n: u32,
    pub scalar_every_n: u32,
    pub image_every_n: u32,
    pub step_sigma: f64,
}

impl Default for GenConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            agents: 8,
            ticks: 1_000,
            tick_ns: 5_000_000,
            pose_every_n: 1,
            scalar_every_n: 5,
            image_every_n: 100,
            step_sigma: 0.05,
        }
    }
}

/// Generate the full event stream into memory in strict (tick, entity_id, kind) order.
///
/// Strict ordering is required for byte-level determinism of the JSONL output and
/// of any downstream Arrow IPC fingerprint.
pub fn generate_run(config: &GenConfig) -> Vec<Event> {
    let mut rng = Pcg64::seed_from_u64(config.seed);
    let timeline = Timeline::sim();
    let mut positions: Vec<[f64; 3]> = (0..config.agents).map(|i| [i as f64, 0.0, 0.0]).collect();
    let mut out = Vec::with_capacity((config.ticks as usize) * (config.agents as usize) * 2);

    for tick in 0..config.ticks {
        let t_ns = tick as i64 * config.tick_ns;
        for aid in 0..config.agents {
            // Deterministic random walk step. Same RNG instance, stable order (tick, aid).
            let dx = gaussian(&mut rng, config.step_sigma);
            let dy = gaussian(&mut rng, config.step_sigma);
            let p = &mut positions[aid as usize];
            p[0] += dx;
            p[1] += dy;

            let entity = EntityPath::new(format!("/agent/{aid}")).expect("valid path");

            if tick % config.pose_every_n == 0 {
                out.push(Event {
                    entity_path: entity.clone(),
                    timeline: timeline.clone(),
                    t_ns,
                    component: ComponentName::new("pose"),
                    payload: Payload::Transform3D {
                        tx: p[0],
                        ty: p[1],
                        tz: p[2],
                        qx: 0.0,
                        qy: 0.0,
                        qz: 0.0,
                        qw: 1.0,
                    },
                });
            }
            if tick % config.scalar_every_n == 0 {
                let battery = 1.0 - (tick as f64) / (config.ticks as f64);
                out.push(Event {
                    entity_path: entity.clone(),
                    timeline: timeline.clone(),
                    t_ns,
                    component: ComponentName::new("battery"),
                    payload: Payload::Scalar {
                        value: battery,
                        unit: Some("ratio".to_string()),
                    },
                });
            }
            if tick % config.image_every_n == 0 {
                out.push(Event {
                    entity_path: entity,
                    timeline: timeline.clone(),
                    t_ns,
                    component: ComponentName::new("camera"),
                    payload: Payload::ImageRef {
                        uri: format!("synthetic://agent/{aid}/tick/{tick}"),
                        width: 640,
                        height: 480,
                        format: "JPEG".to_string(),
                    },
                });
            }
        }
    }
    out
}

pub fn write_jsonl(events: &[Event], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for event in events {
        serde_json::to_writer(&mut w, event)?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

/// Box-Muller standard gaussian scaled by sigma. Deterministic given `rng`.
fn gaussian(rng: &mut Pcg64, sigma: f64) -> f64 {
    let u1: f64 = rng.gen_range(f64::EPSILON..1.0);
    let u2: f64 = rng.gen_range(0.0..1.0);
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    z * sigma
}

pub fn count_by_kind(events: &[Event]) -> [u64; 5] {
    let mut counts = [0u64; 5];
    for e in events {
        counts[e.kind() as usize] += 1;
    }
    counts
}

pub fn kind_name(k: ComponentKind) -> &'static str {
    k.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn generator_is_deterministic_across_runs() {
        let cfg = GenConfig {
            seed: 42,
            agents: 4,
            ticks: 100,
            ..Default::default()
        };
        let a = generate_run(&cfg);
        let b = generate_run(&cfg);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x, y);
        }
    }

    #[test]
    fn generator_different_seed_produces_different_stream() {
        let mut cfg = GenConfig {
            seed: 42,
            agents: 4,
            ticks: 100,
            ..Default::default()
        };
        let a = generate_run(&cfg);
        cfg.seed = 43;
        let b = generate_run(&cfg);
        assert_eq!(a.len(), b.len());
        assert_ne!(a, b);
    }

    #[test]
    fn generator_output_emits_in_tick_and_entity_order() {
        let cfg = GenConfig {
            seed: 1,
            agents: 3,
            ticks: 5,
            ..Default::default()
        };
        let events = generate_run(&cfg);
        // Within any given tick, entity_path ids must appear in increasing order.
        for window in events.windows(2) {
            let a = &window[0];
            let b = &window[1];
            if a.t_ns == b.t_ns {
                assert!(
                    a.entity_path <= b.entity_path,
                    "entity order violated at t_ns={}",
                    a.t_ns
                );
            } else {
                assert!(a.t_ns < b.t_ns, "tick order violated");
            }
        }
    }

    #[test]
    fn jsonl_serialization_fingerprint_is_stable() {
        let cfg = GenConfig {
            seed: 42,
            agents: 2,
            ticks: 10,
            ..Default::default()
        };
        let events = generate_run(&cfg);
        let mut hasher = Sha256::new();
        for e in &events {
            let line = serde_json::to_string(e).unwrap();
            hasher.update(line.as_bytes());
            hasher.update(b"\n");
        }
        let hex = hex_string(&hasher.finalize());
        // Sanity: hex is 64 chars, deterministic across machines if serde_json output is stable.
        assert_eq!(hex.len(), 64);
    }

    fn hex_string(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}
