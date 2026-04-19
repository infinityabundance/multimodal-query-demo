//! `mqd figs` — render PNGs from a bench JSON report. One PNG per suite: the paper
//! includes them with `\includegraphics`. Each figure shows p50 (solid) and p95
//! (dashed) lines from the same `BenchReport` rows. Proximity additionally overlays
//! an $O(N^2)$ reference curve anchored at the smallest measured point, so the
//! quadratic-by-design shape reads directly off the figure.
//!
//! Axis choice: proximity and speed use log-log (the former to turn $y=cN^2$ into
//! a slope-2 line, the latter because tick counts span a decade); the remaining
//! three suites use linear-linear since they carry only three scale points each.

use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use mqd_core::StatusReporter;
use plotters::prelude::*;

use crate::cli::FigsArgs;
use crate::commands::bench::{BenchReport, BenchRun};

pub async fn run(args: FigsArgs, reporter: Arc<dyn StatusReporter>) -> Result<()> {
    reporter.phase("figs");
    let raw = std::fs::read_to_string(&args.from)
        .with_context(|| format!("reading bench report {}", args.from.display()))?;
    let report: BenchReport = serde_json::from_str(&raw).context("parsing bench report")?;

    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("creating output dir {}", args.out.display()))?;

    let samples = report.runs.first().map(|r| r.samples).unwrap_or(0);

    let mut rendered: usize = 0;
    for suite in ["ingest", "latest_at", "range_scan", "proximity", "speed"] {
        let series: Vec<&BenchRun> = report.runs.iter().filter(|r| r.suite == suite).collect();
        if series.is_empty() {
            continue;
        }
        let path = args.out.join(format!("{suite}_latency.png"));
        render(&path, suite, &series, samples)?;
        reporter.info("rendered", &path.display() as &dyn Display);
        rendered += 1;
    }
    reporter.info("figures", &rendered as &dyn Display);
    reporter.done("figs");
    Ok(())
}

struct ChartSpec {
    title: &'static str,
    x_label: &'static str,
    log_x: bool,
    log_y: bool,
    reference: Option<Reference>,
}

struct Reference {
    label: &'static str,
    // f(x) given the anchor point (x0, y0).
    eval: fn(f64, f64, f64) -> f64,
}

fn spec_for(suite: &str) -> ChartSpec {
    match suite {
        "ingest" => ChartSpec {
            title: "ingest: latency (µs) vs total events",
            x_label: "events",
            log_x: false,
            log_y: false,
            reference: None,
        },
        "latest_at" => ChartSpec {
            title: "latest_at: latency (µs) vs agents",
            x_label: "agents",
            log_x: false,
            log_y: false,
            reference: None,
        },
        "range_scan" => ChartSpec {
            title: "range_scan: latency (µs) vs agents (25% window, one entity)",
            x_label: "agents",
            log_x: false,
            log_y: false,
            reference: None,
        },
        "proximity" => ChartSpec {
            title: "proximity_pairs: latency (µs) vs entities (log-log)",
            x_label: "entities (N)",
            log_x: true,
            log_y: true,
            reference: Some(Reference {
                label: "O(N²) reference",
                eval: |x, x0, y0| y0 * (x / x0).powi(2),
            }),
        },
        "speed" => ChartSpec {
            title: "speed_over_ground: latency (µs) vs samples (log-log)",
            x_label: "samples (ticks)",
            log_x: true,
            log_y: true,
            reference: Some(Reference {
                label: "O(N) reference",
                eval: |x, x0, y0| y0 * (x / x0),
            }),
        },
        _ => unreachable!("unknown bench suite: {suite}"),
    }
}

fn render(path: &Path, suite: &str, runs: &[&BenchRun], samples: usize) -> Result<()> {
    let spec = spec_for(suite);
    let p50: Vec<(f64, f64)> = runs.iter().map(|r| (r.x, r.p50_us)).collect();
    let p95: Vec<(f64, f64)> = runs.iter().map(|r| (r.x, r.p95_us)).collect();

    let root = BitMapBackend::new(path, (900, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let (xs_lo, xs_hi) = extent(p50.iter().chain(p95.iter()).map(|(x, _)| *x));
    // Reference curve extends across the x range too — include it in y extent so it
    // isn't clipped by the frame.
    let ref_ys: Vec<f64> = if let Some(r) = &spec.reference {
        let (x0, y0) = (p50[0].0, p50[0].1);
        p50.iter().map(|(x, _)| (r.eval)(*x, x0, y0)).collect()
    } else {
        Vec::new()
    };
    let (ys_lo, ys_hi) = extent(
        p50.iter()
            .chain(p95.iter())
            .map(|(_, y)| *y)
            .chain(ref_ys.iter().copied()),
    );

    // Log scales pad by a decade factor; linear scales by ±10%.
    let (x_lo, x_hi) = if spec.log_x {
        (xs_lo.max(1.0) * 0.9, xs_hi * 1.1)
    } else {
        let pad = (xs_hi - xs_lo).max(1.0) * 0.05;
        ((xs_lo - pad).max(0.0), xs_hi + pad)
    };
    let (y_lo, y_hi) = if spec.log_y {
        (ys_lo.max(0.1) * 0.8, ys_hi * 1.25)
    } else {
        let pad = (ys_hi - ys_lo).max(1.0) * 0.1;
        ((ys_lo - pad).max(0.0), ys_hi + pad)
    };

    let caveat = format!("single-machine, synthetic, n={samples} samples; p50 solid, p95 dashed");

    let (chart_area, footer) = root.split_vertically(560);
    chart_area.fill(&WHITE)?;
    footer.fill(&WHITE)?;
    footer.draw_text(
        &caveat,
        &("sans-serif", 14).into_font().color(&RGBColor(90, 90, 90)),
        (20, 12),
    )?;

    // The four (log-x, log-y) combinations each need their own ChartBuilder call
    // because plotters' Cartesian types differ; the three series drawn inside are
    // identical, factored into `draw_series_set`.
    match (spec.log_x, spec.log_y) {
        (true, true) => {
            let mut chart = ChartBuilder::on(&chart_area)
                .caption(spec.title, ("sans-serif", 22))
                .margin(20)
                .x_label_area_size(45)
                .y_label_area_size(60)
                .build_cartesian_2d((x_lo..x_hi).log_scale(), (y_lo..y_hi).log_scale())?;
            chart
                .configure_mesh()
                .x_desc(spec.x_label)
                .y_desc("latency (µs)")
                .draw()?;
            draw_series_set(&mut chart, &p50, &p95, spec.reference.as_ref(), &p50)?;
            chart
                .configure_series_labels()
                .background_style(WHITE.mix(0.85))
                .border_style(BLACK)
                .position(SeriesLabelPosition::UpperLeft)
                .draw()?;
        }
        (true, false) => {
            let mut chart = ChartBuilder::on(&chart_area)
                .caption(spec.title, ("sans-serif", 22))
                .margin(20)
                .x_label_area_size(45)
                .y_label_area_size(60)
                .build_cartesian_2d((x_lo..x_hi).log_scale(), y_lo..y_hi)?;
            chart
                .configure_mesh()
                .x_desc(spec.x_label)
                .y_desc("latency (µs)")
                .draw()?;
            draw_series_set(&mut chart, &p50, &p95, spec.reference.as_ref(), &p50)?;
            chart
                .configure_series_labels()
                .background_style(WHITE.mix(0.85))
                .border_style(BLACK)
                .position(SeriesLabelPosition::UpperLeft)
                .draw()?;
        }
        (false, _) => {
            let mut chart = ChartBuilder::on(&chart_area)
                .caption(spec.title, ("sans-serif", 22))
                .margin(20)
                .x_label_area_size(45)
                .y_label_area_size(60)
                .build_cartesian_2d(x_lo..x_hi, y_lo..y_hi)?;
            chart
                .configure_mesh()
                .x_desc(spec.x_label)
                .y_desc("latency (µs)")
                .draw()?;
            draw_series_set(&mut chart, &p50, &p95, spec.reference.as_ref(), &p50)?;
            chart
                .configure_series_labels()
                .background_style(WHITE.mix(0.85))
                .border_style(BLACK)
                .position(SeriesLabelPosition::UpperLeft)
                .draw()?;
        }
    }
    root.present()?;
    Ok(())
}

fn draw_series_set<DB, X, Y>(
    chart: &mut ChartContext<'_, DB, Cartesian2d<X, Y>>,
    p50: &[(f64, f64)],
    p95: &[(f64, f64)],
    reference: Option<&Reference>,
    anchor_for_ref: &[(f64, f64)],
) -> Result<()>
where
    DB: DrawingBackend,
    DB::ErrorType: 'static,
    X: plotters::coord::ranged1d::Ranged<ValueType = f64>,
    Y: plotters::coord::ranged1d::Ranged<ValueType = f64>,
{
    // p50 solid line with circle markers.
    chart
        .draw_series(LineSeries::new(p50.iter().copied(), RED.stroke_width(2)))?
        .label("p50")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], RED.stroke_width(2)));
    chart.draw_series(
        p50.iter()
            .map(|(x, y)| Circle::new((*x, *y), 4, RED.filled())),
    )?;

    // p95 dashed.
    let p95_style = RGBColor(200, 100, 100).mix(0.7).stroke_width(1);
    chart
        .draw_series(DashedLineSeries::new(p95.iter().copied(), 8, 4, p95_style))?
        .label("p95")
        .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], p95_style));
    chart.draw_series(p95.iter().map(|(x, y)| {
        Circle::new(
            (*x, *y),
            3,
            RGBColor(200, 100, 100).mix(0.7).stroke_width(1),
        )
    }))?;

    // Reference curve (only proximity / speed).
    if let Some(r) = reference {
        let (x0, y0) = anchor_for_ref[0];
        let ref_points: Vec<(f64, f64)> = anchor_for_ref
            .iter()
            .map(|(x, _)| (*x, (r.eval)(*x, x0, y0)))
            .collect();
        let ref_style = RGBColor(120, 120, 120).mix(0.6).stroke_width(1);
        chart
            .draw_series(DashedLineSeries::new(
                ref_points.into_iter(),
                12,
                6,
                ref_style,
            ))?
            .label(r.label)
            .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], ref_style));
    }
    Ok(())
}

fn extent(it: impl Iterator<Item = f64>) -> (f64, f64) {
    let mut it = it.peekable();
    let first = *it.peek().expect("non-empty series");
    let mut lo = first;
    let mut hi = first;
    for v in it {
        if v < lo {
            lo = v;
        }
        if v > hi {
            hi = v;
        }
    }
    if hi == lo {
        hi = lo + 1.0;
    }
    (lo, hi)
}
