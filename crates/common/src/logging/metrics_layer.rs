use std::time::{Duration, Instant};

use tracing::Subscriber;
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

struct SpanTiming {
    created_at: Instant,
    busy: Duration,
    last_entered: Instant,
}

/// A tracing [`Layer`] that records span busy/idle time as `metrics` histograms.
///
/// For every span that closes, it records:
/// - `strata_span_busy_us{span="<name>"}` — time the span was actively executing (microseconds)
/// - `strata_span_idle_us{span="<name>"}` — time the span existed but was not executing
///   (microseconds)
///
/// These are no-ops if no `metrics` recorder is installed.
#[derive(Debug)]
pub struct MetricsLayer;

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for MetricsLayer {
    fn on_new_span(
        &self,
        _attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: Context<'_, S>,
    ) {
        let now = Instant::now();
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanTiming {
                created_at: now,
                busy: Duration::ZERO,
                last_entered: now,
            });
        }
    }

    fn on_enter(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(timing) = span.extensions_mut().get_mut::<SpanTiming>() {
                timing.last_entered = Instant::now();
            }
        }
    }

    fn on_exit(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(timing) = span.extensions_mut().get_mut::<SpanTiming>() {
                timing.busy += timing.last_entered.elapsed();
            }
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            if let Some(timing) = span.extensions().get::<SpanTiming>() {
                let total = timing.created_at.elapsed();
                let busy = timing.busy;
                let idle = total.saturating_sub(busy);
                let name = span.name().to_string();

                metrics::histogram!("strata_span_busy_us", "span" => name.clone())
                    .record(busy.as_micros() as f64);
                metrics::histogram!("strata_span_idle_us", "span" => name)
                    .record(idle.as_micros() as f64);
            }
        }
    }
}
