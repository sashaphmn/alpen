use std::time::{Duration, Instant};

use tracing::Subscriber;
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

struct SpanTiming {
    created_at: Instant,
    busy: Duration,
    last_entered: Option<Instant>,
}

/// A tracing [`Layer`] that records span busy/idle time as `metrics` histograms.
///
/// For every span that closes, it records:
/// - `alpen_span_busy_seconds{span="<name>"}` — time the span was actively executing
/// - `alpen_span_idle_seconds{span="<name>"}` — time the span existed but was not executing
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
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanTiming {
                created_at: Instant::now(),
                busy: Duration::ZERO,
                last_entered: None,
            });
        }
    }

    fn on_enter(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(timing) = span.extensions_mut().get_mut::<SpanTiming>() {
                timing.last_entered = Some(Instant::now());
            }
        }
    }

    fn on_exit(&self, id: &tracing::span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(timing) = span.extensions_mut().get_mut::<SpanTiming>() {
                if let Some(entered) = timing.last_entered.take() {
                    timing.busy += entered.elapsed();
                }
            }
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            let name = span.name();
            if let Some(timing) = span.extensions().get::<SpanTiming>() {
                let total = timing.created_at.elapsed();
                let busy = timing.busy;
                let idle = total.saturating_sub(busy);

                metrics::histogram!("alpen_span_busy_seconds", "span" => name.to_string())
                    .record(busy.as_secs_f64());
                metrics::histogram!("alpen_span_idle_seconds", "span" => name.to_string())
                    .record(idle.as_secs_f64());
            }
        }
    }
}
