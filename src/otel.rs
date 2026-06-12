use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;

struct HeaderMapCarrier<'a>(&'a mut http::HeaderMap);

impl<'a> opentelemetry::propagation::Injector for HeaderMapCarrier<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (
            http::HeaderName::from_bytes(key.as_bytes()),
            http::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, val);
        }
    }
}

struct HeaderMapCarrierRef<'a>(&'a http::HeaderMap);

impl<'a> opentelemetry::propagation::Extractor for HeaderMapCarrierRef<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

pub fn propagate_context(headers: &mut http::HeaderMap) {
    let parent_cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderMapCarrierRef(headers))
    });

    let span = tracing::Span::current();
    let cx = if span.id().is_none() {
        parent_cx
    } else {
        let _ = tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(&span, parent_cx);
        tracing_opentelemetry::OpenTelemetrySpanExt::context(&span)
    };

    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut HeaderMapCarrier(headers))
    });
}

pub fn init_otel(
    endpoint: &str,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, Box<dyn std::error::Error + Send + Sync>> {
    // Set global propagator to W3C TraceContext
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    let resource = Resource::builder().with_service_name("ethryx").build();

    // Build the OTLP Tracer Provider
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.clone())
        .build();

    global::set_tracer_provider(tracer_provider.clone());

    // Build the OTLP Meter Provider
    let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(meter_exporter).build();

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    global::set_meter_provider(meter_provider);

    Ok(tracer_provider)
}

// ---- Custom metrics::Recorder bridge for OpenTelemetry ----

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use opentelemetry::KeyValue;
use std::sync::Arc;

use std::collections::HashMap;
use std::sync::RwLock;

pub struct OtelRecorder {
    meter: opentelemetry::metrics::Meter,
    counters: RwLock<HashMap<Key, Counter>>,
    gauges: RwLock<HashMap<Key, Gauge>>,
    histograms: RwLock<HashMap<Key, Histogram>>,
}

impl OtelRecorder {
    pub fn new() -> Self {
        Self {
            meter: global::meter("ethryx"),
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }
}

fn convert_labels(key: &Key) -> Vec<KeyValue> {
    key.labels()
        .map(|l| KeyValue::new(l.key().to_owned(), l.value().to_owned()))
        .collect()
}

impl Recorder for OtelRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        {
            let read = self.counters.read().unwrap();
            if let Some(counter) = read.get(key) {
                return counter.clone();
            }
        }
        let mut write = self.counters.write().unwrap();
        if let Some(counter) = write.get(key) {
            return counter.clone();
        }
        let name = key.name().to_string();
        let counter = self.meter.u64_counter(name).build();
        let labels = convert_labels(key);
        let metrics_counter = Counter::from_arc(Arc::new(OtelCounter { counter, labels }));
        write.insert(key.clone(), metrics_counter.clone());
        metrics_counter
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        {
            let read = self.gauges.read().unwrap();
            if let Some(gauge) = read.get(key) {
                return gauge.clone();
            }
        }
        let mut write = self.gauges.write().unwrap();
        if let Some(gauge) = write.get(key) {
            return gauge.clone();
        }
        let name = key.name().to_string();
        let labels = convert_labels(key);
        let backend = if name == "ethryx_active_connections" {
            OtelGaugeBackend::UpDown(self.meter.f64_up_down_counter(name).build())
        } else {
            OtelGaugeBackend::Gauge(self.meter.f64_gauge(name).build())
        };
        let metrics_gauge = Gauge::from_arc(Arc::new(OtelGauge { backend, labels }));
        write.insert(key.clone(), metrics_gauge.clone());
        metrics_gauge
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        {
            let read = self.histograms.read().unwrap();
            if let Some(histogram) = read.get(key) {
                return histogram.clone();
            }
        }
        let mut write = self.histograms.write().unwrap();
        if let Some(histogram) = write.get(key) {
            return histogram.clone();
        }
        let name = key.name().to_string();
        let histogram = self.meter.f64_histogram(name).build();
        let labels = convert_labels(key);
        let metrics_histogram = Histogram::from_arc(Arc::new(OtelHistogram { histogram, labels }));
        write.insert(key.clone(), metrics_histogram.clone());
        metrics_histogram
    }
}

struct OtelCounter {
    counter: opentelemetry::metrics::Counter<u64>,
    labels: Vec<KeyValue>,
}

impl metrics::CounterFn for OtelCounter {
    fn increment(&self, value: u64) {
        self.counter.add(value, &self.labels);
    }

    fn absolute(&self, _value: u64) {}
}

enum OtelGaugeBackend {
    Gauge(opentelemetry::metrics::Gauge<f64>),
    UpDown(opentelemetry::metrics::UpDownCounter<f64>),
}

struct OtelGauge {
    backend: OtelGaugeBackend,
    labels: Vec<KeyValue>,
}

impl metrics::GaugeFn for OtelGauge {
    fn increment(&self, value: f64) {
        match &self.backend {
            OtelGaugeBackend::UpDown(ud) => ud.add(value, &self.labels),
            OtelGaugeBackend::Gauge(g) => g.record(value, &self.labels),
        }
    }

    fn decrement(&self, value: f64) {
        match &self.backend {
            OtelGaugeBackend::UpDown(ud) => ud.add(-value, &self.labels),
            OtelGaugeBackend::Gauge(g) => g.record(-value, &self.labels),
        }
    }

    fn set(&self, value: f64) {
        match &self.backend {
            OtelGaugeBackend::Gauge(g) => g.record(value, &self.labels),
            OtelGaugeBackend::UpDown(_) => {}
        }
    }
}

struct OtelHistogram {
    histogram: opentelemetry::metrics::Histogram<f64>,
    labels: Vec<KeyValue>,
}

impl metrics::HistogramFn for OtelHistogram {
    fn record(&self, value: f64) {
        self.histogram.record(value, &self.labels);
    }
}
