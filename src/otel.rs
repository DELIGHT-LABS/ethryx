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
