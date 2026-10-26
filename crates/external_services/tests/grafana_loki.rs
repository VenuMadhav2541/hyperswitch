use external_services::metrics_provider::{
    grafana::{
        loki::{GrafanaLoki, LokiMetricsQuery, LokiQueryMode},
        GrafanaConfig,
    },
    MetricRange, MetricsError, MetricsProvider,
};
use hyperswitch_masking::Secret;
use serde_json::json;
use time::OffsetDateTime;
use wiremock::{
    matchers::{body_partial_json, header, method, path},
    Mock, MockServer, ResponseTemplate,
};

fn range() -> MetricRange {
    MetricRange {
        from: OffsetDateTime::from_unix_timestamp(1_000)
            .unwrap()
            .date()
            .with_time(OffsetDateTime::from_unix_timestamp(1_000).unwrap().time()),
        to: OffsetDateTime::from_unix_timestamp(1_300)
            .unwrap()
            .date()
            .with_time(OffsetDateTime::from_unix_timestamp(1_300).unwrap().time()),
    }
}

#[tokio::test]
async fn instant_loki_query_preserves_each_instance_and_context() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/ds/query"))
        .and(header("authorization", "Bearer test-token"))
        .and(body_partial_json(json!({"from":"1000000", "to":"1300000", "queries":[{
            "refId":"A", "datasource":{"type":"loki","uid":"sandbox-loki"},
            "expr":"sum by (flow) (count_over_time({app=\"test\"}[5m]))", "queryType":"instant"
        }]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results":{"A":{"status":200,"frames":[
            {"schema":{"fields":[{"name":"Time","type":"time"},{"name":"Value","type":"number","labels":{"flow":"Insert","event":"db_failed"}}]},"data":{"values":[[1300000],[2.0]]}},
            {"schema":{"fields":[{"name":"Value","type":"number","labels":{"flow":"Publish","event":"kafka_failed"}},{"name":"Time","type":"time"}]},"data":{"values":[[3.0],[1300000]]}}
        ]}}}))).expect(1).mount(&server).await;
    let provider = GrafanaLoki::new(
        GrafanaConfig {
            base_url: server.uri(),
            api_token: Secret::new("test-token".into()),
            auth_type: Default::default(),
        },
        Default::default(),
    );
    let request = GrafanaLoki::build_request(
        LokiMetricsQuery {
            datasource_uid: "sandbox-loki".into(),
            expression: "sum by (flow) (count_over_time({app=\"test\"}[5m]))".into(),
            mode: LokiQueryMode::Instant,
            step_seconds: None,
        },
        range(),
    );
    let series = provider.fetch_metrics(request).await.unwrap();
    assert_eq!(series.len(), 2);
    assert_eq!(
        series[0].labels.get("flow").map(String::as_str),
        Some("Insert")
    );
    assert_eq!(series[0].points[0].value, 2.0);
    assert_eq!(
        series[1].labels.get("event").map(String::as_str),
        Some("kafka_failed")
    );
    assert_eq!(series[1].points[0].value, 3.0);
}

#[tokio::test]
async fn range_query_uses_a_duration_string_for_step() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"queries":[{
            "queryType":"range", "step":"10s"
        }]})))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"results":{"A":{"status":200,"frames":[]}}})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let provider = GrafanaLoki::new(
        GrafanaConfig {
            base_url: server.uri(),
            api_token: Secret::new("test-token".into()),
            auth_type: Default::default(),
        },
        Default::default(),
    );
    let request = GrafanaLoki::build_request(
        LokiMetricsQuery {
            datasource_uid: "loki".into(),
            expression: "vector(1)".into(),
            mode: LokiQueryMode::Range,
            step_seconds: Some(10),
        },
        range(),
    );
    assert!(provider.fetch_metrics(request).await.unwrap().is_empty());
}

#[tokio::test]
async fn unauthorized_is_not_reported_as_healthy_empty_data() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"message":"Unauthorized"})))
        .mount(&server)
        .await;
    let provider = GrafanaLoki::new(
        GrafanaConfig {
            base_url: server.uri(),
            api_token: Secret::new("test-token".into()),
            auth_type: Default::default(),
        },
        Default::default(),
    );
    let request = GrafanaLoki::build_request(
        LokiMetricsQuery {
            datasource_uid: "loki".into(),
            expression: "vector(1)".into(),
            mode: LokiQueryMode::Instant,
            step_seconds: None,
        },
        range(),
    );
    let error = provider.fetch_metrics(request).await.unwrap_err();
    assert!(matches!(
        error.current_context(),
        MetricsError::Unauthorized
    ));
    assert!(!format!("{error:?}").contains("test-token"));
}

#[tokio::test]
async fn partial_results_and_wrong_query_ref_fail_closed() {
    for response in [
        json!({"results":{"A":{"status":206,"frames":[]}}}),
        json!({"results":{"B":{"status":200,"frames":[]}}}),
        json!({"results":{"A":{"status":200,"error":"failed test-token query","frames":[]}}}),
        json!({"results":{"A":{"status":200,"frames":[{"schema":{"fields":[{"name":"Time","type":"time"},{"name":"Value","type":"number","labels":{}}]},"data":{"values":[[1300000,1301000],[1]]}}]}}}),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(&server)
            .await;
        let provider = GrafanaLoki::new(
            GrafanaConfig {
                base_url: server.uri(),
                api_token: Secret::new("test-token".into()),
                auth_type: Default::default(),
            },
            Default::default(),
        );
        let request = GrafanaLoki::build_request(
            LokiMetricsQuery {
                datasource_uid: "loki".into(),
                expression: "vector(1)".into(),
                mode: LokiQueryMode::Instant,
                step_seconds: None,
            },
            range(),
        );
        let error = provider.fetch_metrics(request).await.unwrap_err();
        assert!(!format!("{error:?}").contains("test-token"));
    }
}
