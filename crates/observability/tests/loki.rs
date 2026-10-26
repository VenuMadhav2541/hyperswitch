use std::{collections::HashMap, sync::Arc};

use actix_web::{http::StatusCode, test, App};
use observability::{
    auth::X_INTERNAL_API_KEY,
    db::Store,
    domain::notifier::Registry,
    routes::Alerts,
    settings::{Database, Settings},
    state::AppState,
};
use serde_json::{json, Value};
use wiremock::{
    matchers::{body_partial_json, method, path},
    Mock, MockServer, ResponseTemplate,
};

async fn state(grafana: &MockServer) -> AppState {
    let conf = serde_json::from_value::<Settings<_>>(json!({
        "auth":{"internal_api_key":"local-test-key"},
        "chat":{"destinations":{"smoke":{"type":"log"}}},
        "alert":{
            "grafana":{"base_url":grafana.uri(),"api_token":"test-token"},
            "datasources":{"grafana_loki":{"uid":"loki"}},
            "rules":json!([{
                "id":"application-failure","name":"Application failure",
                "config":{
                    "interval_seconds":60,
                    "query":{"datasource":"grafana_loki","expression":"sum by (flow) (count_over_time({app=\"example\"}[$__range]))","mode":"instant","range_seconds":300,"reducer":"last"},
                    "thresholds":{"severities":{"critical":{"threshold":0,"comparison_operator":"GreaterThanThreshold","description":"Application failure","destination":"smoke"}}}
                }
            }]).to_string()
        }
    })).expect("Loki configuration must decode");
    let store = Store::new(&Database {
        username: "unused".into(),
        host: "127.0.0.1".into(),
        dbname: "unused".into(),
        min_idle_pool_size: 0,
        ..Default::default()
    })
    .await
    .unwrap();
    AppState {
        conf: Arc::new(conf),
        router_transport: None,
        chat: Arc::new(Registry::new(HashMap::new())),
        email: Arc::new(Registry::new(HashMap::new())),
        store: Arc::new(store),
    }
}

#[actix_web::test]
async fn notification_auth_and_limits_fail_before_querying_the_source() {
    let grafana = MockServer::start().await;
    let app = test::init_service(App::new().service(Alerts::server(state(&grafana).await))).await;
    let valid = json!({"rule_ids":["application-failure"],"max_messages":1});
    for key in [None, Some("wrong-key")] {
        let mut request = test::TestRequest::post()
            .uri("/alerts/grafana/loki/notify")
            .set_json(&valid);
        if let Some(key) = key {
            request = request.insert_header((X_INTERNAL_API_KEY, key));
        }
        assert_eq!(
            test::call_service(&app, request.to_request())
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for invalid in [
        json!({"rule_ids":[],"max_messages":1}),
        json!({"rule_ids":["application-failure"],"max_messages":21}),
    ] {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/alerts/grafana/loki/notify")
                .insert_header((X_INTERNAL_API_KEY, "local-test-key"))
                .set_json(invalid)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(grafana.received_requests().await.unwrap().is_empty());
}

#[actix_web::test]
async fn authenticated_loki_preview_preserves_instances_and_never_delivers() {
    let grafana = MockServer::start().await;
    let timestamp = time::OffsetDateTime::now_utc().unix_timestamp() * 1000;
    Mock::given(method("POST")).and(path("/api/ds/query"))
        .and(body_partial_json(json!({"queries":[{"expr":"sum by (flow) (count_over_time({app=\"example\"}[300s]))"}]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results":{"A":{"status":200,"frames":[
            {"schema":{"fields":[{"type":"time"},{"type":"number","labels":{"flow":"KafkaPublish","event":"publish_failed"}}]},"data":{"values":[[timestamp],[2]]}},
            {"schema":{"fields":[{"type":"time"},{"type":"number","labels":{"flow":"RedisTokenInsert","event":"insert_failed"}}]},"data":{"values":[[timestamp],[0]]}}
        ]}}}))).expect(1).mount(&grafana).await;
    let app = test::init_service(App::new().service(Alerts::server(state(&grafana).await))).await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/alerts/grafana/loki/evaluate")
            .insert_header((X_INTERNAL_API_KEY, "local-test-key"))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["rules"][0]["instances"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["rules"][0]["instances"][0]["labels"]["event"],
        "publish_failed"
    );
    assert_eq!(body["rules"][0]["instances"][0]["level"], "critical");
    assert_eq!(body["rules"][0]["instances"][1]["level"], "normal");
    assert_eq!(body["delivery_enabled"], false);
}

#[actix_web::test]
async fn datasource_failure_is_not_a_healthy_empty_preview() {
    let grafana = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"message":"Unauthorized"})))
        .mount(&grafana)
        .await;
    let app = test::init_service(App::new().service(Alerts::server(state(&grafana).await))).await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/alerts/grafana/loki/evaluate")
            .insert_header((X_INTERNAL_API_KEY, "local-test-key"))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["rules"][0]["status"], "failed");
    assert!(body["rules"][0].get("instances").is_none());
    assert!(!body.to_string().contains("test-token"));
}

#[actix_web::test]
async fn empty_success_is_explicit_no_data_not_normal() {
    let grafana = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"results":{"A":{"status":200,"frames":[]}}})),
        )
        .mount(&grafana)
        .await;
    let app = test::init_service(App::new().service(Alerts::server(state(&grafana).await))).await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/alerts/grafana/loki/evaluate")
            .insert_header((X_INTERNAL_API_KEY, "local-test-key"))
            .to_request(),
    )
    .await;
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["rules"][0]["no_data"], true);
    assert_eq!(body["rules"][0]["instances"], json!([]));
}

#[actix_web::test]
async fn cloudwatch_route_does_not_dispatch_loki_rules() {
    let grafana = MockServer::start().await;
    let app = test::init_service(App::new().service(Alerts::server(state(&grafana).await))).await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/alerts/grafana/cloudwatch/evaluate")
            .insert_header((X_INTERNAL_API_KEY, "local-test-key"))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["rules"], json!([]));
    assert!(grafana.received_requests().await.unwrap().is_empty());
}

#[actix_web::test]
async fn unauthorized_preview_never_queries_grafana() {
    let grafana = MockServer::start().await;
    let app = test::init_service(App::new().service(Alerts::server(state(&grafana).await))).await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/alerts/grafana/loki/evaluate")
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(grafana.received_requests().await.unwrap().is_empty());
}
