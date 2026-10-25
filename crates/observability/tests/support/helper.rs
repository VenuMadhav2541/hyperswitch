#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc
)]

use common_types::alert_management::{SilenceConfiguration, TagConfiguration};
use diesel::{Connection, PgConnection, RunQueryDsl};
use diesel_models::observability::schema;
use observability::{
    db::Store,
    domain_models::{silences::AlertSilenceNew, tags::AlertTagNew},
    settings::Database,
};
use serde_json::json;
use time::{macros::datetime, PrimitiveDateTime};

pub fn database_url() -> String {
    std::env::var("OBSERVABILITY_TEST_DATABASE_URL")
        .expect("OBSERVABILITY_TEST_DATABASE_URL is required")
}

pub fn database(url: &str) -> Database {
    let parsed = url::Url::parse(url).expect("OBSERVABILITY_TEST_DATABASE_URL must be a URL");
    Database {
        username: parsed.username().to_owned(),
        password: parsed.password().unwrap_or_default().to_owned().into(),
        host: parsed
            .host_str()
            .expect("database URL must include a host")
            .to_owned(),
        port: parsed.port().unwrap_or(5432),
        dbname: parsed.path().trim_start_matches('/').to_owned(),
        pool_size: 8,
        min_idle_pool_size: 1,
        connection_timeout: 10,
    }
}

pub async fn store(url: &str) -> Store {
    Store::new(&database(url)).await.unwrap()
}

pub fn clean(url: &str) {
    let mut connection = PgConnection::establish(url).unwrap();
    diesel::delete(schema::alert_tag_mapper::table)
        .execute(&mut connection)
        .unwrap();
    diesel::delete(schema::alert_silences::table)
        .execute(&mut connection)
        .unwrap();
    diesel::delete(schema::alert_tags::table)
        .execute(&mut connection)
        .unwrap();
    diesel::delete(schema::alert_rules::table)
        .execute(&mut connection)
        .unwrap();
}

pub fn insert_rule(url: &str, id: &str) {
    let mut connection = PgConnection::establish(url).unwrap();
    diesel::sql_query(
        "INSERT INTO alert_rules (id, name, status, alerts_configuration) VALUES ($1, $1, 'inactive', '{}'::jsonb)",
    )
    .bind::<diesel::sql_types::Text, _>(id)
    .execute(&mut connection)
    .unwrap();
}

pub fn tag_configuration(group_by: &[&str]) -> TagConfiguration {
    serde_json::from_value(json!({
        "version":"v1", "grouping":{"group_by":group_by}
    }))
    .unwrap()
}

pub fn tag(key: &str, value: &str, group_by: &[&str]) -> AlertTagNew {
    AlertTagNew::try_new(
        key.to_owned(),
        value.to_owned(),
        tag_configuration(group_by),
    )
    .unwrap()
}

pub fn silence_configuration(
    tag_ids: Vec<uuid::Uuid>,
    rule_id: Option<&str>,
) -> SilenceConfiguration {
    let group_values = rule_id.map_or_else(|| json!({}), |id| json!({"alert_rule_id":id}));
    serde_json::from_value(json!({
        "version":"v1", "tag_ids":tag_ids, "group_values":group_values
    }))
    .unwrap()
}

pub fn silence(
    tag_ids: Vec<uuid::Uuid>,
    rule_id: Option<&str>,
    starts_at: PrimitiveDateTime,
    ends_at: PrimitiveDateTime,
) -> AlertSilenceNew {
    AlertSilenceNew::try_new(
        silence_configuration(tag_ids, rule_id),
        starts_at,
        ends_at,
        "maintenance".to_owned(),
    )
    .unwrap()
}

pub const NOW: PrimitiveDateTime = datetime!(2026-10-25 10:00);
