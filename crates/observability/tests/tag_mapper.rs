//! Real-PostgreSQL membership and grouping-conflict tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[path = "support/helper.rs"]
mod support;

use diesel::{Connection, PgConnection, QueryDsl, RunQueryDsl};
use diesel_models::observability::schema;
use observability::{
    db::{tag_mapper::TagMapperInterface, tags::TagsInterface},
    domain_models::{
        tag_mapper::{AlertTagMapping, AttachTagOutcome},
        tags::{
            AlertTag, AlertTagConfigurationUpdate, CreateTagOutcome, DeleteTagOutcome,
            UpdateTagOutcome,
        },
    },
};
use support::{clean, database_url, insert_rule, store, tag, tag_configuration};
use uuid::Uuid;

fn created(outcome: CreateTagOutcome) -> AlertTag {
    match outcome {
        CreateTagOutcome::Created(tag) => tag,
        other => panic!("expected created tag, got {other:?}"),
    }
}

fn mapping(rule: &str, tag_id: Uuid) -> AlertTagMapping {
    AlertTagMapping::try_new(rule.to_owned(), tag_id).unwrap()
}

/// Run after applying observability migrations to a disposable database.
#[tokio::test]
#[ignore = "requires OBSERVABILITY_TEST_DATABASE_URL and applied observability migrations"]
async fn postgres_membership_is_idempotent_and_grouping_conflicts_are_atomic() {
    let url = database_url();
    clean(&url);
    insert_rule(&url, "rule-one");
    insert_rule(&url, "rule-two");
    let store = store(&url).await;

    let first = created(
        store
            .create_tag(tag("group", "daily", &["alert_rule_id", "time.day"]))
            .await
            .unwrap(),
    );
    let second = created(
        store
            .create_tag(tag("group", "weekly", &["alert_rule_id", "time.week"]))
            .await
            .unwrap(),
    );

    assert_eq!(
        store
            .attach_tag(mapping("missing-rule", first.id))
            .await
            .unwrap(),
        AttachTagOutcome::MissingRule
    );
    assert_eq!(
        store
            .attach_tag(mapping(
                "rule-one",
                Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(),
            ))
            .await
            .unwrap(),
        AttachTagOutcome::MissingTag
    );

    let (left, right) = tokio::join!(
        store.attach_tag(mapping("rule-one", first.id)),
        store.attach_tag(mapping("rule-one", second.id)),
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AttachTagOutcome::Attached))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AttachTagOutcome::GroupingConflict { .. }))
            .count(),
        1
    );
    let attached = store.list_rule_tags("rule-one").await.unwrap();
    assert_eq!(attached.len(), 1);
    assert!(attached[0].configuration.is_grouping_enabled());
    assert_eq!(
        store
            .attach_tag(mapping("rule-one", attached[0].id))
            .await
            .unwrap(),
        AttachTagOutcome::AlreadyAttached
    );
    assert_eq!(
        store.delete_tag(attached[0].id).await.unwrap(),
        DeleteTagOutcome::InUse
    );
    let mut connection = PgConnection::establish(&url).unwrap();
    assert!(
        diesel::delete(schema::alert_tags::table.find(attached[0].id))
            .execute(&mut connection)
            .is_err()
    );
    diesel::delete(schema::alert_rules::table.find("rule-one"))
        .execute(&mut connection)
        .unwrap();
    assert!(store
        .list_tag_rules(attached[0].id)
        .await
        .unwrap()
        .is_empty());
    assert!(store.find_tag(attached[0].id).await.unwrap().is_some());

    let classification = created(store.create_tag(tag("team", "core", &[])).await.unwrap());
    let competing = created(
        store
            .create_tag(tag("group", "instance", &["dimensions.InstanceId"]))
            .await
            .unwrap(),
    );
    assert_eq!(
        store
            .attach_tag(mapping("rule-two", classification.id))
            .await
            .unwrap(),
        AttachTagOutcome::Attached
    );

    let (edit, attach) = tokio::join!(
        store.update_tag_configuration(
            classification.id,
            AlertTagConfigurationUpdate::try_new(tag_configuration(&["alert_rule_id"])).unwrap(),
        ),
        store.attach_tag(mapping("rule-two", competing.id)),
    );
    let edit = edit.unwrap();
    let attach = attach.unwrap();
    assert!(matches!(
        (&edit, &attach),
        (
            UpdateTagOutcome::Updated(_),
            AttachTagOutcome::GroupingConflict { .. }
        ) | (
            UpdateTagOutcome::GroupingConflict { .. },
            AttachTagOutcome::Attached
        )
    ));
    let grouped = store
        .list_rule_tags("rule-two")
        .await
        .unwrap()
        .into_iter()
        .filter(|tag| tag.configuration.is_grouping_enabled())
        .count();
    assert_eq!(grouped, 1);
}
