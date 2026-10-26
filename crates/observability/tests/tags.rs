//! Real-PostgreSQL tag repository tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[path = "support/helper.rs"]
mod support;

use std::time::Duration;

use observability::{
    db::{silences::SilencesInterface, tags::TagsInterface},
    domain_models::{
        silences::CreateSilenceOutcome,
        tags::{AlertTagConfigurationUpdate, CreateTagOutcome, DeleteTagOutcome, UpdateTagOutcome},
    },
};
use support::{clean, database_url, silence, store, tag, tag_configuration};
use time::macros::datetime;

/// Run after applying observability migrations to a disposable database.
#[tokio::test]
#[ignore = "requires OBSERVABILITY_TEST_DATABASE_URL and applied observability migrations"]
async fn postgres_tags_enforce_identity_round_trip_updates_and_managed_deletion() {
    let url = database_url();
    clean(&url);
    let store = store(&url).await;

    let created = match store
        .create_tag(tag(" team ", " core ", &[]))
        .await
        .unwrap()
    {
        CreateTagOutcome::Created(tag) => tag,
        other => panic!("expected created tag, got {other:?}"),
    };
    assert_eq!(created.tag_key, "team");
    assert_eq!(created.tag_value, "core");
    assert_eq!(store.find_tag(created.id).await.unwrap().unwrap(), created);
    assert!(matches!(
        store.create_tag(tag("team", "core", &[])).await.unwrap(),
        CreateTagOutcome::AlreadyExists
    ));

    std::thread::sleep(Duration::from_millis(2));
    let updated = match store
        .update_tag_configuration(
            created.id,
            AlertTagConfigurationUpdate::try_new(tag_configuration(&["alert_rule_id"])).unwrap(),
        )
        .await
        .unwrap()
    {
        UpdateTagOutcome::Updated(tag) => tag,
        other => panic!("expected updated tag, got {other:?}"),
    };
    assert_eq!(updated.created_at, created.created_at);
    assert!(updated.last_updated_at > created.last_updated_at);
    assert_eq!(updated.configuration.group_by().len(), 1);

    let expired = store
        .create_silence(silence(
            vec![created.id],
            None,
            datetime!(2000-01-01 00:00),
            datetime!(2000-01-01 01:00),
        ))
        .await
        .unwrap();
    assert!(matches!(expired, CreateSilenceOutcome::Created(_)));
    assert_eq!(
        store.delete_tag(created.id).await.unwrap(),
        DeleteTagOutcome::Deleted
    );
    assert_eq!(
        store.delete_tag(created.id).await.unwrap(),
        DeleteTagOutcome::NotFound
    );

    let live = match store
        .create_tag(tag("team", "database", &[]))
        .await
        .unwrap()
    {
        CreateTagOutcome::Created(tag) => tag,
        other => panic!("expected created tag, got {other:?}"),
    };
    assert!(matches!(
        store
            .create_silence(silence(
                vec![live.id],
                None,
                datetime!(2099-01-01 00:00),
                datetime!(2099-01-01 01:00),
            ))
            .await
            .unwrap(),
        CreateSilenceOutcome::Created(_)
    ));
    assert_eq!(
        store.delete_tag(live.id).await.unwrap(),
        DeleteTagOutcome::InUse
    );
}
