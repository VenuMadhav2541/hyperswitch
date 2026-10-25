//! Real-PostgreSQL silence repository and tag-reference concurrency tests.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[path = "support/helper.rs"]
mod support;

use observability::{
    db::{silences::SilencesInterface, tags::TagsInterface},
    domain_models::{
        silences::{
            AlertSilenceUpdate, CreateSilenceOutcome, MissingSilenceReference, UpdateSilenceOutcome,
        },
        tags::{CreateTagOutcome, DeleteTagOutcome},
    },
};
use support::{clean, database_url, insert_rule, silence, silence_configuration, store, tag, NOW};
use time::{macros::datetime, Duration};
use uuid::Uuid;

/// Run after applying observability migrations to a disposable database.
#[tokio::test]
#[ignore = "requires OBSERVABILITY_TEST_DATABASE_URL and applied observability migrations"]
async fn postgres_silences_validate_references_boundaries_hard_delete_and_concurrency() {
    let url = database_url();
    clean(&url);
    insert_rule(&url, "rule-one");
    let store = store(&url).await;

    let stored_tag = match store.create_tag(tag("team", "core", &[])).await.unwrap() {
        CreateTagOutcome::Created(tag) => tag,
        other => panic!("expected created tag, got {other:?}"),
    };
    let missing_tag = Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    assert!(matches!(
        store
            .create_silence(silence(
                vec![missing_tag],
                None,
                NOW,
                NOW + Duration::hours(1),
            ))
            .await
            .unwrap(),
        CreateSilenceOutcome::MissingReference(MissingSilenceReference::Tag(id))
            if id == missing_tag
    ));
    assert!(matches!(
        store
            .create_silence(silence(
                vec![stored_tag.id],
                Some("missing-rule"),
                NOW,
                NOW + Duration::hours(1),
            ))
            .await
            .unwrap(),
        CreateSilenceOutcome::MissingReference(MissingSilenceReference::AlertRule(rule))
            if rule == "missing-rule"
    ));

    let ended = store
        .create_silence(silence(
            vec![stored_tag.id],
            None,
            NOW - Duration::hours(1),
            NOW,
        ))
        .await
        .unwrap();
    assert!(matches!(ended, CreateSilenceOutcome::Created(_)));
    let active = match store
        .create_silence(silence(
            vec![stored_tag.id],
            Some("rule-one"),
            NOW,
            NOW + Duration::hours(1),
        ))
        .await
        .unwrap()
    {
        CreateSilenceOutcome::Created(silence) => silence,
        other => panic!("expected created silence, got {other:?}"),
    };
    let future = store
        .create_silence(silence(
            vec![stored_tag.id],
            None,
            NOW + Duration::minutes(1),
            NOW + Duration::hours(2),
        ))
        .await
        .unwrap();
    assert!(matches!(future, CreateSilenceOutcome::Created(_)));
    assert_eq!(
        store.list_active_silences(NOW).await.unwrap(),
        vec![active.clone()]
    );

    let update = AlertSilenceUpdate::try_new(
        silence_configuration(vec![stored_tag.id], Some("rule-one")),
        NOW,
        NOW + Duration::hours(3),
        " extended maintenance ".to_owned(),
    )
    .unwrap();
    let updated = match store.update_silence(active.id, update).await.unwrap() {
        UpdateSilenceOutcome::Updated(silence) => silence,
        other => panic!("expected updated silence, got {other:?}"),
    };
    assert_eq!(updated.created_at, active.created_at);
    assert_eq!(updated.reason, "extended maintenance");
    assert!(store.delete_silence(updated.id).await.unwrap());
    assert!(!store.delete_silence(updated.id).await.unwrap());

    let race_tag = match store
        .create_tag(tag("service", "payments", &[]))
        .await
        .unwrap()
    {
        CreateTagOutcome::Created(tag) => tag,
        other => panic!("expected created tag, got {other:?}"),
    };
    let (deleted, created) = tokio::join!(
        store.delete_tag(race_tag.id),
        store.create_silence(silence(
            vec![race_tag.id],
            None,
            datetime!(2099-01-01 00:00),
            datetime!(2099-01-01 01:00),
        )),
    );
    let deleted = deleted.unwrap();
    let created = created.unwrap();
    assert!(
        matches!(
            (&deleted, &created),
            (
                DeleteTagOutcome::Deleted,
                CreateSilenceOutcome::MissingReference(MissingSilenceReference::Tag(id))
            ) if *id == race_tag.id
        ) || matches!(
            (&deleted, &created),
            (DeleteTagOutcome::InUse, CreateSilenceOutcome::Created(_))
        )
    );
    if deleted == DeleteTagOutcome::Deleted {
        assert!(store.find_tag(race_tag.id).await.unwrap().is_none());
    } else {
        assert!(store.find_tag(race_tag.id).await.unwrap().is_some());
    }

    let source_tag = match store
        .create_tag(tag("service", "source", &[]))
        .await
        .unwrap()
    {
        CreateTagOutcome::Created(tag) => tag,
        other => panic!("expected created tag, got {other:?}"),
    };
    let update_tag = match store
        .create_tag(tag("service", "update-target", &[]))
        .await
        .unwrap()
    {
        CreateTagOutcome::Created(tag) => tag,
        other => panic!("expected created tag, got {other:?}"),
    };
    let existing = match store
        .create_silence(silence(
            vec![source_tag.id],
            None,
            datetime!(2099-02-01 00:00),
            datetime!(2099-02-01 01:00),
        ))
        .await
        .unwrap()
    {
        CreateSilenceOutcome::Created(silence) => silence,
        other => panic!("expected created silence, got {other:?}"),
    };
    let update = AlertSilenceUpdate::try_new(
        silence_configuration(vec![update_tag.id], None),
        datetime!(2099-02-01 00:00),
        datetime!(2099-02-01 02:00),
        "updated selection".to_owned(),
    )
    .unwrap();
    let (deleted, updated) = tokio::join!(
        store.delete_tag(update_tag.id),
        store.update_silence(existing.id, update),
    );
    let deleted = deleted.unwrap();
    let updated = updated.unwrap();
    assert!(
        matches!(
            (&deleted, &updated),
            (
                DeleteTagOutcome::Deleted,
                UpdateSilenceOutcome::MissingReference(MissingSilenceReference::Tag(id))
            ) if *id == update_tag.id
        ) || matches!(
            (&deleted, &updated),
            (DeleteTagOutcome::InUse, UpdateSilenceOutcome::Updated(_))
        )
    );
}
