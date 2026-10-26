//! Internal storage seam for bounded infrastructure-tag reconciliation.

use diesel_models::{
    errors::DatabaseError, observability::alert_manager::alert_rules as storage, StorageResult,
};
use error_stack::report;

use crate::{
    db::Store,
    domain_models::{
        alert_rules::AlertRule,
        infra_tags::{InfraTagAssignments, ReconcileInfraTagsOutcome, SYSTEM_TAG_KEYS},
        tags::{AlertTagNew, CreateTagOutcome},
    },
};

#[async_trait::async_trait]
pub trait InfraTagsInterface {
    /// Stable ID-ordered page; `after` is exclusive and `limit` is already bounded by the caller.
    async fn list_infra_reconciliation_batch(
        &self,
        after: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<AlertRule>>;

    /// Create one system catalogue row through the dedicated bootstrap path.
    async fn create_infra_tag(&self, new: AlertTagNew) -> StorageResult<CreateTagOutcome>;

    /// Reconcile one existing rule atomically against the configuration used to derive assignments.
    async fn reconcile_infra_tags(
        &self,
        alert_rule_id: &str,
        expected_configuration: common_types::alert_rules::stored::AlertConfiguration,
        assignments: InfraTagAssignments,
    ) -> StorageResult<ReconcileInfraTagsOutcome>;
}

#[async_trait::async_trait]
impl InfraTagsInterface for Store {
    async fn list_infra_reconciliation_batch(
        &self,
        after: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<AlertRule>> {
        storage::AlertRule::list_batch(&self.connection().await?, after, limit)
            .await
            .map(|rules| rules.into_iter().map(AlertRule::from).collect())
    }

    async fn create_infra_tag(&self, new: AlertTagNew) -> StorageResult<CreateTagOutcome> {
        diesel_models::observability::alert_manager::tags::AlertTagNew::from(new)
            .create(&self.connection().await?)
            .await
            .map(Into::into)
    }

    async fn reconcile_infra_tags(
        &self,
        alert_rule_id: &str,
        expected_configuration: common_types::alert_rules::stored::AlertConfiguration,
        assignments: InfraTagAssignments,
    ) -> StorageResult<ReconcileInfraTagsOutcome> {
        let system = assignments
            .system
            .into_iter()
            .map(|tag| tag.into_new().map(Into::into))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| report!(DatabaseError::Others))?;
        let explicit = assignments
            .explicit
            .into_iter()
            .map(|tag| tag.into_new().map(Into::into))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| report!(DatabaseError::Others))?;
        storage::AlertRule::reconcile_infra_tags(
            &self.connection().await?,
            alert_rule_id.to_owned(),
            expected_configuration,
            SYSTEM_TAG_KEYS.iter().map(ToString::to_string).collect(),
            system,
            explicit,
        )
        .await
        .map(Into::into)
    }
}
