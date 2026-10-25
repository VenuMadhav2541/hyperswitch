//! Storage operations for alert rules.

use diesel_models::{
    errors::DatabaseError, observability::alert_manager::alert_rules as storage, StorageResult,
};
use error_stack::report;
use time::PrimitiveDateTime;

use crate::{
    db::Store,
    domain_models::{
        alert_rules as domain,
        infra_tags::{assignments_for_rule, ExplicitAssignments, SYSTEM_TAG_KEYS},
    },
};

#[async_trait::async_trait]
pub trait AlertRulesInterface {
    async fn list_alert_rules(&self) -> StorageResult<Vec<domain::AlertRule>>;

    async fn find_alert_rule(&self, id: &str) -> StorageResult<Option<domain::AlertRule>>;

    async fn upsert_alert_rule(
        &self,
        new: domain::AlertRuleNew,
    ) -> StorageResult<domain::AlertRule>;

    /// `false` when no rule has this id.
    async fn delete_alert_rule(&self, id: &str) -> StorageResult<bool>;

    async fn list_active_alert_rules(&self) -> StorageResult<Vec<domain::AlertRule>>;

    /// Active rules whose `next_run_at` has passed.
    async fn list_due_alert_rules(
        &self,
        now: PrimitiveDateTime,
    ) -> StorageResult<Vec<domain::AlertRule>>;

    /// Move an active rule's `next_run_at` from `seen` to `next`. `false` when the rule changed
    /// since it was read, so whoever changed it first runs it.
    async fn claim_alert_rule(
        &self,
        id: &str,
        seen: PrimitiveDateTime,
        next: PrimitiveDateTime,
    ) -> StorageResult<bool>;
}

#[async_trait::async_trait]
impl AlertRulesInterface for Store {
    async fn list_alert_rules(&self) -> StorageResult<Vec<domain::AlertRule>> {
        storage::AlertRule::list(&self.connection().await?)
            .await
            .map(|rows| rows.into_iter().map(domain::AlertRule::from).collect())
    }

    async fn find_alert_rule(&self, id: &str) -> StorageResult<Option<domain::AlertRule>> {
        storage::AlertRule::find(&self.connection().await?, id)
            .await
            .map(|row| row.map(domain::AlertRule::from))
    }

    async fn upsert_alert_rule(
        &self,
        new: domain::AlertRuleNew,
    ) -> StorageResult<domain::AlertRule> {
        let assignments =
            assignments_for_rule(new.alerts_configuration(), ExplicitAssignments::default())
                .map_err(|_| report!(DatabaseError::Others))?;
        if let Some(namespace) = &assignments.missing_service_namespace {
            router_env::logger::warn!(
                namespace = namespace,
                "alert rule has no service classification for its CloudWatch namespace"
            );
        }
        let system = assignments
            .system
            .into_iter()
            .map(|tag| tag.into_new().map(Into::into))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| report!(DatabaseError::Others))?;
        storage::AlertRuleNew::from(new)
            .upsert_with_infra_tags(
                &self.connection().await?,
                SYSTEM_TAG_KEYS.iter().map(ToString::to_string).collect(),
                system,
                Vec::new(),
            )
            .await
            .map(domain::AlertRule::from)
    }

    async fn delete_alert_rule(&self, id: &str) -> StorageResult<bool> {
        storage::AlertRule::delete(&self.connection().await?, id).await
    }

    async fn list_active_alert_rules(&self) -> StorageResult<Vec<domain::AlertRule>> {
        storage::AlertRule::list_active(&self.connection().await?)
            .await
            .map(|rows| rows.into_iter().map(domain::AlertRule::from).collect())
    }

    async fn list_due_alert_rules(
        &self,
        now: PrimitiveDateTime,
    ) -> StorageResult<Vec<domain::AlertRule>> {
        storage::AlertRule::list_due(&self.connection().await?, now)
            .await
            .map(|rows| rows.into_iter().map(domain::AlertRule::from).collect())
    }

    async fn claim_alert_rule(
        &self,
        id: &str,
        seen: PrimitiveDateTime,
        next: PrimitiveDateTime,
    ) -> StorageResult<bool> {
        storage::AlertRule::claim(&self.connection().await?, id, seen, next).await
    }
}
