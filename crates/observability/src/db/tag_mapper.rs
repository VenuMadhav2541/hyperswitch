//! Domain-facing atomic rule/tag membership operations.

use diesel_models::{observability::alert_manager::tag_mapper as storage, StorageResult};
use uuid::Uuid;

use crate::{
    db::Store,
    domain_models::{tag_mapper as domain, tags::AlertTag},
};

/// Rule/tag membership persistence seam.
#[async_trait::async_trait]
pub trait TagMapperInterface {
    /// Idempotently attach with reference and grouping-conflict checks in one transaction.
    async fn attach_tag(
        &self,
        mapping: domain::AlertTagMapping,
    ) -> StorageResult<domain::AttachTagOutcome>;
    /// Idempotently detach under the same lock order as attach and tag edits.
    async fn detach_tag(
        &self,
        mapping: domain::AlertTagMapping,
    ) -> StorageResult<domain::DetachTagOutcome>;
    /// List every tag attached to a rule in stable key/value order.
    async fn list_rule_tags(&self, alert_rule_id: &str) -> StorageResult<Vec<AlertTag>>;
    /// List every rule using a tag in stable ID order.
    async fn list_tag_rules(&self, tag_id: Uuid) -> StorageResult<Vec<String>>;
}

#[async_trait::async_trait]
impl TagMapperInterface for Store {
    async fn attach_tag(
        &self,
        mapping: domain::AlertTagMapping,
    ) -> StorageResult<domain::AttachTagOutcome> {
        storage::AlertTagMapping::from(mapping)
            .attach(&self.connection().await?)
            .await
            .map(Into::into)
    }

    async fn detach_tag(
        &self,
        mapping: domain::AlertTagMapping,
    ) -> StorageResult<domain::DetachTagOutcome> {
        storage::AlertTagMapping::from(mapping)
            .detach(&self.connection().await?)
            .await
            .map(Into::into)
    }

    async fn list_rule_tags(&self, alert_rule_id: &str) -> StorageResult<Vec<AlertTag>> {
        storage::AlertTagMapping::list_rule_tags(&self.connection().await?, alert_rule_id)
            .await
            .map(|tags| tags.into_iter().map(Into::into).collect())
    }

    async fn list_tag_rules(&self, tag_id: Uuid) -> StorageResult<Vec<String>> {
        storage::AlertTagMapping::list_tag_rules(&self.connection().await?, tag_id).await
    }
}
