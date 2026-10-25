//! Domain-facing storage operations for reusable alert tags.

use diesel_models::{observability::alert_manager::tags as storage, StorageResult};
use uuid::Uuid;

use crate::{db::Store, domain_models::tags as domain};

/// Alert tag persistence seam.
#[async_trait::async_trait]
pub trait TagsInterface {
    /// Create a unique normalized key/value pair.
    async fn create_tag(&self, new: domain::AlertTagNew)
        -> StorageResult<domain::CreateTagOutcome>;
    /// Find one tag by UUID.
    async fn find_tag(&self, id: Uuid) -> StorageResult<Option<domain::AlertTag>>;
    /// List tags by key/value/UUID.
    async fn list_tags(&self) -> StorageResult<Vec<domain::AlertTag>>;
    /// Atomically edit grouping behavior and reject affected-rule conflicts.
    async fn update_tag_configuration(
        &self,
        id: Uuid,
        update: domain::AlertTagConfigurationUpdate,
    ) -> StorageResult<domain::UpdateTagOutcome>;
    /// Hard-delete a tag only when no mapping or live/future silence uses it.
    async fn delete_tag(&self, id: Uuid) -> StorageResult<domain::DeleteTagOutcome>;
}

#[async_trait::async_trait]
impl TagsInterface for Store {
    async fn create_tag(
        &self,
        new: domain::AlertTagNew,
    ) -> StorageResult<domain::CreateTagOutcome> {
        storage::AlertTagNew::from(new)
            .create(&self.connection().await?)
            .await
            .map(Into::into)
    }

    async fn find_tag(&self, id: Uuid) -> StorageResult<Option<domain::AlertTag>> {
        storage::AlertTag::find(&self.connection().await?, id)
            .await
            .map(|tag| tag.map(Into::into))
    }

    async fn list_tags(&self) -> StorageResult<Vec<domain::AlertTag>> {
        storage::AlertTag::list(&self.connection().await?)
            .await
            .map(|tags| tags.into_iter().map(Into::into).collect())
    }

    async fn update_tag_configuration(
        &self,
        id: Uuid,
        update: domain::AlertTagConfigurationUpdate,
    ) -> StorageResult<domain::UpdateTagOutcome> {
        storage::AlertTag::update_configuration(&self.connection().await?, id, update.into())
            .await
            .map(Into::into)
    }

    async fn delete_tag(&self, id: Uuid) -> StorageResult<domain::DeleteTagOutcome> {
        storage::AlertTag::delete_managed(&self.connection().await?, id)
            .await
            .map(Into::into)
    }
}
