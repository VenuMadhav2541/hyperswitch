//! Domain-facing scheduled-silence storage operations.

use diesel_models::{observability::alert_manager::silences as storage, StorageResult};
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::{db::Store, domain_models::silences as domain};

/// Scheduled silence persistence seam.
#[async_trait::async_trait]
pub trait SilencesInterface {
    /// Create after atomically locking and checking every selected reference.
    async fn create_silence(
        &self,
        new: domain::AlertSilenceNew,
    ) -> StorageResult<domain::CreateSilenceOutcome>;
    /// Find one silence by UUID.
    async fn find_silence(&self, id: Uuid) -> StorageResult<Option<domain::AlertSilence>>;
    /// List silences in stable start/UUID order.
    async fn list_silences(&self) -> StorageResult<Vec<domain::AlertSilence>>;
    /// Replace all mutable fields while preserving identity and creation time.
    async fn update_silence(
        &self,
        id: Uuid,
        update: domain::AlertSilenceUpdate,
    ) -> StorageResult<domain::UpdateSilenceOutcome>;
    /// Hard-delete; false when already absent.
    async fn delete_silence(&self, id: Uuid) -> StorageResult<bool>;
    /// List rows active at the captured UTC instant (`starts_at <= now < ends_at`).
    async fn list_active_silences(
        &self,
        now: PrimitiveDateTime,
    ) -> StorageResult<Vec<domain::AlertSilence>>;
}

#[async_trait::async_trait]
impl SilencesInterface for Store {
    async fn create_silence(
        &self,
        new: domain::AlertSilenceNew,
    ) -> StorageResult<domain::CreateSilenceOutcome> {
        storage::AlertSilenceNew::from(new)
            .create(&self.connection().await?)
            .await
            .map(Into::into)
    }

    async fn find_silence(&self, id: Uuid) -> StorageResult<Option<domain::AlertSilence>> {
        storage::AlertSilence::find(&self.connection().await?, id)
            .await
            .map(|silence| silence.map(Into::into))
    }

    async fn list_silences(&self) -> StorageResult<Vec<domain::AlertSilence>> {
        storage::AlertSilence::list(&self.connection().await?)
            .await
            .map(|silences| silences.into_iter().map(Into::into).collect())
    }

    async fn update_silence(
        &self,
        id: Uuid,
        update: domain::AlertSilenceUpdate,
    ) -> StorageResult<domain::UpdateSilenceOutcome> {
        storage::AlertSilence::update(&self.connection().await?, id, update.into())
            .await
            .map(Into::into)
    }

    async fn delete_silence(&self, id: Uuid) -> StorageResult<bool> {
        storage::AlertSilence::delete(&self.connection().await?, id).await
    }

    async fn list_active_silences(
        &self,
        now: PrimitiveDateTime,
    ) -> StorageResult<Vec<domain::AlertSilence>> {
        storage::AlertSilence::list_active(&self.connection().await?, now)
            .await
            .map(|silences| silences.into_iter().map(Into::into).collect())
    }
}
