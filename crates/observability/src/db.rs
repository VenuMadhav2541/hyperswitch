//! The storage layer: the one place that holds a connection to the observability database.
//!
//! [`crate::core`] asks [`StorageInterface`] for what it needs in domain types and never sees a
//! connection or a diesel row. Mirrors `router/src/db.rs`: each table declares its operations as an
//! interface trait in its own module, [`StorageInterface`] combines them, and every store
//! implements all of them. [`Store`] is the Postgres implementation; another store — a mock, or a
//! wrapper that adds behaviour around [`Store`] — implements the same traits and is chosen in
//! [`crate::state::AppState::new`], with nothing in `core` changing.
//!
//! Adding a table means adding its interface, implementing it for every store, and adding it to
//! the bounds of [`StorageInterface`].
//!
//! Much thinner than the router's storage layer, because almost none of what that layer does
//! applies here: there is one database, no tenants, no replica, no Redis-backed storage scheme and
//! no encrypted columns.

pub mod alert_rules;
pub mod alerts_info;
pub mod blacklist;
pub mod dictionary;
pub mod infra_tags;
pub mod lifecycle_events;
pub mod metadata;
pub mod rule_toggles;
pub mod silences;
pub mod tag_mapper;
pub mod tags;
pub mod thresholds;

use std::{sync::Arc, time::Duration};

use common_utils::external_service::NoOpEventEmitter;
use diesel_models::{
    errors::DatabaseError, DatabaseConnectionWithContext, DejaPgConnection, StorageResult,
};
use error_stack::ResultExt;
use hyperswitch_masking::PeekInterface;

use crate::{errors::ConfigurationError, settings::Database};

/// Every storage operation this service performs.
///
/// Held by [`crate::state::AppState`] as `Arc<dyn StorageInterface>`: one store shared by every
/// worker, so a store never needs to be cloneable itself.
pub trait StorageInterface:
    Send
    + Sync
    + alert_rules::AlertRulesInterface
    + alerts_info::AlertsInfoInterface
    + blacklist::BlacklistInterface
    + dictionary::DictionaryInterface
    + infra_tags::InfraTagsInterface
    + lifecycle_events::LifecycleEventsInterface
    + metadata::AlertMetadataInterface
    + rule_toggles::RuleTogglesInterface
    + silences::SilencesInterface
    + tag_mapper::TagMapperInterface
    + tags::TagsInterface
    + thresholds::ThresholdsInterface
{
}

impl StorageInterface for Store {}

/// Implement alert-management interfaces as explicit unsupported stubs for unrelated test stores.
///
/// Production stores must implement the interfaces directly. This macro keeps pre-existing route
/// fixtures honest (they still satisfy every aggregate bound) without fabricating successful
/// management writes that those fixtures never exercise.
#[macro_export]
macro_rules! impl_unimplemented_alert_management_interfaces {
    ($store:ty) => {
        #[async_trait::async_trait]
        impl $crate::db::infra_tags::InfraTagsInterface for $store {
            async fn list_infra_reconciliation_batch(
                &self,
                _after: Option<&str>,
                _limit: i64,
            ) -> diesel_models::StorageResult<Vec<$crate::domain_models::alert_rules::AlertRule>>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn create_infra_tag(
                &self,
                _new: $crate::domain_models::tags::AlertTagNew,
            ) -> diesel_models::StorageResult<$crate::domain_models::tags::CreateTagOutcome> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn reconcile_infra_tags(
                &self,
                _alert_rule_id: &str,
                _expected_configuration: common_types::alert_rules::stored::AlertConfiguration,
                _assignments: $crate::domain_models::infra_tags::InfraTagAssignments,
            ) -> diesel_models::StorageResult<
                $crate::domain_models::infra_tags::ReconcileInfraTagsOutcome,
            > {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
        }

        #[async_trait::async_trait]
        impl $crate::db::tags::TagsInterface for $store {
            async fn create_tag(
                &self,
                _new: $crate::domain_models::tags::AlertTagNew,
            ) -> diesel_models::StorageResult<$crate::domain_models::tags::CreateTagOutcome> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn find_tag(
                &self,
                _id: uuid::Uuid,
            ) -> diesel_models::StorageResult<Option<$crate::domain_models::tags::AlertTag>> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn list_tags(
                &self,
            ) -> diesel_models::StorageResult<Vec<$crate::domain_models::tags::AlertTag>> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn update_tag_configuration(
                &self,
                _id: uuid::Uuid,
                _update: $crate::domain_models::tags::AlertTagConfigurationUpdate,
            ) -> diesel_models::StorageResult<$crate::domain_models::tags::UpdateTagOutcome> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn delete_tag(
                &self,
                _id: uuid::Uuid,
            ) -> diesel_models::StorageResult<$crate::domain_models::tags::DeleteTagOutcome> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
        }

        #[async_trait::async_trait]
        impl $crate::db::tag_mapper::TagMapperInterface for $store {
            async fn attach_tag(
                &self,
                _mapping: $crate::domain_models::tag_mapper::AlertTagMapping,
            ) -> diesel_models::StorageResult<$crate::domain_models::tag_mapper::AttachTagOutcome>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn detach_tag(
                &self,
                _mapping: $crate::domain_models::tag_mapper::AlertTagMapping,
            ) -> diesel_models::StorageResult<$crate::domain_models::tag_mapper::DetachTagOutcome>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn list_rule_tags(
                &self,
                _alert_rule_id: &str,
            ) -> diesel_models::StorageResult<Vec<$crate::domain_models::tags::AlertTag>> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn list_tag_rules(
                &self,
                _tag_id: uuid::Uuid,
            ) -> diesel_models::StorageResult<Vec<String>> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
        }

        #[async_trait::async_trait]
        impl $crate::db::silences::SilencesInterface for $store {
            async fn create_silence(
                &self,
                _new: $crate::domain_models::silences::AlertSilenceNew,
            ) -> diesel_models::StorageResult<$crate::domain_models::silences::CreateSilenceOutcome>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn find_silence(
                &self,
                _id: uuid::Uuid,
            ) -> diesel_models::StorageResult<Option<$crate::domain_models::silences::AlertSilence>>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn list_silences(
                &self,
            ) -> diesel_models::StorageResult<Vec<$crate::domain_models::silences::AlertSilence>>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn update_silence(
                &self,
                _id: uuid::Uuid,
                _update: $crate::domain_models::silences::AlertSilenceUpdate,
            ) -> diesel_models::StorageResult<$crate::domain_models::silences::UpdateSilenceOutcome>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn delete_silence(&self, _id: uuid::Uuid) -> diesel_models::StorageResult<bool> {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
            async fn list_active_silences(
                &self,
                _now: time::PrimitiveDateTime,
            ) -> diesel_models::StorageResult<Vec<$crate::domain_models::silences::AlertSilence>>
            {
                Err(error_stack::report!(
                    diesel_models::errors::DatabaseError::Others
                ))
            }
        }
    };
}

type PgPool = bb8::Pool<async_bb8_diesel::ConnectionManager<DejaPgConnection>>;

/// The observability database, behind a connection pool.
///
/// Cheap to clone: the pool is shared, not copied.
#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Build the pool, opening `min_idle_pool_size` connections before returning.
    ///
    /// With at least one idle connection configured an unreachable database fails here, at boot.
    pub async fn new(database: &Database) -> Result<Self, ConfigurationError> {
        let database_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            database.username,
            database.password.peek(),
            database.host,
            database.port,
            database.dbname
        );

        let pool = bb8::Pool::builder()
            .max_size(database.pool_size)
            .min_idle(Some(database.min_idle_pool_size))
            .connection_timeout(Duration::from_secs(database.connection_timeout))
            .build(async_bb8_diesel::ConnectionManager::new(database_url))
            .await
            // The error is not attached: a connection failure can echo the URL, password included.
            .map_err(|_| {
                ConfigurationError::ConfigParsingError(format!(
                    "failed to connect to the observability database at {}:{}/{}",
                    database.host, database.port, database.dbname
                ))
            })?;

        Ok(Self { pool })
    }

    /// Lease a connection. Returned to the pool when dropped.
    ///
    /// No request id and no event emitter: this service does not report its database calls as
    /// external service calls.
    async fn connection(&self) -> StorageResult<DatabaseConnectionWithContext<'_>> {
        let connection = self
            .pool
            .get()
            .await
            .change_context(DatabaseError::DatabaseConnectionError)?;

        Ok(DatabaseConnectionWithContext::new(
            connection,
            None,
            Arc::new(NoOpEventEmitter),
        ))
    }
}
