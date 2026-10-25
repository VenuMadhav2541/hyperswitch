use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use common_types::alert_management::SilenceConfiguration;
use diesel::{dsl::sql, prelude::*, sql_types::Timestamp};
use error_stack::report;
use time::PrimitiveDateTime;
use uuid::Uuid;

use super::tags::{lock_rules, lock_tags};
use crate::{
    errors::DatabaseError,
    observability::{
        alert_manager::silences::{
            AlertSilence, AlertSilenceNew, AlertSilenceUpdate, CreateSilenceOutcome,
            MissingSilenceReference, UpdateSilenceOutcome,
        },
        schema::alert_silences,
    },
    DatabaseConnectionWithContext, RawPgConnection, StorageResult,
};

impl AlertSilence {
    pub async fn find(
        conn: &DatabaseConnectionWithContext<'_>,
        id: Uuid,
    ) -> StorageResult<Option<Self>> {
        alert_silences::table
            .find(id)
            .select(Self::as_select())
            .first_async(conn.raw_connection())
            .await
            .optional()
            .map_err(other)
    }

    pub async fn list(conn: &DatabaseConnectionWithContext<'_>) -> StorageResult<Vec<Self>> {
        alert_silences::table
            .order((alert_silences::starts_at, alert_silences::id))
            .select(Self::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(other)
    }

    pub async fn list_active(
        conn: &DatabaseConnectionWithContext<'_>,
        now: PrimitiveDateTime,
    ) -> StorageResult<Vec<Self>> {
        alert_silences::table
            .filter(alert_silences::ends_at.gt(now))
            .filter(alert_silences::starts_at.le(now))
            .order((alert_silences::ends_at, alert_silences::id))
            .select(Self::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(other)
    }

    pub async fn update(
        conn: &DatabaseConnectionWithContext<'_>,
        id: Uuid,
        update: AlertSilenceUpdate,
    ) -> StorageResult<UpdateSilenceOutcome> {
        conn.raw_connection()
            .transaction_async(move |connection| async move {
                let exists = alert_silences::table
                    .find(id)
                    .select(alert_silences::id)
                    .first_async::<Uuid>(&connection)
                    .await
                    .optional()?
                    .is_some();
                if !exists {
                    return Ok(UpdateSilenceOutcome::NotFound);
                }
                if let Some(missing) = lock_references(&connection, &update.configuration).await? {
                    return Ok(UpdateSilenceOutcome::MissingReference(missing));
                }
                let still_exists = alert_silences::table
                    .find(id)
                    .select(alert_silences::id)
                    .for_update()
                    .first_async::<Uuid>(&connection)
                    .await
                    .optional()?
                    .is_some();
                if !still_exists {
                    return Ok(UpdateSilenceOutcome::NotFound);
                }
                diesel::update(alert_silences::table.find(id))
                    .set((
                        alert_silences::configuration.eq(update.configuration),
                        alert_silences::starts_at.eq(update.starts_at),
                        alert_silences::ends_at.eq(update.ends_at),
                        alert_silences::reason.eq(update.reason),
                        alert_silences::last_updated_at.eq(sql::<Timestamp>("CURRENT_TIMESTAMP")),
                    ))
                    .returning(Self::as_returning())
                    .get_result_async(&connection)
                    .await
                    .map(UpdateSilenceOutcome::Updated)
            })
            .await
            .map_err(other)
    }

    pub async fn delete(conn: &DatabaseConnectionWithContext<'_>, id: Uuid) -> StorageResult<bool> {
        diesel::delete(alert_silences::table.find(id))
            .execute_async(conn.raw_connection())
            .await
            .map(|deleted| deleted == 1)
            .map_err(other)
    }
}

impl AlertSilenceNew {
    pub async fn create(
        self,
        conn: &DatabaseConnectionWithContext<'_>,
    ) -> StorageResult<CreateSilenceOutcome> {
        conn.raw_connection()
            .transaction_async(move |connection| async move {
                if let Some(missing) = lock_references(&connection, &self.configuration).await? {
                    return Ok(CreateSilenceOutcome::MissingReference(missing));
                }
                diesel::insert_into(alert_silences::table)
                    .values(self)
                    .returning(AlertSilence::as_returning())
                    .get_result_async(&connection)
                    .await
                    .map(CreateSilenceOutcome::Created)
            })
            .await
            .map_err(other)
    }
}

async fn lock_references(
    connection: &RawPgConnection,
    configuration: &SilenceConfiguration,
) -> Result<Option<MissingSilenceReference>, diesel::result::Error> {
    let locked = lock_tags(connection, configuration.tag_ids()).await?;
    if locked.len() != configuration.tag_ids().len() {
        let present = locked.into_iter().map(|tag| tag.id).collect::<Vec<_>>();
        if let Some(missing) = configuration
            .tag_ids()
            .iter()
            .find(|id| !present.contains(id))
        {
            return Ok(Some(MissingSilenceReference::Tag(*missing)));
        }
    }

    if let Some(alert_rule_id) = configuration.alert_rule_id() {
        let rule_ids = vec![alert_rule_id.to_owned()];
        if lock_rules(connection, &rule_ids).await?.is_empty() {
            return Ok(Some(MissingSilenceReference::AlertRule(
                alert_rule_id.to_owned(),
            )));
        }
    }
    Ok(None)
}

fn other(error: diesel::result::Error) -> error_stack::Report<DatabaseError> {
    report!(error).change_context(DatabaseError::Others)
}
