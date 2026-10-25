use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use common_types::alert_management::TagConfiguration;
use diesel::{
    dsl::sql,
    prelude::*,
    sql_types::{Bool, Text, Timestamp},
};
use error_stack::report;
use uuid::Uuid;

use crate::{
    errors::DatabaseError,
    observability::{
        alert_manager::tags::{
            AlertTag, AlertTagNew, CreateTagOutcome, DeleteTagOutcome, UpdateTagOutcome,
        },
        schema::{alert_rules, alert_silences, alert_tag_mapper, alert_tags},
    },
    DatabaseConnectionWithContext, RawPgConnection, StorageResult,
};

impl AlertTag {
    pub async fn find(
        conn: &DatabaseConnectionWithContext<'_>,
        id: Uuid,
    ) -> StorageResult<Option<Self>> {
        alert_tags::table
            .find(id)
            .select(Self::as_select())
            .first_async(conn.raw_connection())
            .await
            .optional()
            .map_err(other)
    }

    pub async fn list(conn: &DatabaseConnectionWithContext<'_>) -> StorageResult<Vec<Self>> {
        alert_tags::table
            .order((alert_tags::tag_key, alert_tags::tag_value, alert_tags::id))
            .select(Self::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(other)
    }

    pub async fn update_configuration(
        conn: &DatabaseConnectionWithContext<'_>,
        id: Uuid,
        configuration: TagConfiguration,
    ) -> StorageResult<UpdateTagOutcome> {
        conn.raw_connection()
            .transaction_async(move |connection| async move {
                let Some(_locked_tag) = lock_tag(&connection, id).await? else {
                    return Ok(UpdateTagOutcome::NotFound);
                };

                let rule_ids = alert_tag_mapper::table
                    .filter(alert_tag_mapper::tag_id.eq(id))
                    .order(alert_tag_mapper::alert_rule_id)
                    .select(alert_tag_mapper::alert_rule_id)
                    .load_async::<String>(&connection)
                    .await?;
                lock_rules(&connection, &rule_ids).await?;

                if configuration.is_grouping_enabled() && !rule_ids.is_empty() {
                    let memberships = alert_tag_mapper::table
                        .inner_join(alert_tags::table)
                        .filter(alert_tag_mapper::alert_rule_id.eq_any(rule_ids.clone()))
                        .filter(alert_tag_mapper::tag_id.ne(id))
                        .order((alert_tag_mapper::alert_rule_id, alert_tag_mapper::tag_id))
                        .select((alert_tag_mapper::alert_rule_id, alert_tags::configuration))
                        .load_async::<(String, TagConfiguration)>(&connection)
                        .await?;
                    if let Some((alert_rule_id, _)) = memberships
                        .into_iter()
                        .find(|(_, other)| other.is_grouping_enabled())
                    {
                        return Ok(UpdateTagOutcome::GroupingConflict { alert_rule_id });
                    }
                }

                diesel::update(alert_tags::table.find(id))
                    .set((
                        alert_tags::configuration.eq(configuration),
                        alert_tags::last_updated_at.eq(sql::<Timestamp>("CURRENT_TIMESTAMP")),
                    ))
                    .returning(Self::as_returning())
                    .get_result_async(&connection)
                    .await
                    .map(UpdateTagOutcome::Updated)
            })
            .await
            .map_err(other)
    }

    /// Delete only when no mapping or non-expired silence references this tag.
    pub async fn delete_managed(
        conn: &DatabaseConnectionWithContext<'_>,
        id: Uuid,
    ) -> StorageResult<DeleteTagOutcome> {
        conn.raw_connection()
            .transaction_async(move |connection| async move {
                let Some(_locked_tag) = lock_tag(&connection, id).await? else {
                    return Ok(DeleteTagOutcome::NotFound);
                };

                let rule_ids = alert_tag_mapper::table
                    .filter(alert_tag_mapper::tag_id.eq(id))
                    .order(alert_tag_mapper::alert_rule_id)
                    .select(alert_tag_mapper::alert_rule_id)
                    .load_async::<String>(&connection)
                    .await?;
                lock_rules(&connection, &rule_ids).await?;
                if !rule_ids.is_empty() {
                    return Ok(DeleteTagOutcome::InUse);
                }

                let referenced_by_silence = alert_silences::table
                    .filter(sql::<Bool>("ends_at > CURRENT_TIMESTAMP"))
                    .filter(
                        sql::<Bool>("configuration -> 'tag_ids' ? ")
                            .bind::<Text, _>(id.to_string()),
                    )
                    .select(alert_silences::id)
                    .first_async::<Uuid>(&connection)
                    .await
                    .optional()?
                    .is_some();
                if referenced_by_silence {
                    return Ok(DeleteTagOutcome::InUse);
                }

                diesel::delete(alert_tags::table.find(id))
                    .execute_async(&connection)
                    .await?;
                Ok(DeleteTagOutcome::Deleted)
            })
            .await
            .map_err(other)
    }
}

impl AlertTagNew {
    pub async fn create(
        self,
        conn: &DatabaseConnectionWithContext<'_>,
    ) -> StorageResult<CreateTagOutcome> {
        diesel::insert_into(alert_tags::table)
            .values(self)
            .on_conflict((alert_tags::tag_key, alert_tags::tag_value))
            .do_nothing()
            .returning(AlertTag::as_returning())
            .get_result_async(conn.raw_connection())
            .await
            .optional()
            .map(|row| row.map_or(CreateTagOutcome::AlreadyExists, CreateTagOutcome::Created))
            .map_err(other)
    }
}

pub(super) async fn lock_tag(
    connection: &RawPgConnection,
    id: Uuid,
) -> Result<Option<AlertTag>, diesel::result::Error> {
    alert_tags::table
        .find(id)
        .select(AlertTag::as_select())
        .for_update()
        .first_async(connection)
        .await
        .optional()
}

pub(super) async fn lock_tags(
    connection: &RawPgConnection,
    ids: &[Uuid],
) -> Result<Vec<AlertTag>, diesel::result::Error> {
    let mut sorted = ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.is_empty() {
        return Ok(Vec::new());
    }
    alert_tags::table
        .filter(alert_tags::id.eq_any(sorted))
        .order(alert_tags::id)
        .select(AlertTag::as_select())
        .for_update()
        .load_async(connection)
        .await
}

pub(super) async fn lock_rules(
    connection: &RawPgConnection,
    rule_ids: &[String],
) -> Result<Vec<String>, diesel::result::Error> {
    let mut sorted = rule_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.is_empty() {
        return Ok(Vec::new());
    }
    alert_rules::table
        .filter(alert_rules::id.eq_any(sorted))
        .order(alert_rules::id)
        .select(alert_rules::id)
        .for_update()
        .load_async(connection)
        .await
}

fn other(error: diesel::result::Error) -> error_stack::Report<DatabaseError> {
    report!(error).change_context(DatabaseError::Others)
}
