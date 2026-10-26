use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use common_types::alert_rules::stored::{AlertConfiguration, AlertRuleStatus};
use diesel::prelude::*;
use error_stack::report;
use time::PrimitiveDateTime;

#[derive(Debug)]
enum ReconcileTransactionError {
    Database(diesel::result::Error),
    StaleConfiguration,
}

impl From<diesel::result::Error> for ReconcileTransactionError {
    fn from(error: diesel::result::Error) -> Self {
        Self::Database(error)
    }
}

use crate::{
    errors::DatabaseError,
    observability::{
        alert_manager::{
            alert_rules::{AlertRule, AlertRuleNew, ReconcileInfraTagsOutcome},
            tags::{AlertTag, AlertTagNew},
        },
        schema::{
            alert_rules::{self, dsl},
            alert_tag_mapper, alert_tags,
        },
    },
    DatabaseConnectionWithContext, RawPgConnection, StorageResult,
};

impl AlertRule {
    pub async fn list(conn: &DatabaseConnectionWithContext<'_>) -> StorageResult<Vec<Self>> {
        dsl::alert_rules
            .order(dsl::id)
            .select(Self::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }

    pub async fn list_batch(
        conn: &DatabaseConnectionWithContext<'_>,
        after: Option<&str>,
        limit: i64,
    ) -> StorageResult<Vec<Self>> {
        let mut query = crate::list::into_boxed_list(dsl::alert_rules);
        if let Some(after) = after {
            query = query.filter(dsl::id.gt(after.to_owned()));
        }
        query
            .order(dsl::id)
            .limit(limit)
            .select(Self::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }

    /// Reconcile only reserved system mappings when the locked rule still has the expected config.
    pub async fn reconcile_infra_tags(
        conn: &DatabaseConnectionWithContext<'_>,
        id: String,
        expected_configuration: AlertConfiguration,
        system_keys: Vec<String>,
        system_tags: Vec<AlertTagNew>,
        explicit_tags: Vec<AlertTagNew>,
    ) -> StorageResult<ReconcileInfraTagsOutcome> {
        match conn
            .raw_connection()
            .transaction_async(move |connection| async move {
                let tag_ids =
                    prepare_tags(&connection, &system_keys, system_tags, explicit_tags).await?;
                let configuration = dsl::alert_rules
                    .find(id.clone())
                    .select(dsl::alerts_configuration)
                    .for_update()
                    .first_async::<AlertConfiguration>(&connection)
                    .await
                    .optional()?;
                let Some(configuration) = configuration else {
                    return Ok(ReconcileInfraTagsOutcome::MissingRule);
                };
                if configuration != expected_configuration {
                    return Err(ReconcileTransactionError::StaleConfiguration);
                }
                replace_system_mappings(&connection, &id, &system_keys, &tag_ids).await?;
                Ok(ReconcileInfraTagsOutcome::Applied)
            })
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(ReconcileTransactionError::StaleConfiguration) => {
                Ok(ReconcileInfraTagsOutcome::StaleConfiguration)
            }
            Err(ReconcileTransactionError::Database(error)) => {
                Err(report!(error).change_context(DatabaseError::Others))
            }
        }
    }

    pub async fn find(
        conn: &DatabaseConnectionWithContext<'_>,
        id: &str,
    ) -> StorageResult<Option<Self>> {
        dsl::alert_rules
            .filter(dsl::id.eq(id.to_owned()))
            .select(Self::as_select())
            .first_async(conn.raw_connection())
            .await
            .optional()
            .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }

    pub async fn list_active(conn: &DatabaseConnectionWithContext<'_>) -> StorageResult<Vec<Self>> {
        dsl::alert_rules
            .filter(dsl::status.eq(AlertRuleStatus::Active))
            .order(dsl::id)
            .select(Self::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }

    /// Active rules whose `next_run_at` has passed.
    pub async fn list_due(
        conn: &DatabaseConnectionWithContext<'_>,
        now: PrimitiveDateTime,
    ) -> StorageResult<Vec<Self>> {
        dsl::alert_rules
            .filter(dsl::status.eq(AlertRuleStatus::Active))
            .filter(dsl::next_run_at.le(now))
            .order(dsl::id)
            .select(Self::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }

    /// Move an active rule's `next_run_at` from `seen` to `next`. `false` when the rule changed
    /// since it was read (another caller claimed it, or it was replaced or deactivated).
    pub async fn claim(
        conn: &DatabaseConnectionWithContext<'_>,
        id: &str,
        seen: PrimitiveDateTime,
        next: PrimitiveDateTime,
    ) -> StorageResult<bool> {
        diesel::update(
            dsl::alert_rules
                .filter(dsl::id.eq(id.to_owned()))
                .filter(dsl::status.eq(AlertRuleStatus::Active))
                .filter(dsl::next_run_at.eq(seen)),
        )
        .set(dsl::next_run_at.eq(next))
        .execute_async(conn.raw_connection())
        .await
        .map(|updated| updated == 1)
        .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }

    /// `false` when no rule has this id.
    pub async fn delete(conn: &DatabaseConnectionWithContext<'_>, id: &str) -> StorageResult<bool> {
        diesel::delete(dsl::alert_rules.filter(dsl::id.eq(id.to_owned())))
            .execute_async(conn.raw_connection())
            .await
            .map(|deleted| deleted > 0)
            .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }
}

impl AlertRuleNew {
    /// Create the rule, or replace everything but `created_at` of an existing one.
    pub async fn upsert(
        self,
        conn: &DatabaseConnectionWithContext<'_>,
    ) -> StorageResult<AlertRule> {
        upsert_rule(conn.raw_connection(), self)
            .await
            .map_err(|error| report!(error).change_context(DatabaseError::Others))
    }

    /// Upsert a rule and reconcile only its reserved system tag mappings in one transaction.
    pub async fn upsert_with_infra_tags(
        self,
        conn: &DatabaseConnectionWithContext<'_>,
        system_keys: Vec<String>,
        system_tags: Vec<AlertTagNew>,
        explicit_tags: Vec<AlertTagNew>,
    ) -> StorageResult<AlertRule> {
        conn.raw_connection()
            .transaction_async(move |connection| async move {
                let tag_ids =
                    prepare_tags(&connection, &system_keys, system_tags, explicit_tags).await?;
                let id = self.id.clone();
                let rule = upsert_rule(&connection, self).await?;
                replace_system_mappings(&connection, &id, &system_keys, &tag_ids).await?;
                Ok(rule)
            })
            .await
            .map_err(|error: diesel::result::Error| {
                report!(error).change_context(DatabaseError::Others)
            })
    }
}

async fn upsert_rule(
    connection: &RawPgConnection,
    rule: AlertRuleNew,
) -> Result<AlertRule, diesel::result::Error> {
    diesel::insert_into(alert_rules::table)
        .values(rule)
        .on_conflict(dsl::id)
        .do_update()
        .set((
            dsl::name.eq(diesel::upsert::excluded(dsl::name)),
            dsl::status.eq(diesel::upsert::excluded(dsl::status)),
            dsl::alerts_configuration.eq(diesel::upsert::excluded(dsl::alerts_configuration)),
            dsl::next_run_at.eq(diesel::upsert::excluded(dsl::next_run_at)),
            dsl::last_updated_at.eq(diesel::upsert::excluded(dsl::last_updated_at)),
        ))
        .returning(AlertRule::as_returning())
        .get_result_async(connection)
        .await
}

/// Return desired system IDs followed by additive explicit IDs. A shared identity with a different
/// configuration aborts the whole transaction.
async fn prepare_tags(
    connection: &RawPgConnection,
    system_keys: &[String],
    system_tags: Vec<AlertTagNew>,
    explicit_tags: Vec<AlertTagNew>,
) -> Result<Vec<uuid::Uuid>, diesel::result::Error> {
    let mut desired = system_tags;
    desired.extend(explicit_tags);
    for tag in desired.iter().cloned() {
        diesel::insert_into(alert_tags::table)
            .values(tag)
            .on_conflict((alert_tags::tag_key, alert_tags::tag_value))
            .do_nothing()
            .execute_async(connection)
            .await?;
    }

    let mut lock_ids = alert_tags::table
        .filter(alert_tags::tag_key.eq_any(system_keys.to_vec()))
        .select(alert_tags::id)
        .load_async::<uuid::Uuid>(connection)
        .await?;
    for tag in &desired {
        let id = alert_tags::table
            .filter(alert_tags::tag_key.eq(tag.tag_key.clone()))
            .filter(alert_tags::tag_value.eq(tag.tag_value.clone()))
            .select(alert_tags::id)
            .first_async::<uuid::Uuid>(connection)
            .await?;
        lock_ids.push(id);
    }
    lock_ids.sort_unstable();
    lock_ids.dedup();
    let locked = alert_tags::table
        .filter(alert_tags::id.eq_any(lock_ids))
        .order(alert_tags::id)
        .select(AlertTag::as_select())
        .for_update()
        .load_async::<AlertTag>(connection)
        .await?;

    let mut desired_ids = Vec::with_capacity(desired.len());
    for expected in desired {
        let Some(actual) = locked
            .iter()
            .find(|tag| tag.tag_key == expected.tag_key && tag.tag_value == expected.tag_value)
        else {
            return Err(diesel::result::Error::RollbackTransaction);
        };
        if actual.configuration != expected.configuration {
            return Err(diesel::result::Error::RollbackTransaction);
        }
        desired_ids.push(actual.id);
    }
    Ok(desired_ids)
}

async fn replace_system_mappings(
    connection: &RawPgConnection,
    rule_id: &str,
    system_keys: &[String],
    desired_ids: &[uuid::Uuid],
) -> Result<(), diesel::result::Error> {
    let reserved_ids = alert_tags::table
        .filter(alert_tags::tag_key.eq_any(system_keys.to_vec()))
        .select(alert_tags::id)
        .load_async::<uuid::Uuid>(connection)
        .await?;
    diesel::delete(
        alert_tag_mapper::table
            .filter(alert_tag_mapper::alert_rule_id.eq(rule_id.to_owned()))
            .filter(alert_tag_mapper::tag_id.eq_any(reserved_ids)),
    )
    .execute_async(connection)
    .await?;
    for tag_id in desired_ids {
        diesel::insert_into(alert_tag_mapper::table)
            .values((
                alert_tag_mapper::alert_rule_id.eq(rule_id.to_owned()),
                alert_tag_mapper::tag_id.eq(*tag_id),
            ))
            .on_conflict_do_nothing()
            .execute_async(connection)
            .await?;
    }
    Ok(())
}
