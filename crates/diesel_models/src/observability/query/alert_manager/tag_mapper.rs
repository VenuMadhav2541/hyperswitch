use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use common_types::alert_management::TagConfiguration;
use diesel::prelude::*;
use error_stack::report;
use uuid::Uuid;

use super::tags::{lock_rules, lock_tag};
use crate::{
    errors::DatabaseError,
    observability::{
        alert_manager::{
            tag_mapper::{AlertTagMapping, AttachTagOutcome, DetachTagOutcome},
            tags::AlertTag,
        },
        schema::{alert_tag_mapper, alert_tags},
    },
    DatabaseConnectionWithContext, StorageResult,
};

impl AlertTagMapping {
    pub async fn attach(
        self,
        conn: &DatabaseConnectionWithContext<'_>,
    ) -> StorageResult<AttachTagOutcome> {
        conn.raw_connection()
            .transaction_async(move |connection| async move {
                let Some(tag) = lock_tag(&connection, self.tag_id).await? else {
                    return Ok(AttachTagOutcome::MissingTag);
                };
                if lock_rules(&connection, std::slice::from_ref(&self.alert_rule_id))
                    .await?
                    .is_empty()
                {
                    return Ok(AttachTagOutcome::MissingRule);
                }

                let exists = alert_tag_mapper::table
                    .find((self.alert_rule_id.clone(), self.tag_id))
                    .select(alert_tag_mapper::tag_id)
                    .first_async::<Uuid>(&connection)
                    .await
                    .optional()?
                    .is_some();
                if exists {
                    return Ok(AttachTagOutcome::AlreadyAttached);
                }

                if tag.configuration.is_grouping_enabled() {
                    let existing = alert_tag_mapper::table
                        .inner_join(alert_tags::table)
                        .filter(alert_tag_mapper::alert_rule_id.eq(self.alert_rule_id.clone()))
                        .order(alert_tag_mapper::tag_id)
                        .select((alert_tag_mapper::tag_id, alert_tags::configuration))
                        .load_async::<(Uuid, TagConfiguration)>(&connection)
                        .await?
                        .into_iter()
                        .find(|(_, configuration)| configuration.is_grouping_enabled());
                    if let Some((existing_tag_id, _)) = existing {
                        return Ok(AttachTagOutcome::GroupingConflict { existing_tag_id });
                    }
                }

                let inserted = diesel::insert_into(alert_tag_mapper::table)
                    .values(self)
                    .on_conflict_do_nothing()
                    .execute_async(&connection)
                    .await?;
                Ok(if inserted == 1 {
                    AttachTagOutcome::Attached
                } else {
                    AttachTagOutcome::AlreadyAttached
                })
            })
            .await
            .map_err(other)
    }

    pub async fn detach(
        self,
        conn: &DatabaseConnectionWithContext<'_>,
    ) -> StorageResult<DetachTagOutcome> {
        conn.raw_connection()
            .transaction_async(move |connection| async move {
                let Some(_tag) = lock_tag(&connection, self.tag_id).await? else {
                    return Ok(DetachTagOutcome::Absent);
                };
                if lock_rules(&connection, std::slice::from_ref(&self.alert_rule_id))
                    .await?
                    .is_empty()
                {
                    return Ok(DetachTagOutcome::Absent);
                }
                let deleted =
                    diesel::delete(alert_tag_mapper::table.find((self.alert_rule_id, self.tag_id)))
                        .execute_async(&connection)
                        .await?;
                Ok(if deleted == 1 {
                    DetachTagOutcome::Detached
                } else {
                    DetachTagOutcome::Absent
                })
            })
            .await
            .map_err(other)
    }

    pub async fn list_rule_tags(
        conn: &DatabaseConnectionWithContext<'_>,
        alert_rule_id: &str,
    ) -> StorageResult<Vec<AlertTag>> {
        alert_tag_mapper::table
            .inner_join(alert_tags::table)
            .filter(alert_tag_mapper::alert_rule_id.eq(alert_rule_id.to_owned()))
            .order((alert_tags::tag_key, alert_tags::tag_value, alert_tags::id))
            .select(AlertTag::as_select())
            .load_async(conn.raw_connection())
            .await
            .map_err(other)
    }

    pub async fn list_tag_rules(
        conn: &DatabaseConnectionWithContext<'_>,
        tag_id: Uuid,
    ) -> StorageResult<Vec<String>> {
        alert_tag_mapper::table
            .filter(alert_tag_mapper::tag_id.eq(tag_id))
            .order(alert_tag_mapper::alert_rule_id)
            .select(alert_tag_mapper::alert_rule_id)
            .load_async(conn.raw_connection())
            .await
            .map_err(other)
    }
}

fn other(error: diesel::result::Error) -> error_stack::Report<DatabaseError> {
    report!(error).change_context(DatabaseError::Others)
}
