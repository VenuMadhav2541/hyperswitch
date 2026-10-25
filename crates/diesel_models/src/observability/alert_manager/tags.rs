//! PostgreSQL rows and mutation outcomes for alert tags.

use common_types::alert_management::TagConfiguration;
use diesel::{Identifiable, Insertable, Queryable, Selectable};
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::observability::schema::alert_tags;

/// A validated tag ready to insert.
#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = alert_tags)]
pub struct AlertTagNew {
    pub tag_key: String,
    pub tag_value: String,
    pub configuration: TagConfiguration,
}

/// A stored alert tag.
#[derive(Clone, Debug, Identifiable, Queryable, Selectable)]
#[diesel(table_name = alert_tags, check_for_backend(diesel::pg::Pg))]
pub struct AlertTag {
    pub id: Uuid,
    pub tag_key: String,
    pub tag_value: String,
    pub configuration: TagConfiguration,
    pub created_at: PrimitiveDateTime,
    pub last_updated_at: PrimitiveDateTime,
}

/// Result of creating a globally unique key/value pair.
#[derive(Clone, Debug)]
pub enum CreateTagOutcome {
    /// The new row.
    Created(AlertTag),
    /// The key/value pair already exists.
    AlreadyExists,
}

/// Result of changing only a tag's configuration.
#[derive(Clone, Debug)]
pub enum UpdateTagOutcome {
    /// The changed row.
    Updated(AlertTag),
    /// No tag has the requested ID.
    NotFound,
    /// Enabling grouping would give this rule multiple grouping tags.
    GroupingConflict { alert_rule_id: String },
}

/// Result of managed tag deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteTagOutcome {
    /// The row was deleted.
    Deleted,
    /// No tag has the requested ID.
    NotFound,
    /// A rule mapping or live/future silence still references the tag.
    InUse,
}
