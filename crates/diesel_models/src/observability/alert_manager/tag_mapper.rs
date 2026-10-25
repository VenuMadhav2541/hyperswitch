//! PostgreSQL row and outcomes for rule/tag membership.

use diesel::{Insertable, Queryable, Selectable};
use uuid::Uuid;

use crate::observability::schema::alert_tag_mapper;

/// A rule/tag membership row.
#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = alert_tag_mapper, check_for_backend(diesel::pg::Pg))]
pub struct AlertTagMapping {
    pub alert_rule_id: String,
    pub tag_id: Uuid,
}

/// Result of atomically attaching a tag to a rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachTagOutcome {
    /// The mapping was inserted.
    Attached,
    /// The mapping already existed.
    AlreadyAttached,
    /// The rule does not exist.
    MissingRule,
    /// The tag does not exist.
    MissingTag,
    /// The rule already has a different grouping-enabled tag.
    GroupingConflict { existing_tag_id: Uuid },
}

/// Result of detaching a tag from a rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachTagOutcome {
    /// A mapping was removed.
    Detached,
    /// The mapping was absent (including when either referenced row is absent).
    Absent,
}
