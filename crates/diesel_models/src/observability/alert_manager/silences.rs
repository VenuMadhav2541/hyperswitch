//! PostgreSQL rows and mutation outcomes for scheduled silences.

use common_types::alert_management::SilenceConfiguration;
use diesel::{Identifiable, Insertable, Queryable, Selectable};
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::observability::schema::alert_silences;

/// A validated silence ready to insert.
#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = alert_silences)]
pub struct AlertSilenceNew {
    pub configuration: SilenceConfiguration,
    pub starts_at: PrimitiveDateTime,
    pub ends_at: PrimitiveDateTime,
    pub reason: String,
}

/// Validated replacement fields for an existing silence.
#[derive(Clone, Debug)]
pub struct AlertSilenceUpdate {
    pub configuration: SilenceConfiguration,
    pub starts_at: PrimitiveDateTime,
    pub ends_at: PrimitiveDateTime,
    pub reason: String,
}

/// A stored scheduled silence.
#[derive(Clone, Debug, Identifiable, Queryable, Selectable)]
#[diesel(table_name = alert_silences, check_for_backend(diesel::pg::Pg))]
pub struct AlertSilence {
    pub id: Uuid,
    pub configuration: SilenceConfiguration,
    pub starts_at: PrimitiveDateTime,
    pub ends_at: PrimitiveDateTime,
    pub reason: String,
    pub created_at: PrimitiveDateTime,
    pub last_updated_at: PrimitiveDateTime,
}

/// A missing reference discovered while storing a silence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MissingSilenceReference {
    /// A referenced tag does not exist.
    Tag(Uuid),
    /// The explicitly selected rule does not exist.
    AlertRule(String),
}

/// Result of creating a silence.
#[derive(Clone, Debug)]
pub enum CreateSilenceOutcome {
    /// The new row.
    Created(AlertSilence),
    /// A selected object does not exist.
    MissingReference(MissingSilenceReference),
}

/// Result of editing a silence.
#[derive(Clone, Debug)]
pub enum UpdateSilenceOutcome {
    /// The changed row.
    Updated(AlertSilence),
    /// No silence has the requested ID.
    NotFound,
    /// A selected object does not exist.
    MissingReference(MissingSilenceReference),
}
