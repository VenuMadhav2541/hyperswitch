//! Validated scheduled-silence domain types and outcomes.

use common_types::alert_management::SilenceConfiguration;
use diesel_models::observability::alert_manager::silences as storage;
use time::PrimitiveDateTime;
use uuid::Uuid;

const MAX_REASON_LENGTH: usize = 2_048;

/// A stored scheduled silence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertSilence {
    pub id: Uuid,
    pub configuration: SilenceConfiguration,
    pub starts_at: PrimitiveDateTime,
    pub ends_at: PrimitiveDateTime,
    pub reason: String,
    pub created_at: PrimitiveDateTime,
    pub last_updated_at: PrimitiveDateTime,
}

/// A validated silence ready to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertSilenceNew {
    configuration: SilenceConfiguration,
    starts_at: PrimitiveDateTime,
    ends_at: PrimitiveDateTime,
    reason: String,
}

/// Validated fields replacing an existing silence while preserving identity and creation time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertSilenceUpdate {
    configuration: SilenceConfiguration,
    starts_at: PrimitiveDateTime,
    ends_at: PrimitiveDateTime,
    reason: String,
}

impl AlertSilenceNew {
    /// Validate selection, finite ordered timestamps, and operational reason.
    pub fn try_new(
        configuration: SilenceConfiguration,
        starts_at: PrimitiveDateTime,
        ends_at: PrimitiveDateTime,
        reason: String,
    ) -> Result<Self, String> {
        let (configuration, reason) = validate(configuration, starts_at, ends_at, reason)?;
        Ok(Self {
            configuration,
            starts_at,
            ends_at,
            reason,
        })
    }
}

impl AlertSilenceUpdate {
    /// Validate a complete replacement. Every field remains required.
    pub fn try_new(
        configuration: SilenceConfiguration,
        starts_at: PrimitiveDateTime,
        ends_at: PrimitiveDateTime,
        reason: String,
    ) -> Result<Self, String> {
        let (configuration, reason) = validate(configuration, starts_at, ends_at, reason)?;
        Ok(Self {
            configuration,
            starts_at,
            ends_at,
            reason,
        })
    }
}

/// A selected object that does not exist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MissingSilenceReference {
    /// A referenced tag does not exist.
    Tag(Uuid),
    /// The explicitly selected rule does not exist.
    AlertRule(String),
}

/// Outcome of creating a silence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateSilenceOutcome {
    /// The silence was created.
    Created(AlertSilence),
    /// A selected tag or explicit rule does not exist.
    MissingReference(MissingSilenceReference),
}

/// Outcome of editing a silence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateSilenceOutcome {
    /// The silence was updated.
    Updated(AlertSilence),
    /// The silence ID does not exist.
    NotFound,
    /// A selected tag or explicit rule does not exist.
    MissingReference(MissingSilenceReference),
}

fn validate(
    configuration: SilenceConfiguration,
    starts_at: PrimitiveDateTime,
    ends_at: PrimitiveDateTime,
    reason: String,
) -> Result<(SilenceConfiguration, String), String> {
    configuration.validate()?;
    if ends_at <= starts_at {
        return Err("ends_at must be later than starts_at".to_owned());
    }
    let reason = reason.trim().to_owned();
    if reason.is_empty() || reason.chars().count() > MAX_REASON_LENGTH {
        return Err(format!(
            "reason must be 1 to {MAX_REASON_LENGTH} characters"
        ));
    }
    Ok((configuration, reason))
}

impl From<AlertSilenceNew> for storage::AlertSilenceNew {
    fn from(silence: AlertSilenceNew) -> Self {
        Self {
            configuration: silence.configuration,
            starts_at: silence.starts_at,
            ends_at: silence.ends_at,
            reason: silence.reason,
        }
    }
}

impl From<AlertSilenceUpdate> for storage::AlertSilenceUpdate {
    fn from(silence: AlertSilenceUpdate) -> Self {
        Self {
            configuration: silence.configuration,
            starts_at: silence.starts_at,
            ends_at: silence.ends_at,
            reason: silence.reason,
        }
    }
}

impl From<storage::AlertSilence> for AlertSilence {
    fn from(silence: storage::AlertSilence) -> Self {
        Self {
            id: silence.id,
            configuration: silence.configuration,
            starts_at: silence.starts_at,
            ends_at: silence.ends_at,
            reason: silence.reason,
            created_at: silence.created_at,
            last_updated_at: silence.last_updated_at,
        }
    }
}

impl From<storage::MissingSilenceReference> for MissingSilenceReference {
    fn from(reference: storage::MissingSilenceReference) -> Self {
        match reference {
            storage::MissingSilenceReference::Tag(id) => Self::Tag(id),
            storage::MissingSilenceReference::AlertRule(id) => Self::AlertRule(id),
        }
    }
}

impl From<storage::CreateSilenceOutcome> for CreateSilenceOutcome {
    fn from(outcome: storage::CreateSilenceOutcome) -> Self {
        match outcome {
            storage::CreateSilenceOutcome::Created(silence) => Self::Created(silence.into()),
            storage::CreateSilenceOutcome::MissingReference(reference) => {
                Self::MissingReference(reference.into())
            }
        }
    }
}

impl From<storage::UpdateSilenceOutcome> for UpdateSilenceOutcome {
    fn from(outcome: storage::UpdateSilenceOutcome) -> Self {
        match outcome {
            storage::UpdateSilenceOutcome::Updated(silence) => Self::Updated(silence.into()),
            storage::UpdateSilenceOutcome::NotFound => Self::NotFound,
            storage::UpdateSilenceOutcome::MissingReference(reference) => {
                Self::MissingReference(reference.into())
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;
    use time::macros::datetime;

    use super::*;

    fn configuration() -> SilenceConfiguration {
        serde_json::from_value(json!({
            "version":"v1", "tag_ids":[],
            "group_values":{"alert_rule_id":"postgres-cpu"}
        }))
        .unwrap()
    }

    #[test]
    fn silence_requires_an_ordered_window_and_reason() {
        let start = datetime!(2026-10-25 09:00);
        let end = datetime!(2026-10-25 10:00);
        let silence =
            AlertSilenceNew::try_new(configuration(), start, end, " maintenance ".to_owned())
                .unwrap();
        assert_eq!(
            storage::AlertSilenceNew::from(silence).reason,
            "maintenance"
        );
        assert!(AlertSilenceNew::try_new(configuration(), start, start, "x".to_owned()).is_err());
        assert!(AlertSilenceNew::try_new(configuration(), start, end, " ".to_owned()).is_err());
    }
}
