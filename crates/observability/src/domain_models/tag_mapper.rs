//! Validated rule/tag membership commands and typed outcomes.

use diesel_models::observability::alert_manager::tag_mapper as storage;
use uuid::Uuid;

use super::tags::validate_rule_id;

/// A validated rule/tag pair used for attach and detach.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertTagMapping {
    alert_rule_id: String,
    tag_id: Uuid,
}

impl AlertTagMapping {
    /// Validate the stable rule identifier.
    pub fn try_new(alert_rule_id: String, tag_id: Uuid) -> Result<Self, String> {
        validate_rule_id(&alert_rule_id)?;
        Ok(Self {
            alert_rule_id,
            tag_id,
        })
    }

    pub(crate) fn tag_id(&self) -> Uuid {
        self.tag_id
    }
}

/// Outcome of an atomic attach.
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
    /// The rule already has another grouping-enabled tag.
    GroupingConflict { existing_tag_id: Uuid },
    /// Reserved classifications can only be attached by infrastructure classification.
    SystemOwned,
}

/// Outcome of an idempotent detach.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachTagOutcome {
    /// A mapping was removed.
    Detached,
    /// The mapping was absent.
    Absent,
    /// Reserved classifications can only be detached by infrastructure classification.
    SystemOwned,
}

impl From<AlertTagMapping> for storage::AlertTagMapping {
    fn from(mapping: AlertTagMapping) -> Self {
        Self {
            alert_rule_id: mapping.alert_rule_id,
            tag_id: mapping.tag_id,
        }
    }
}

impl From<storage::AttachTagOutcome> for AttachTagOutcome {
    fn from(outcome: storage::AttachTagOutcome) -> Self {
        match outcome {
            storage::AttachTagOutcome::Attached => Self::Attached,
            storage::AttachTagOutcome::AlreadyAttached => Self::AlreadyAttached,
            storage::AttachTagOutcome::MissingRule => Self::MissingRule,
            storage::AttachTagOutcome::MissingTag => Self::MissingTag,
            storage::AttachTagOutcome::GroupingConflict { existing_tag_id } => {
                Self::GroupingConflict { existing_tag_id }
            }
        }
    }
}

impl From<storage::DetachTagOutcome> for DetachTagOutcome {
    fn from(outcome: storage::DetachTagOutcome) -> Self {
        match outcome {
            storage::DetachTagOutcome::Detached => Self::Detached,
            storage::DetachTagOutcome::Absent => Self::Absent,
        }
    }
}
