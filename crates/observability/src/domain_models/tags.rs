//! Validated domain types and outcomes for reusable alert tags.

use common_types::alert_management::TagConfiguration;
use diesel_models::observability::alert_manager::tags as storage;
use time::PrimitiveDateTime;
use uuid::Uuid;

const MAX_TAG_KEY_LENGTH: usize = 64;
const MAX_TAG_VALUE_LENGTH: usize = 255;

/// A stored alert tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertTag {
    pub id: Uuid,
    pub tag_key: String,
    pub tag_value: String,
    pub configuration: TagConfiguration,
    pub created_at: PrimitiveDateTime,
    pub last_updated_at: PrimitiveDateTime,
}

/// A validated tag ready to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertTagNew {
    tag_key: String,
    tag_value: String,
    configuration: TagConfiguration,
}

impl AlertTagNew {
    /// Normalize key/value whitespace and validate every stored field.
    pub fn try_new(
        tag_key: String,
        tag_value: String,
        configuration: TagConfiguration,
    ) -> Result<Self, String> {
        let tag_key = normalize(tag_key, "tag_key", MAX_TAG_KEY_LENGTH)?;
        let tag_value = normalize(tag_value, "tag_value", MAX_TAG_VALUE_LENGTH)?;
        configuration.validate()?;
        Ok(Self {
            tag_key,
            tag_value,
            configuration,
        })
    }

    pub(crate) fn is_system_owned(&self) -> bool {
        super::infra_tags::SYSTEM_TAG_KEYS.contains(&self.tag_key.as_str())
    }
}

/// A validated configuration-only tag edit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertTagConfigurationUpdate {
    configuration: TagConfiguration,
}

impl AlertTagConfigurationUpdate {
    /// Validate a replacement configuration.
    pub fn try_new(configuration: TagConfiguration) -> Result<Self, String> {
        configuration.validate()?;
        Ok(Self { configuration })
    }
}

/// Outcome of creating a unique tag identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateTagOutcome {
    /// The tag was created.
    Created(AlertTag),
    /// The normalized key/value pair already exists.
    AlreadyExists,
    /// Reserved classification keys can only be created by infrastructure bootstrap/classification.
    SystemOwned,
}

/// Outcome of changing tag grouping behavior.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateTagOutcome {
    /// The tag was updated.
    Updated(AlertTag),
    /// The tag ID does not exist.
    NotFound,
    /// A mapped rule would have multiple grouping-enabled tags.
    GroupingConflict { alert_rule_id: String },
    /// Reserved classification configuration is managed by infrastructure classification.
    SystemOwned,
}

/// Outcome of managed hard deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteTagOutcome {
    /// The tag was deleted.
    Deleted,
    /// The tag ID does not exist.
    NotFound,
    /// A mapping or live/future silence still references the tag.
    InUse,
    /// Reserved classification rows are managed by infrastructure classification.
    SystemOwned,
}

pub(crate) fn validate_rule_id(value: &str) -> Result<(), String> {
    common_types::alert_management::GroupingField::AlertRuleId.validate_value(value)
}

fn normalize(value: String, field: &str, maximum: usize) -> Result<String, String> {
    let value = value.trim().to_owned();
    if value.is_empty() || value.chars().count() > maximum {
        Err(format!("{field} must be 1 to {maximum} characters"))
    } else {
        Ok(value)
    }
}

impl From<AlertTagNew> for storage::AlertTagNew {
    fn from(tag: AlertTagNew) -> Self {
        Self {
            tag_key: tag.tag_key,
            tag_value: tag.tag_value,
            configuration: tag.configuration,
        }
    }
}

impl From<AlertTagConfigurationUpdate> for TagConfiguration {
    fn from(update: AlertTagConfigurationUpdate) -> Self {
        update.configuration
    }
}

impl From<storage::AlertTag> for AlertTag {
    fn from(tag: storage::AlertTag) -> Self {
        Self {
            id: tag.id,
            tag_key: tag.tag_key,
            tag_value: tag.tag_value,
            configuration: tag.configuration,
            created_at: tag.created_at,
            last_updated_at: tag.last_updated_at,
        }
    }
}

impl From<storage::CreateTagOutcome> for CreateTagOutcome {
    fn from(outcome: storage::CreateTagOutcome) -> Self {
        match outcome {
            storage::CreateTagOutcome::Created(tag) => Self::Created(tag.into()),
            storage::CreateTagOutcome::AlreadyExists => Self::AlreadyExists,
        }
    }
}

impl From<storage::UpdateTagOutcome> for UpdateTagOutcome {
    fn from(outcome: storage::UpdateTagOutcome) -> Self {
        match outcome {
            storage::UpdateTagOutcome::Updated(tag) => Self::Updated(tag.into()),
            storage::UpdateTagOutcome::NotFound => Self::NotFound,
            storage::UpdateTagOutcome::GroupingConflict { alert_rule_id } => {
                Self::GroupingConflict { alert_rule_id }
            }
        }
    }
}

impl From<storage::DeleteTagOutcome> for DeleteTagOutcome {
    fn from(outcome: storage::DeleteTagOutcome) -> Self {
        match outcome {
            storage::DeleteTagOutcome::Deleted => Self::Deleted,
            storage::DeleteTagOutcome::NotFound => Self::NotFound,
            storage::DeleteTagOutcome::InUse => Self::InUse,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    fn configuration() -> TagConfiguration {
        serde_json::from_value(json!({
            "version":"v1", "grouping":{"group_by":[]}
        }))
        .unwrap()
    }

    #[test]
    fn tag_identity_is_trimmed_and_bounded() {
        let tag = AlertTagNew::try_new(" team ".to_owned(), " core ".to_owned(), configuration())
            .unwrap();
        let stored = storage::AlertTagNew::from(tag);
        assert_eq!(stored.tag_key, "team");
        assert_eq!(stored.tag_value, "core");

        assert!(AlertTagNew::try_new(" ".to_owned(), "x".to_owned(), configuration()).is_err());
        assert!(AlertTagNew::try_new(
            "x".repeat(MAX_TAG_KEY_LENGTH + 1),
            "x".to_owned(),
            configuration()
        )
        .is_err());
    }
}
