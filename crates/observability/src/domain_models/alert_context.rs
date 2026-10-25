//! Normalized, provider-independent values used by grouping and silence matching.

use std::collections::{BTreeMap, BTreeSet};

use common_types::alert_management::GroupingField;
use time::{Duration, PrimitiveDateTime};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertContext {
    pub rule_id: String,
    pub identity_dimensions: BTreeMap<String, String>,
    /// Captured event time in UTC. Retries must retain this value.
    pub occurred_at: PrimitiveDateTime,
    pub occurrence_id: Option<String>,
    pub attached_tag_ids: BTreeSet<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ContextResolutionError {
    #[error("context is missing required field `{0}`")]
    MissingField(GroupingField),
    #[error("context field `{field}` has an invalid value: {reason}")]
    InvalidValue {
        field: GroupingField,
        reason: String,
    },
}

impl AlertContext {
    pub fn resolve(&self, field: &GroupingField) -> Result<String, ContextResolutionError> {
        let value = match field {
            GroupingField::AlertRuleId => self.rule_id.clone(),
            GroupingField::OccurrenceId => self
                .occurrence_id
                .clone()
                .ok_or_else(|| ContextResolutionError::MissingField(field.clone()))?,
            GroupingField::Dimension(name) => self
                .identity_dimensions
                .get(name)
                .cloned()
                .ok_or_else(|| ContextResolutionError::MissingField(field.clone()))?,
            GroupingField::TimeDay => self.occurred_at.date().to_string(),
            GroupingField::TimeWeek => {
                let days = i64::from(self.occurred_at.date().weekday().number_days_from_monday());
                (self.occurred_at.date() - Duration::days(days)).to_string()
            }
        };
        field
            .validate_value(&value)
            .map_err(|reason| ContextResolutionError::InvalidValue {
                field: field.clone(),
                reason,
            })?;
        Ok(value)
    }
}
