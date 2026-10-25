//! Pure grouping-key resolution and silence matching. This module does not route or send alerts.

use std::collections::BTreeMap;

use common_types::alert_management::{GroupingField, SilenceConfiguration};

use crate::domain_models::{
    alert_context::{AlertContext, ContextResolutionError},
    silences::AlertSilence,
};

/// Canonical ordered field/value pairs. No delimiter-based string encoding is used.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GroupKey(Vec<(GroupingField, String)>);

impl GroupKey {
    pub fn values(&self) -> &[(GroupingField, String)] {
        &self.0
    }
}

/// Resolve fields once, sorting them so semantically equivalent field sets have one identity.
pub fn resolve_group_key(
    context: &AlertContext,
    fields: &[GroupingField],
) -> Result<GroupKey, ContextResolutionError> {
    let mut fields = fields.to_vec();
    fields.sort();
    fields.dedup();
    fields
        .into_iter()
        .map(|field| context.resolve(&field).map(|value| (field, value)))
        .collect::<Result<Vec<_>, _>>()
        .map(GroupKey)
}

/// A silence selects all listed tags (AND) and all fixed group values (AND).
pub fn silence_matches(
    context: &AlertContext,
    configuration: &SilenceConfiguration,
) -> Result<bool, ContextResolutionError> {
    let resolved = resolve_values(context, configuration.group_values().keys())?;
    Ok(configuration
        .tag_ids()
        .iter()
        .all(|id| context.attached_tag_ids.contains(id))
        && configuration
            .group_values()
            .iter()
            .all(|(field, expected)| resolved.get(field) == Some(expected)))
}

/// Separate active silences are alternatives (OR). Missing required context remains an error.
pub fn matches_any_silence(
    context: &AlertContext,
    silences: &[AlertSilence],
) -> Result<bool, ContextResolutionError> {
    for silence in silences {
        if silence_matches(context, &silence.configuration)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn resolve_values<'a>(
    context: &AlertContext,
    fields: impl Iterator<Item = &'a GroupingField>,
) -> Result<BTreeMap<GroupingField, String>, ContextResolutionError> {
    fields
        .map(|field| context.resolve(field).map(|value| (field.clone(), value)))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use common_types::alert_management::SilenceConfiguration;
    use serde_json::json;
    use time::macros::datetime;
    use uuid::Uuid;

    use super::*;

    fn context(at: time::PrimitiveDateTime) -> AlertContext {
        AlertContext {
            rule_id: "infra-cpu".to_owned(),
            identity_dimensions: BTreeMap::from([
                ("InstanceId".to_owned(), "i-123".to_owned()),
                ("instanceid".to_owned(), "wrong-case".to_owned()),
            ]),
            occurred_at: at,
            occurrence_id: None,
            attached_tag_ids: BTreeSet::new(),
        }
    }

    fn silence(tag_ids: Vec<Uuid>, values: serde_json::Value) -> SilenceConfiguration {
        serde_json::from_value(json!({
            "version": "v1", "tag_ids": tag_ids, "group_values": values
        }))
        .unwrap()
    }

    #[test]
    fn key_is_canonical_and_dimensions_are_exact() {
        let context = context(datetime!(2026-10-25 23:59:59));
        let first = resolve_group_key(
            &context,
            &[
                GroupingField::TimeDay,
                GroupingField::Dimension("InstanceId".to_owned()),
                GroupingField::AlertRuleId,
            ],
        )
        .unwrap();
        let second = resolve_group_key(
            &context,
            &[
                GroupingField::AlertRuleId,
                GroupingField::Dimension("InstanceId".to_owned()),
                GroupingField::TimeDay,
            ],
        )
        .unwrap();
        assert_eq!(first, second);
        assert!(first
            .values()
            .contains(&(GroupingField::TimeDay, "2026-10-25".to_owned())));
    }

    #[test]
    fn utc_day_and_monday_week_roll_over_from_captured_time() {
        let sunday = context(datetime!(2026-10-25 23:59:59));
        let monday = context(datetime!(2026-10-26 00:00:00));
        assert_eq!(
            sunday.resolve(&GroupingField::TimeWeek).unwrap(),
            "2026-10-19"
        );
        assert_eq!(
            monday.resolve(&GroupingField::TimeWeek).unwrap(),
            "2026-10-26"
        );
        assert_ne!(
            sunday.resolve(&GroupingField::TimeDay).unwrap(),
            monday.resolve(&GroupingField::TimeDay).unwrap()
        );
    }

    #[test]
    fn missing_requested_fields_error_instead_of_collapsing() {
        let context = context(datetime!(2026-10-25 10:00));
        assert_eq!(
            context.resolve(&GroupingField::OccurrenceId),
            Err(ContextResolutionError::MissingField(
                GroupingField::OccurrenceId
            ))
        );
        assert!(context
            .resolve(&GroupingField::Dimension("missing".to_owned()))
            .is_err());
    }

    #[test]
    fn silence_uses_tag_and_group_and_semantics() {
        let first = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let second = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mut context = context(datetime!(2026-10-25 10:00));
        context.attached_tag_ids.insert(first);
        let configuration = silence(
            vec![first, second],
            json!({"alert_rule_id":"infra-cpu", "dimensions.InstanceId":"i-123"}),
        );
        assert!(!silence_matches(&context, &configuration).unwrap());
        context.attached_tag_ids.insert(second);
        assert!(silence_matches(&context, &configuration).unwrap());
    }

    #[test]
    fn fixed_silence_time_does_not_move() {
        let configuration = silence(vec![], json!({"time.day":"2026-10-25"}));
        assert!(silence_matches(&context(datetime!(2026-10-25 23:59:59)), &configuration).unwrap());
        assert!(
            !silence_matches(&context(datetime!(2026-10-26 00:00:00)), &configuration).unwrap()
        );
    }
}
