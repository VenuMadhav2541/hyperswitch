//! Strict, versioned JSON configurations stored by alert management.
//!
//! These types are the persistence/runtime boundary shared by Diesel and the observability
//! service. Deserialization checks the wire shape; callers must also invoke `validate` before a
//! value is written.

use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use diesel::{sql_types::Jsonb, AsExpression, FromSqlRow};
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use time::{Date, Month, Weekday};
use uuid::Uuid;

/// Maximum serialized size of either stored configuration.
pub const MAX_CONFIGURATION_BYTES: usize = 64 * 1024;
/// Maximum fields used to form one grouping key.
pub const MAX_GROUPING_FIELDS: usize = 16;
/// Maximum tags selected by one silence.
pub const MAX_SILENCE_TAGS: usize = 64;
/// Maximum explicit field/value restrictions on one silence.
pub const MAX_GROUP_VALUES: usize = 16;
/// Maximum length of a normalized dimension name.
pub const MAX_DIMENSION_NAME_LENGTH: usize = 64;
/// Maximum length of an opaque dimension or occurrence value.
pub const MAX_GROUP_VALUE_LENGTH: usize = 255;

/// A normalized field that grouping and silence matching can resolve.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GroupingField {
    /// The stable alert rule identifier.
    AlertRuleId,
    /// The stable firing-episode identifier.
    OccurrenceId,
    /// A named normalized resource dimension.
    Dimension(String),
    /// The UTC calendar day (`YYYY-MM-DD`).
    TimeDay,
    /// The Monday starting a UTC week (`YYYY-MM-DD`).
    TimeWeek,
}

impl GroupingField {
    /// Validate a resolved value for this field.
    pub fn validate_value(&self, value: &str) -> Result<(), String> {
        match self {
            Self::AlertRuleId => validate_rule_id(value),
            Self::OccurrenceId | Self::Dimension(_) => {
                if value.trim().is_empty() || value.chars().count() > MAX_GROUP_VALUE_LENGTH {
                    Err(format!(
                        "group_values.{self} must be 1 to {MAX_GROUP_VALUE_LENGTH} characters"
                    ))
                } else {
                    Ok(())
                }
            }
            Self::TimeDay => parse_iso_date(value)
                .map(|_| ())
                .map_err(|reason| format!("group_values.{self} {reason}")),
            Self::TimeWeek => {
                let date = parse_iso_date(value)
                    .map_err(|reason| format!("group_values.{self} {reason}"))?;
                if date.weekday() == Weekday::Monday {
                    Ok(())
                } else {
                    Err(format!("group_values.{self} must be a Monday"))
                }
            }
        }
    }
}

impl fmt::Display for GroupingField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlertRuleId => formatter.write_str("alert_rule_id"),
            Self::OccurrenceId => formatter.write_str("occurrence_id"),
            Self::Dimension(name) => write!(formatter, "dimensions.{name}"),
            Self::TimeDay => formatter.write_str("time.day"),
            Self::TimeWeek => formatter.write_str("time.week"),
        }
    }
}

impl FromStr for GroupingField {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "alert_rule_id" => Ok(Self::AlertRuleId),
            "occurrence_id" => Ok(Self::OccurrenceId),
            "time.day" => Ok(Self::TimeDay),
            "time.week" => Ok(Self::TimeWeek),
            _ => {
                let Some(name) = value.strip_prefix("dimensions.") else {
                    return Err(format!("unsupported grouping field `{value}`"));
                };
                if name.trim().is_empty()
                    || name != name.trim()
                    || name.chars().count() > MAX_DIMENSION_NAME_LENGTH
                    || name.chars().any(char::is_control)
                {
                    return Err(format!(
                        "dimension name must be 1 to {MAX_DIMENSION_NAME_LENGTH} non-control characters without surrounding whitespace"
                    ));
                }
                Ok(Self::Dimension(name.to_owned()))
            }
        }
    }
}

impl Serialize for GroupingField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for GroupingField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// Tag-owned grouping behavior. An empty list means classification only.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupingConfiguration {
    /// Fields forming a grouping key, in canonical order.
    pub group_by: Vec<GroupingField>,
}

/// The versioned `alert_tags.configuration` column.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, AsExpression, FromSqlRow)]
#[diesel(sql_type = Jsonb)]
#[serde(tag = "version", rename_all = "snake_case")]
pub enum TagConfiguration {
    /// The first tag configuration version.
    V1(TagConfigurationV1),
}

common_utils::impl_to_sql_from_sql_json!(TagConfiguration);

/// Version 1 of a tag configuration.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TagConfigurationV1 {
    /// Optional grouping behavior, explicitly represented even when empty.
    pub grouping: GroupingConfiguration,
}

impl TagConfiguration {
    /// Validate grouping bounds, uniqueness, and serialized size.
    pub fn validate(&self) -> Result<(), String> {
        let Self::V1(configuration) = self;
        let fields = &configuration.grouping.group_by;
        if fields.len() > MAX_GROUPING_FIELDS {
            return Err(format!(
                "configuration.grouping.group_by allows at most {MAX_GROUPING_FIELDS} fields"
            ));
        }
        if fields.iter().collect::<BTreeSet<_>>().len() != fields.len() {
            return Err("configuration.grouping.group_by contains duplicate fields".to_owned());
        }
        validate_size(self, "configuration")
    }

    /// Whether this tag contributes grouping behavior.
    pub fn is_grouping_enabled(&self) -> bool {
        let Self::V1(configuration) = self;
        !configuration.grouping.group_by.is_empty()
    }

    /// The fields selected by this configuration.
    pub fn group_by(&self) -> &[GroupingField] {
        let Self::V1(configuration) = self;
        &configuration.grouping.group_by
    }
}

/// The versioned `alert_silences.configuration` column.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, AsExpression, FromSqlRow)]
#[diesel(sql_type = Jsonb)]
#[serde(tag = "version", rename_all = "snake_case")]
pub enum SilenceConfiguration {
    /// The first silence configuration version.
    V1(SilenceConfigurationV1),
}

common_utils::impl_to_sql_from_sql_json!(SilenceConfiguration);

/// Version 1 of a silence selection.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SilenceConfigurationV1 {
    /// Every listed tag must be attached to a rule for the silence to match.
    pub tag_ids: Vec<Uuid>,
    /// Every listed normalized field must equal its resolved value.
    #[serde(deserialize_with = "deserialize_group_values")]
    pub group_values: BTreeMap<GroupingField, String>,
}

impl SilenceConfiguration {
    /// Validate selection bounds, uniqueness, values, and serialized size.
    pub fn validate(&self) -> Result<(), String> {
        let Self::V1(configuration) = self;
        if configuration.tag_ids.is_empty() && configuration.group_values.is_empty() {
            return Err("configuration must select at least one tag_id or group_value".to_owned());
        }
        if configuration.tag_ids.len() > MAX_SILENCE_TAGS {
            return Err(format!(
                "configuration.tag_ids allows at most {MAX_SILENCE_TAGS} entries"
            ));
        }
        if configuration.tag_ids.iter().collect::<BTreeSet<_>>().len()
            != configuration.tag_ids.len()
        {
            return Err("configuration.tag_ids contains duplicate UUIDs".to_owned());
        }
        if configuration.group_values.len() > MAX_GROUP_VALUES {
            return Err(format!(
                "configuration.group_values allows at most {MAX_GROUP_VALUES} entries"
            ));
        }
        configuration
            .group_values
            .iter()
            .try_for_each(|(field, value)| field.validate_value(value))?;
        validate_size(self, "configuration")
    }

    /// Referenced tag IDs in request order.
    pub fn tag_ids(&self) -> &[Uuid] {
        let Self::V1(configuration) = self;
        &configuration.tag_ids
    }

    /// Explicit normalized field restrictions.
    pub fn group_values(&self) -> &BTreeMap<GroupingField, String> {
        let Self::V1(configuration) = self;
        &configuration.group_values
    }

    /// An explicitly selected rule ID, when present.
    pub fn alert_rule_id(&self) -> Option<&str> {
        self.group_values()
            .get(&GroupingField::AlertRuleId)
            .map(String::as_str)
    }
}

fn validate_size<T: Serialize>(value: &T, path: &str) -> Result<(), String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| format!("{path} could not be serialized for storage"))?;
    if bytes.len() > MAX_CONFIGURATION_BYTES {
        Err(format!(
            "{path} must be at most {MAX_CONFIGURATION_BYTES} serialized bytes"
        ))
    } else {
        Ok(())
    }
}

fn validate_rule_id(value: &str) -> Result<(), String> {
    const MAX_RULE_ID_LENGTH: usize = 64;
    let leading = |character: char| character.is_ascii_lowercase() || character.is_ascii_digit();
    let valid = (1..=MAX_RULE_ID_LENGTH).contains(&value.len())
        && value.starts_with(leading)
        && value
            .chars()
            .all(|character| leading(character) || character == '-' || character == '_');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "group_values.alert_rule_id must be 1 to {MAX_RULE_ID_LENGTH} lowercase letters, digits, `-` or `_`, starting with a letter or digit"
        ))
    }
}

fn parse_iso_date(value: &str) -> Result<Date, &'static str> {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| index != 4 && index != 7 && !byte.is_ascii_digit())
    {
        return Err("must be a valid ISO date in YYYY-MM-DD form");
    }
    let year = value[0..4]
        .parse::<i32>()
        .map_err(|_| "must be a valid ISO date in YYYY-MM-DD form")?;
    let month = value[5..7]
        .parse::<u8>()
        .ok()
        .and_then(|number| Month::try_from(number).ok())
        .ok_or("must be a valid ISO date in YYYY-MM-DD form")?;
    let day = value[8..10]
        .parse::<u8>()
        .map_err(|_| "must be a valid ISO date in YYYY-MM-DD form")?;
    Date::from_calendar_date(year, month, day)
        .map_err(|_| "must be a valid ISO date in YYYY-MM-DD form")
}

fn deserialize_group_values<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<GroupingField, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct GroupValuesVisitor;

    impl<'de> Visitor<'de> for GroupValuesVisitor {
        type Value = BTreeMap<GroupingField, String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an object of unique grouping fields and string values")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut values = BTreeMap::new();
            while let Some((field, value)) = map.next_entry::<GroupingField, String>()? {
                match values.entry(field) {
                    Entry::Vacant(entry) => {
                        entry.insert(value);
                    }
                    Entry::Occupied(entry) => {
                        return Err(de::Error::custom(format!(
                            "duplicate group_values key `{}`",
                            entry.key()
                        )));
                    }
                }
            }
            Ok(values)
        }
    }

    deserializer.deserialize_map(GroupValuesVisitor)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn grouping_fields_are_canonical_strings_and_object_keys() {
        let fields = [
            (GroupingField::AlertRuleId, "alert_rule_id"),
            (GroupingField::OccurrenceId, "occurrence_id"),
            (
                GroupingField::Dimension("InstanceId".to_owned()),
                "dimensions.InstanceId",
            ),
            (GroupingField::TimeDay, "time.day"),
            (GroupingField::TimeWeek, "time.week"),
        ];
        for (field, wire) in fields {
            assert_eq!(field.to_string(), wire);
            assert_eq!(serde_json::to_value(&field).unwrap(), json!(wire));
            assert_eq!(wire.parse::<GroupingField>().unwrap(), field);
        }
        for invalid in ["", "dimensions.", "dimension.x", "time.month"] {
            assert!(invalid.parse::<GroupingField>().is_err(), "{invalid}");
        }
    }

    #[test]
    fn tag_configuration_requires_the_complete_strict_shape() {
        let value = json!({"version":"v1","grouping":{"group_by":[]}});
        let parsed: TagConfiguration = serde_json::from_value(value.clone()).unwrap();
        parsed.validate().unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), value);

        for invalid in [
            json!({"version":"v1","grouping":{}}),
            json!({"version":"v1"}),
            json!({"version":"v1","grouping":{"group_by":null}}),
            json!({"version":"v1","grouping":{"group_by":[],"extra":true}}),
            json!({"version":"v2","grouping":{"group_by":[]}}),
        ] {
            assert!(serde_json::from_value::<TagConfiguration>(invalid).is_err());
        }
    }

    #[test]
    fn semantic_duplicates_are_rejected() {
        let tag: TagConfiguration = serde_json::from_value(json!({
            "version":"v1",
            "grouping":{"group_by":["alert_rule_id","alert_rule_id"]}
        }))
        .unwrap();
        assert!(tag.validate().unwrap_err().contains("duplicate"));

        let id = "11111111-1111-4111-8111-111111111111";
        let silence: SilenceConfiguration = serde_json::from_value(json!({
            "version":"v1", "tag_ids":[id,id], "group_values":{}
        }))
        .unwrap();
        assert!(silence.validate().unwrap_err().contains("duplicate"));
    }

    #[test]
    fn silence_configuration_requires_complete_meaningful_selection() {
        let value = json!({
            "version":"v1",
            "tag_ids":["11111111-1111-4111-8111-111111111111"],
            "group_values":{"alert_rule_id":"postgres-cpu","time.day":"2026-10-25"}
        });
        let parsed: SilenceConfiguration = serde_json::from_value(value.clone()).unwrap();
        parsed.validate().unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), value);

        let empty: SilenceConfiguration = serde_json::from_value(json!({
            "version":"v1", "tag_ids":[], "group_values":{}
        }))
        .unwrap();
        assert!(empty.validate().is_err());

        for invalid in [
            json!({"version":"v1","tag_ids":[]}),
            json!({"version":"v1","group_values":{"alert_rule_id":"rule"}}),
            json!({"version":"v1","tag_ids":null,"group_values":{}}),
            json!({"version":"v1","tag_ids":["not-a-uuid"],"group_values":{}}),
            json!({"version":"v1","tag_ids":[],"group_values":{"unknown":"x"}}),
            json!({"version":"v1","tag_ids":[],"group_values":{},"extra":true}),
        ] {
            assert!(serde_json::from_value::<SilenceConfiguration>(invalid).is_err());
        }
    }

    #[test]
    fn duplicate_raw_object_keys_are_rejected() {
        let duplicate = r#"{
            "version":"v1",
            "tag_ids":["11111111-1111-4111-8111-111111111111"],
            "group_values":{"time.day":"2026-10-25","time.day":"2026-10-26"}
        }"#;
        assert!(serde_json::from_str::<SilenceConfiguration>(duplicate).is_err());

        let duplicate_field = r#"{
            "version":"v1", "tag_ids":[], "tag_ids":[],
            "group_values":{"alert_rule_id":"rule"}
        }"#;
        assert!(serde_json::from_str::<SilenceConfiguration>(duplicate_field).is_err());
    }

    #[test]
    fn group_values_are_field_validated() {
        for (field, good, bad) in [
            (GroupingField::AlertRuleId, "postgres-cpu", "Postgres CPU"),
            (GroupingField::OccurrenceId, "episode-1", ""),
            (
                GroupingField::Dimension("InstanceId".to_owned()),
                "db-1",
                " ",
            ),
            (GroupingField::TimeDay, "2026-10-25", "2026-02-30"),
            (GroupingField::TimeWeek, "2026-10-26", "2026-10-25"),
        ] {
            assert!(field.validate_value(good).is_ok(), "{field}: {good}");
            assert!(field.validate_value(bad).is_err(), "{field}: {bad}");
        }
    }

    #[test]
    fn explicit_empty_containers_survive_round_trip() {
        let tag: TagConfiguration =
            serde_json::from_str(r#"{"version":"v1","grouping":{"group_by":[]}}"#).unwrap();
        let encoded = serde_json::to_value(tag).unwrap();
        assert_eq!(encoded["grouping"]["group_by"], Value::Array(Vec::new()));

        let silence: SilenceConfiguration = serde_json::from_str(
            r#"{"version":"v1","tag_ids":[],"group_values":{"alert_rule_id":"rule"}}"#,
        )
        .unwrap();
        assert_eq!(serde_json::to_value(silence).unwrap()["tag_ids"], json!([]));
    }
}
