//! Proposed infrastructure tag catalogue and deterministic rule classification.
//!
//! `alert_type`, `metric_source`, and `service` are system-owned. Component and team values are
//! accepted only as explicit metadata; they are never inferred from a rule name.

use common_types::{
    alert_management::{
        GroupingConfiguration, GroupingField, TagConfiguration, TagConfigurationV1,
    },
    alert_rules::stored::{AlertConfiguration, AlertQuery, CloudwatchQuery},
};

use super::tags::AlertTagNew;

pub const SYSTEM_TAG_KEYS: [&str; 3] = ["alert_type", "metric_source", "service"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagDescriptor {
    pub key: String,
    pub value: String,
    pub configuration: TagConfiguration,
}

impl TagDescriptor {
    fn classification(key: &str, value: &str) -> Self {
        Self {
            key: key.to_owned(),
            value: value.to_owned(),
            configuration: configuration(&[]),
        }
    }

    fn grouping(value: &str, fields: Vec<GroupingField>) -> Self {
        Self {
            key: "grouping".to_owned(),
            value: value.to_owned(),
            configuration: TagConfiguration::V1(TagConfigurationV1 {
                grouping: GroupingConfiguration { group_by: fields },
            }),
        }
    }

    pub fn into_new(self) -> Result<AlertTagNew, String> {
        AlertTagNew::try_new(self.key, self.value, self.configuration)
    }
}

/// The small proposed V1 catalogue. Creating it does not attach a grouping tag to any rule.
pub fn predefined_catalogue() -> Vec<TagDescriptor> {
    let mut tags = vec![
        TagDescriptor::classification("alert_type", "infra"),
        TagDescriptor::classification("metric_source", "cloudwatch"),
    ];
    tags.extend(SERVICES.map(|(_, service)| TagDescriptor::classification("service", service)));
    tags.extend([
        TagDescriptor::grouping("rule", vec![GroupingField::AlertRuleId]),
        TagDescriptor::grouping(
            "daily",
            vec![GroupingField::AlertRuleId, GroupingField::TimeDay],
        ),
        TagDescriptor::grouping(
            "weekly",
            vec![GroupingField::AlertRuleId, GroupingField::TimeWeek],
        ),
        TagDescriptor::grouping(
            "ec2_instance",
            vec![
                GroupingField::AlertRuleId,
                GroupingField::Dimension("InstanceId".to_owned()),
            ],
        ),
        TagDescriptor::grouping(
            "rds_instance",
            vec![
                GroupingField::AlertRuleId,
                GroupingField::Dimension("DBInstanceIdentifier".to_owned()),
            ],
        ),
        TagDescriptor::grouping(
            "occurrence",
            vec![GroupingField::AlertRuleId, GroupingField::OccurrenceId],
        ),
    ]);
    tags
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExplicitAssignments {
    pub component: Option<String>,
    pub team: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InfraTagAssignments {
    pub system: Vec<TagDescriptor>,
    pub explicit: Vec<TagDescriptor>,
    /// Present for an unknown namespace. Classification remains valid without a guessed service.
    pub missing_service_namespace: Option<String>,
}

/// Classify a typed stored rule and add only explicitly supplied component/team metadata.
pub fn assignments_for_rule(
    configuration: &AlertConfiguration,
    explicit: ExplicitAssignments,
) -> Result<InfraTagAssignments, String> {
    let AlertConfiguration::V1(configuration) = configuration;
    let AlertQuery::Cloudwatch(CloudwatchQuery::V1(query)) = &configuration.query;

    let mut system = vec![
        TagDescriptor::classification("alert_type", "infra"),
        TagDescriptor::classification("metric_source", "cloudwatch"),
    ];
    let service = service_for_namespace(&query.namespace);
    if let Some(service) = service {
        system.push(TagDescriptor::classification("service", service));
    }

    let explicit = [("component", explicit.component), ("team", explicit.team)]
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| TagDescriptor::classification(key, &value)))
        .map(|descriptor| {
            // Reuse the write model's normalization and bounds at this trust boundary.
            descriptor.clone().into_new().map(|new| {
                let stored =
                    diesel_models::observability::alert_manager::tags::AlertTagNew::from(new);
                TagDescriptor {
                    key: stored.tag_key,
                    value: stored.tag_value,
                    configuration: stored.configuration,
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(InfraTagAssignments {
        system,
        explicit,
        missing_service_namespace: service.is_none().then(|| query.namespace.clone()),
    })
}

pub fn service_for_namespace(namespace: &str) -> Option<&'static str> {
    SERVICES
        .iter()
        .find_map(|(candidate, service)| (*candidate == namespace).then_some(*service))
}

const SERVICES: [(&str, &str); 8] = [
    ("AWS/RDS", "rds"),
    ("AWS/EC2", "ec2"),
    ("AWS/ES", "opensearch"),
    ("AWS/ElastiCache", "elasticache"),
    ("AWS/ApplicationELB", "application_elb"),
    ("AWS/NetworkELB", "network_elb"),
    ("AWS/CloudFront", "cloudfront"),
    ("ContainerInsights", "container_insights"),
];

fn configuration(fields: &[GroupingField]) -> TagConfiguration {
    TagConfiguration::V1(TagConfigurationV1 {
        grouping: GroupingConfiguration {
            group_by: fields.to_vec(),
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    fn rule(namespace: &str) -> AlertConfiguration {
        serde_json::from_value(json!({
            "version": "v1",
            "interval_seconds": 60,
            "query": {
                "datasource": "cloudwatch", "version": "v1", "region": "us-east-1",
                "namespace": namespace, "metric_name": "CPUUtilization", "dimensions": {},
                "statistic": "Average", "period": 60
            },
            "thresholds": { "version": "v1", "severities": { "critical": {
                "threshold": 80.0, "comparison_operator": "GreaterThanThreshold",
                "description": "high", "destination": "infra"
            }}}
        }))
        .unwrap()
    }

    #[test]
    fn maps_only_exact_documented_namespaces() {
        for (namespace, service) in SERVICES {
            assert_eq!(service_for_namespace(namespace), Some(service));
        }
        assert_eq!(service_for_namespace("aws/rds"), None);
        assert_eq!(service_for_namespace("Custom/Clickhouse"), None);
    }

    #[test]
    fn unknown_namespace_is_visible_and_does_not_guess_from_names() {
        let assignments = assignments_for_rule(
            &rule("Custom/postgres-in-rule-name"),
            ExplicitAssignments::default(),
        )
        .unwrap();
        assert_eq!(assignments.system.len(), 2);
        assert_eq!(
            assignments.missing_service_namespace.as_deref(),
            Some("Custom/postgres-in-rule-name")
        );
        assert!(assignments.explicit.is_empty());
    }

    #[test]
    fn component_and_team_require_explicit_values() {
        let assignments = assignments_for_rule(
            &rule("AWS/EC2"),
            ExplicitAssignments {
                component: Some(" kafka ".to_owned()),
                team: Some("platform".to_owned()),
            },
        )
        .unwrap();
        assert!(assignments
            .explicit
            .iter()
            .any(|tag| tag.key == "component" && tag.value == "kafka"));
        assert!(assignments
            .explicit
            .iter()
            .any(|tag| tag.key == "team" && tag.value == "platform"));
    }

    #[test]
    fn catalogue_does_not_activate_grouping() {
        let catalogue = predefined_catalogue();
        assert_eq!(catalogue.len(), 16);
        assert!(catalogue
            .iter()
            .filter(|tag| tag.key == "grouping")
            .all(|tag| tag.configuration.is_grouping_enabled()));
    }
}
