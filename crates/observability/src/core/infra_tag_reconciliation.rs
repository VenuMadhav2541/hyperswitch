//! Explicit internal entry point for bounded, restartable infrastructure-tag reconciliation.
//!
//! This is deliberately not wired to startup, evaluation, or a public API.

use crate::{
    db::StorageInterface,
    domain_models::{
        infra_tags::{
            assignments_for_rule, predefined_catalogue, ExplicitAssignments, TagDescriptor,
            SYSTEM_TAG_KEYS,
        },
        tags::CreateTagOutcome,
    },
};

const MAX_BATCH_SIZE: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconciliationMode {
    Preview,
    Apply,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconciliationChange {
    pub alert_rule_id: String,
    pub additions: Vec<(String, String)>,
    pub removals: Vec<(String, String)>,
    pub missing_service_namespace: Option<String>,
    pub applied: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ReconciliationError {
    #[error("batch limit must be from 1 to {MAX_BATCH_SIZE}")]
    InvalidLimit,
    #[error("explicit infrastructure tag metadata is invalid: {0}")]
    InvalidAssignment(String),
    #[error("catalogue tag `{key}={value}` exists with an incompatible configuration")]
    CatalogueConflict { key: String, value: String },
    #[error("infrastructure tag reconciliation storage operation failed")]
    Storage,
}

/// Explicitly preview or create the proposed catalogue. Existing compatible rows are unchanged.
pub async fn bootstrap_catalogue(
    store: &dyn StorageInterface,
    mode: ReconciliationMode,
) -> Result<Vec<(String, String)>, ReconciliationError> {
    let existing = store
        .list_tags()
        .await
        .map_err(|_| ReconciliationError::Storage)?;
    let catalogue = predefined_catalogue();
    for descriptor in &catalogue {
        if let Some(tag) = existing
            .iter()
            .find(|tag| tag.tag_key == descriptor.key && tag.tag_value == descriptor.value)
        {
            if tag.configuration != descriptor.configuration {
                return Err(ReconciliationError::CatalogueConflict {
                    key: descriptor.key.clone(),
                    value: descriptor.value.clone(),
                });
            }
        }
    }
    let additions = catalogue
        .iter()
        .filter(|descriptor| {
            !existing
                .iter()
                .any(|tag| tag.tag_key == descriptor.key && tag.tag_value == descriptor.value)
        })
        .map(|descriptor| (descriptor.key.clone(), descriptor.value.clone()))
        .collect::<Vec<_>>();
    if mode == ReconciliationMode::Apply {
        for descriptor in catalogue {
            let key = descriptor.key.clone();
            let value = descriptor.value.clone();
            let expected = descriptor.configuration.clone();
            match store
                .create_tag(
                    descriptor
                        .into_new()
                        .map_err(ReconciliationError::InvalidAssignment)?,
                )
                .await
                .map_err(|_| ReconciliationError::Storage)?
            {
                CreateTagOutcome::Created(_) => {}
                CreateTagOutcome::AlreadyExists => {
                    let compatible = store
                        .list_tags()
                        .await
                        .map_err(|_| ReconciliationError::Storage)?
                        .into_iter()
                        .any(|tag| {
                            tag.tag_key == key
                                && tag.tag_value == value
                                && tag.configuration == expected
                        });
                    if !compatible {
                        return Err(ReconciliationError::CatalogueConflict { key, value });
                    }
                }
            }
        }
    }
    Ok(additions)
}

/// Preview or apply one stable, ID-ordered page. Call again with the final rule ID as `after`.
pub async fn reconcile_batch(
    store: &dyn StorageInterface,
    after: Option<&str>,
    limit: usize,
    mode: ReconciliationMode,
    explicit_for_rule: impl Fn(&str) -> ExplicitAssignments,
) -> Result<Vec<ReconciliationChange>, ReconciliationError> {
    if !(1..=MAX_BATCH_SIZE).contains(&limit) {
        return Err(ReconciliationError::InvalidLimit);
    }
    let limit = i64::try_from(limit).map_err(|_| ReconciliationError::InvalidLimit)?;
    let rules = store
        .list_infra_reconciliation_batch(after, limit)
        .await
        .map_err(|_| ReconciliationError::Storage)?;
    let mut changes = Vec::with_capacity(rules.len());
    for rule in rules {
        let assignments =
            assignments_for_rule(&rule.alerts_configuration, explicit_for_rule(&rule.id))
                .map_err(ReconciliationError::InvalidAssignment)?;
        let current = store
            .list_rule_tags(&rule.id)
            .await
            .map_err(|_| ReconciliationError::Storage)?;
        let mut additions = desired(&assignments.system, &assignments.explicit)
            .into_iter()
            .filter(|pair| {
                !current
                    .iter()
                    .any(|tag| tag.tag_key == pair.0 && tag.tag_value == pair.1)
            })
            .collect::<Vec<_>>();
        let mut removals = current
            .iter()
            .filter(|tag| SYSTEM_TAG_KEYS.contains(&tag.tag_key.as_str()))
            .map(|tag| (tag.tag_key.clone(), tag.tag_value.clone()))
            .filter(|pair| {
                !assignments
                    .system
                    .iter()
                    .any(|tag| tag.key == pair.0 && tag.value == pair.1)
            })
            .collect::<Vec<_>>();
        additions.sort();
        removals.sort();

        let applied = if mode == ReconciliationMode::Apply {
            store
                .reconcile_infra_tags(&rule.id, assignments.clone())
                .await
                .map_err(|_| ReconciliationError::Storage)?
        } else {
            false
        };
        changes.push(ReconciliationChange {
            alert_rule_id: rule.id,
            additions,
            removals,
            missing_service_namespace: assignments.missing_service_namespace,
            applied,
        });
    }
    Ok(changes)
}

fn desired(system: &[TagDescriptor], explicit: &[TagDescriptor]) -> Vec<(String, String)> {
    system
        .iter()
        .chain(explicit)
        .map(|tag| (tag.key.clone(), tag.value.clone()))
        .collect()
}
