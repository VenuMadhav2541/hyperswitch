pub mod grafana_cloudwatch;
pub mod grafana_loki;

use common_types::alert_rules::MetricSource;
use common_utils::date_time;
use error_stack::{AttachmentKind, FrameKind, Report, ResultExt};
use external_services::metrics_provider::{
    grafana::cloudwatch::GrafanaCloudwatch, MetricRange, MetricSeries, MetricsProvider,
};
use futures::future::join_all;
use hyperswitch_masking::Secret;
use time::PrimitiveDateTime;

use crate::{
    core::notifier,
    domain::notifier::Outcome,
    domain_models::alert_rules::next_run_after,
    domain_models::evaluator::{
        Delivery, Evaluation, EvaluatorError, EvaluatorResult, Notification, RuleDefinition,
        RuleRun,
    },
    errors::{ObservabilityApiResult, ObservabilityError},
    logger,
    services::chat,
    settings::evaluator::{AlertSettings, RuleSource},
    state::AppState,
    types::{AlertEvaluationResponse, AlertRuleEvaluationResponse, ChatNotifyRequest},
};

pub trait AlertEvaluator: MetricsProvider {
    type Evaluation;

    fn prepare_request(
        &self,
        rule: &RuleDefinition,
        datasource_uid: &str,
        now: PrimitiveDateTime,
    ) -> EvaluatorResult<(MetricRange, Self::MetricsRequest)>;

    fn evaluate(
        &self,
        rule: &RuleDefinition,
        range: MetricRange,
        series: Vec<MetricSeries>,
    ) -> EvaluatorResult<Self::Evaluation>;
}

pub async fn notify(state: AppState, _: ()) -> ObservabilityApiResult<AlertEvaluationResponse> {
    let provider = GrafanaCloudwatch::new(
        state.conf.alert.get_inner().grafana.clone(),
        state.conf.proxy.clone(),
    );
    alert_processor(&provider, &state, true).await
}

pub async fn evaluate(state: AppState, _: ()) -> ObservabilityApiResult<AlertEvaluationResponse> {
    let provider = GrafanaCloudwatch::new(
        state.conf.alert.get_inner().grafana.clone(),
        state.conf.proxy.clone(),
    );
    alert_processor(&provider, &state, false).await
}

async fn alert_processor<O: AlertEvaluator<Evaluation = Evaluation>>(
    operation: &O,
    state: &AppState,
    deliver: bool,
) -> ObservabilityApiResult<AlertEvaluationResponse> {
    let settings = state.conf.alert.get_inner();
    let now = date_time::now();
    let definitions = load_rules(state, MetricSource::GrafanaCloudwatch, deliver, now).await?;
    let rules = definitions
        .iter()
        .filter(|rule| rule.config.query.metric_source() == MetricSource::GrafanaCloudwatch)
        .map(|rule| {
            let destination = rule
                .config
                .thresholds
                .severities
                .first_key_value()
                .map(|(_, threshold)| threshold.destination.as_str())
                .ok_or_else(|| {
                    Report::new(ObservabilityError::InternalServerError)
                        .attach_printable("rule must configure at least one threshold")
                })?;
            Ok((rule, destination))
        })
        .collect::<ObservabilityApiResult<Vec<_>>>()?;

    let reports = join_all(rules.into_iter().map(|(rule, error_destination)| {
        process_rule(
            operation,
            state,
            settings,
            rule,
            error_destination,
            now,
            deliver,
        )
    }))
    .await;

    Ok(AlertEvaluationResponse {
        evaluated_at: now,
        rules: reports,
    })
}

pub(super) async fn load_rules(
    state: &AppState,
    source: MetricSource,
    claim_due: bool,
    now: PrimitiveDateTime,
) -> ObservabilityApiResult<Vec<RuleDefinition>> {
    load_selected_rules(state, source, claim_due, now, &[]).await
}

pub(super) async fn load_selected_rules(
    state: &AppState,
    source: MetricSource,
    claim_due: bool,
    now: PrimitiveDateTime,
    selected: &[String],
) -> ObservabilityApiResult<Vec<RuleDefinition>> {
    let settings = state.conf.alert.get_inner();
    if settings.rule_source == RuleSource::Config {
        return Ok(settings
            .rules
            .iter()
            .filter(|r| {
                r.config.query.metric_source() == source
                    && (selected.is_empty() || selected.contains(&r.id))
            })
            .cloned()
            .collect());
    }
    let rows = if claim_due {
        state.store.list_due_alert_rules(now).await
    } else {
        state.store.list_active_alert_rules().await
    }
    .change_context(ObservabilityError::InternalServerError)
    .attach_printable("Failed to load database alert rules")?;
    let mut definitions = Vec::new();
    for row in rows {
        let seen = row.next_run_at;
        let definition = RuleDefinition::from(row);
        if definition.config.query.metric_source() != source
            || (!selected.is_empty() && !selected.contains(&definition.id))
        {
            continue;
        }
        if matches!(&definition.config.query, common_types::alert_rules::AlertQuery::GrafanaLoki(query) if !query.delivery_enabled || query.pending_seconds != 0)
        {
            definitions.push(definition);
            continue;
        }
        if claim_due
            && !state
                .store
                .claim_alert_rule(
                    &definition.id,
                    seen,
                    next_run_after(now, definition.config.interval_seconds),
                )
                .await
                .change_context(ObservabilityError::InternalServerError)?
        {
            continue;
        }
        definitions.push(definition);
    }
    Ok(definitions)
}

async fn process_rule<O: AlertEvaluator<Evaluation = Evaluation>>(
    operation: &O,
    state: &AppState,
    settings: &AlertSettings,
    rule: &RuleDefinition,
    error_destination: &str,
    now: PrimitiveDateTime,
    deliver: bool,
) -> AlertRuleEvaluationResponse {
    let result = async {
        let datasource = settings
            .datasources
            .get(&MetricSource::GrafanaCloudwatch)
            .ok_or(EvaluatorError::UnexpectedResponse)
            .attach_printable("rule metric source is not configured")?;
        let (range, request) = operation.prepare_request(rule, &datasource.uid, now)?;
        let series = operation
            .fetch_metrics(request)
            .await
            .change_context(EvaluatorError::DatasourceRequestFailed)?;
        operation.evaluate(rule, range, series)
    }
    .await;

    let run = build_rule_run(rule, now, result, error_destination);
    let notification = match &run {
        RuleRun::Evaluated { notification, .. } => notification.as_ref(),
        RuleRun::Failed { notification, .. } => Some(notification),
    };
    let delivery = match (deliver, notification) {
        (true, Some(notification)) => Some(deliver_notification(state.clone(), notification).await),
        _ => None,
    };
    AlertRuleEvaluationResponse::from((rule, run, delivery))
}

fn build_rule_run(
    rule: &RuleDefinition,
    now: PrimitiveDateTime,
    result: EvaluatorResult<Evaluation>,
    error_destination: &str,
) -> RuleRun {
    let result = result.and_then(|evaluation| {
        let notification = match (&evaluation.breach, evaluation.latest_point.as_ref()) {
            (Some(threshold), Some(point)) => Some(chat::create_notification(
                rule,
                now,
                chat::ChatBody::Alarm {
                    evaluation: &evaluation,
                    threshold,
                    point,
                },
            )?),
            (Some(_), None) => {
                return Err(Report::new(EvaluatorError::UnexpectedResponse)
                    .attach_printable("a measured breach is missing its current datapoint"));
            }
            (None, _) => None,
        };
        Ok(RuleRun::Evaluated {
            evaluation,
            notification,
        })
    });
    match result {
        Ok(run) => run,
        Err(error) => {
            let error = format_error_report(&error);
            logger::error!(tag = "evaluator_rule_error", rule_id = %rule.id, error = %error, "Rule evaluation failed");
            RuleRun::Failed {
                notification: chat::create_notification(
                    rule,
                    now,
                    chat::ChatBody::RuleError {
                        error: &error,
                        destination: error_destination,
                    },
                )
                .unwrap_or_else(|_| Notification {
                    heading: "RULE ERROR".to_owned(),
                    destination: error_destination.to_owned(),
                    message: "CloudWatch notification received an incompatible query type"
                        .to_owned(),
                }),
                error,
            }
        }
    }
}

async fn deliver_notification(state: AppState, notification: &Notification) -> Delivery {
    match notifier::notify_chat(
        state,
        &notification.destination,
        ChatNotifyRequest {
            text: Secret::new(notification.message.clone()),
            reply_to: None,
        },
    )
    .await
    {
        Ok(Outcome::Delivered(receipt)) => {
            logger::info!(tag = "evaluator_notification", heading = %notification.heading, destination = %notification.destination, message_id = ?receipt.message_id, "Evaluator notification delivered");
            Delivery {
                delivered: true,
                error: None,
            }
        }
        Ok(Outcome::Refused(refusal)) => {
            let error = format!("chat destination refused notification: {}", refusal.code);
            logger::error!(tag = "evaluator_notification", heading = %notification.heading, destination = %notification.destination, error = %error, "Evaluator notification refused");
            Delivery {
                delivered: false,
                error: Some(error),
            }
        }
        Err(error) => {
            let error = format_error_report(&error);
            logger::error!(tag = "evaluator_notification", heading = %notification.heading, destination = %notification.destination, error = %error, "Evaluator notification failed");
            Delivery {
                delivered: false,
                error: Some(error),
            }
        }
    }
}

fn format_error_report<C: error_stack::Context>(error: &Report<C>) -> String {
    let mut parts = Vec::new();
    for frame in error.frames() {
        let part = match frame.kind() {
            FrameKind::Context(context) => Some(context.to_string()),
            FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
                Some(attachment.to_string())
            }
            FrameKind::Attachment(_) => None,
        };
        if let Some(part) = part {
            if !part.is_empty() && parts.last() != Some(&part) {
                parts.push(part);
            }
        }
    }
    parts.join(": ")
}

#[cfg(test)]
mod tests {
    use external_services::metrics_provider::MetricPoint;
    use time::macros::datetime;

    use super::*;
    use crate::domain_models::evaluator::AlertEvaluationLevel;

    fn rule() -> RuleDefinition {
        serde_json::from_value(serde_json::json!({
            "id": "requests", "name": "Requests",
            "config": {
                "interval_seconds": 60,
                "query": {
                    "datasource": "grafana_cloudwatch", "region": "ap-south-2",
                    "namespace": "AWS/ApplicationELB", "metric_name": "RequestCount",
                    "statistic": "Sum", "period": 60
                },
                "thresholds": {"severities": {"critical": {
                    "threshold": 1.0, "comparison_operator": "GreaterThanThreshold",
                    "description": "requests", "destination": "oncall"
                }}}
            }
        }))
        .unwrap()
    }

    #[test]
    fn missing_selected_breach_point_is_a_rule_error_not_silence() {
        let rule = rule();
        let now = datetime!(2026-03-18 10:08:00);
        let evaluation = Evaluation {
            level: AlertEvaluationLevel::Critical,
            latest_point: None,
            datapoints: vec![],
            breach: rule
                .config
                .thresholds
                .severities
                .first_key_value()
                .map(|(_, threshold)| threshold.clone()),
            window: MetricRange { from: now, to: now },
        };
        let RuleRun::Failed {
            error,
            notification,
        } = build_rule_run(&rule, now, Ok(evaluation), "oncall")
        else {
            panic!("a breach without a selected point must be a rule error");
        };
        assert!(error.contains("missing its current datapoint"));
        assert_eq!(notification.destination, "oncall");
        assert!(notification.message.contains("RULE ERROR"));
    }

    #[test]
    fn normal_and_no_data_do_not_construct_notifications() {
        let rule = rule();
        let now = datetime!(2026-03-18 10:08:00);
        for level in [AlertEvaluationLevel::Normal, AlertEvaluationLevel::NoData] {
            let evaluation = Evaluation {
                level,
                latest_point: (level == AlertEvaluationLevel::Normal).then_some(MetricPoint {
                    timestamp: now,
                    value: 0.0,
                }),
                datapoints: vec![],
                breach: None,
                window: MetricRange { from: now, to: now },
            };
            assert!(matches!(
                build_rule_run(&rule, now, Ok(evaluation), "oncall"),
                RuleRun::Evaluated {
                    notification: None,
                    ..
                }
            ));
        }
    }

    #[test]
    fn evaluation_failure_keeps_the_required_error_notification() {
        let rule = rule();
        let now = datetime!(2026-03-18 10:08:00);
        let error = Report::new(EvaluatorError::DatasourceRequestFailed)
            .attach_printable("backend rejected query");
        let RuleRun::Failed {
            error,
            notification,
        } = build_rule_run(&rule, now, Err(error), "oncall")
        else {
            panic!("an evaluation failure must construct a rule error");
        };
        assert!(error.contains("backend rejected query"));
        assert_eq!(notification.destination, "oncall");
        assert!(notification.message.contains("RULE ERROR"));
        assert!(notification.message.contains("Unavailable"));
    }
}
