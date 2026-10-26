# Database-backed Loki application alerts — code changes

## Context

OP already has an infrastructure-alert foundation that queries CloudWatch through Grafana, evaluates configured thresholds and delivers notifications through its existing chat notifier. The local restructuring separates provider transport, evaluator logic and notification construction so additional datasources can follow the same pattern.

Application alerting needs to move into this foundation as well. The existing application catalogue contains Loki-backed detectors for HTTP errors and internal failures such as webhook decoding, workflow insertion, Redis operations and Kafka publishing. These detectors may produce several distinct labeled error instances for a single rule, unlike the scalar evaluation used by the CloudWatch path.

The intended ownership is:

- **OP:** infrastructure/application rule storage, query execution, evaluation, detailed message construction, routing and eventually persistent lifecycle/delivery management.
- **Grafana:** datasource query gateway, not the final application alert evaluator.
- **Loki:** current source for application error signals and diagnostic context.
- **Existing notifier:** delivery abstraction reused by both infrastructure and application alerts.
- **`hyperswitch-alerts`:** existing business SR/volume alerting; not replaced by this work.

Loki is the first application datasource because the current logs already carry the required internal/handled/background error signals and context. HTTP 4xx/5xx, host resource and business metrics cannot be assumed to represent these events. A later VictoriaMetrics migration requires equivalent event metrics for each detector, not simply changing the datasource URL.

## Problem statement

The CloudWatch foundation cannot directly express the complete application-alert path:

1. Its evaluation result is scalar, while one application rule can return multiple labeled instances that need independent conditions and messages.
2. Application messages need actual error text and available service/API/method/flow/connector/file/request context, not a numeric breach or a summary of how many rules ran.
3. Rule selection and scheduling need to use the existing database rule foundation, without duplicating rule storage or hardcoding application rules in a separate system.
4. Source expressions, reducers, thresholds, cadence and state policies must be preserved. Treating unsupported expressions as approximate nonzero checks or dropping quiet rules would risk missing alerts.
5. Query failures, empty results, malformed samples and provider refusals must stay distinguishable; none should silently become a healthy result or successful delivery.
6. Stateful pending/firing/recovery, repeat suppression, retry and threading behavior cannot be supplied by a stateless query-and-send endpoint alone.

**Required outcome:** extend the existing foundation so a stored application rule can query Loki through Grafana, evaluate every returned instance, construct its detailed message and use the existing OP notifier. Complete the remaining expression/policy coverage and durable instance lifecycle before replacing the old application alert evaluation path. No required detector may be lost at cutover.

**This change's boundary:** implement the database-to-query-to-detailed-message-to-notifier foundation now, explicitly block unsupported/stateful behavior, and identify the precise remaining code work instead of claiming the entire alert system is finished.

## 1. Scope

Extend the existing CloudWatch/Grafana alert foundation to support application alerts from Loki. Reuse the provider, evaluator, storage, authentication and notifier patterns instead of introducing a separate alerting stack.

OP owns rule selection, queries, condition evaluation, message construction and delivery. Grafana is used only to query the datasource. No Beacon integration is introduced. Existing business SR/volume alerting is outside this change.

The implementation contains:

- Shared labeled metrics models for CloudWatch and Loki.
- A Loki `MetricsProvider` implementation using the shared Grafana client.
- Versioned database rule configuration and rule CRUD, based on the existing alert-rule foundation.
- Per-instance Loki evaluation rather than a single scalar response.
- Detailed application alert message construction.
- Authenticated read-only evaluation and bounded one-shot notification endpoints.
- Configuration validation, catalogue conversion and automated tests.

Persistent application lifecycle, pending timers, deduplication, durable retries and recovery are not implemented by this change. Their required code work is listed in section 11.

## 2. Processing path

```text
RuleDefinition from config / alert_rules
    -> select GrafanaLoki rules
    -> AlertEvaluator::prepare_request
    -> MetricsProvider::fetch_metrics
    -> shared GrafanaClient
    -> labeled MetricSeries
    -> AlertEvaluator::evaluate
    -> ApplicationEvaluationBatch
    -> render_application_alert
    -> preview response / existing notifier::notify_chat
```

The existing CloudWatch path remains separate at source selection and evaluation. It shares the transport, metrics types, evaluator contract and notifier infrastructure.

## 3. Shared query and metrics models

### `crates/common_types/src/alert_rules.rs`

- Export the `stored` configuration module.
- Add `AlertQuery::GrafanaLoki` and `MetricSource::GrafanaLoki`.
- Add `AlertQuery::metric_source()` so callers select the configured datasource without hardcoding CloudWatch.
- Add `GrafanaLokiQuery`:
  - `expression`: LogQL expression.
  - `mode`: instant or range query.
  - `range_seconds`: evaluation lookback.
  - `step_seconds`: range-query step.
  - `reducer`: last/count/sum/minimum/maximum/mean.
  - `delivery_enabled`: explicit send opt-in, default false.
  - `pending_seconds`: source pending duration; nonzero pending cannot enable delivery yet.
  - `labels`: static context merged with query labels.
  - `fields`: canonical message fields mapped to returned label names.
- Add `expanded_expression()` for `$__range`, `$__interval` and `$__auto`; reject unsupported macros.
- Validate expression, lookback, step, allowed field mappings and delivery/pending constraints.
- Preserve existing CloudWatch query/statistic/severity/threshold models.

### `crates/external_services/src/metrics_provider.rs`

Move common metric types out of the CloudWatch-specific module:

```text
MetricRange  { from, to }
MetricPoint  { timestamp, value }
MetricSeries { labels, points }
```

Preserve the `MetricsProvider` contract:

- Associated query/request/response/error types.
- Infallible `build_request`.
- `handle_response` for provider parsing.
- Async `fetch_metrics`.

Loki uses labels to distinguish error instances and preserve message context. CloudWatch uses the same timestamped point representation.

### `crates/external_services/src/metrics_provider/error.rs`

Remove the old provider-validation-specific error pieces as part of the shared provider restructuring. Validation is not reintroduced into the transport interface.

## 4. Grafana providers

### `crates/external_services/src/metrics_provider/grafana.rs`

- Export the Loki provider module.
- Add shared generic request execution on `GrafanaClient`.
- Keep provider request/response parsing outside the shared transport.
- Reuse the existing transport/configuration/proxy behavior for both sources.

### `crates/external_services/src/metrics_provider/grafana/cloudwatch.rs`

- Adapt CloudWatch to shared metrics types and shared client execution.
- Keep compatibility reexports for callers using the earlier type paths.
- Preserve CloudWatch-specific request construction and response interpretation.
- Preserve backend-driven query validation rather than adding provider-side startup validation.

### `crates/external_services/src/metrics_provider/grafana/loki.rs`

New `GrafanaLoki: MetricsProvider` implementation:

1. Define typed instant/range query and Grafana request/response models.
2. Build `/api/ds/query` requests with query reference `A`.
3. Encode the query step as a duration string, such as `1s`.
4. Execute through shared `GrafanaClient`.
5. Parse each numeric field into a labeled `MetricSeries`.
6. Reject query errors, incomplete results, frame notices and malformed schemas.
7. Reject null/non-finite values, invalid timestamps and mismatched column lengths.

The parser fails rather than silently dropping invalid samples. It is not a general raw-log parser or Grafana expression interpreter.

## 5. Stored rules and CRUD

### `crates/common_types/src/alert_rules/stored.rs`

Integrate the independently versioned rule configuration model and extend it with Loki:

- `AlertRuleStatus`: active/inactive.
- `AlertConfiguration::V1` with versioned query and thresholds.
- Stored `AlertQuery::Loki(LokiQuery::V1(...))` alongside CloudWatch.
- Validation and conversion into runtime `AlertRuleConfig`.
- Tests for version handling, roundtrip conversion, context mappings and pending/delivery guards.

Stored query tag is `datasource: "loki"`; runtime query tag is `datasource: "grafana_loki"`. The conversion layer bridges them.

### `crates/api_models/src/observability/alert_manager/alert_rules.rs`

Add typed upsert, single-rule, list and delete API models.

### `crates/diesel_models/src/observability/alert_manager/alert_rules.rs`

Add persisted/new rule row models with typed status and configuration.

### `crates/diesel_models/src/observability/query/alert_manager/alert_rules.rs`

Implement repository queries:

- List all rules and find by ID.
- Upsert and delete.
- List active rules.
- List due active rules.
- Claim a due rule by comparing its observed `next_run_at` and advancing it atomically.

### `crates/diesel_models/src/observability/schema.rs`

Register `alert_rules` in the Diesel schema.

### `crates/observability/migrations/2026-10-21-000008_create_alert_rules/`

`crates/observability/migrations/2026-10-21-000008_create_alert_rules/up.sql` creates:

```text
alert_rules
  id                    VARCHAR(64) PRIMARY KEY
  name                  VARCHAR(255)
  status                VARCHAR(32)
  alerts_configuration  JSONB
  next_run_at
  created_at
  last_updated_at
```

Add a partial due-rule index for active rows. `crates/observability/migrations/2026-10-21-000008_create_alert_rules/down.sql` drops the table.

### `crates/observability/src/db/alert_rules.rs`

Add `AlertRulesInterface` and its `Store` implementation, delegating to the Diesel queries and converting rows into domain models.

### `crates/observability/src/domain_models/alert_rules.rs`

- Add domain rule/new-rule models.
- Validate ID, name, status and stored configuration.
- Set creation/update/due timestamps.
- Add `next_run_after()` for cadence scheduling.
- Convert storage rows to domain rows, API responses and runtime `RuleDefinition`.
- Upsert preserves creation time and makes the replaced rule due again.

### `crates/observability/src/core/alert_rules.rs`

Implement list/get/upsert/delete orchestration. Upsert additionally checks that the rule's datasource and destination IDs exist in the current OP configuration.

### `crates/observability/src/routes/alert_rules.rs`

Expose thin authenticated CRUD handlers using the existing `server_wrap` pattern:

```text
GET     /alerts/rules
GET     /alerts/rules/{id}
PUT     /alerts/rules/{id}
DELETE  /alerts/rules/{id}
```

### Module exports and error mapping

Register the rule modules in:

- `crates/api_models/src/observability/alert_manager.rs`
- `crates/diesel_models/src/observability/alert_manager.rs`
- `crates/diesel_models/src/observability/query/alert_manager.rs`
- `crates/observability/src/core.rs`
- `crates/observability/src/db.rs` — also extends `StorageInterface`.
- `crates/observability/src/domain_models.rs`
- `crates/observability/src/routes.rs`

`crates/observability/src/errors.rs` adds rule validation/not-found errors and their HTTP mappings.

## 6. Settings and shared evaluator changes

### `crates/observability/src/settings/evaluator.rs`

- Add `RuleSource::{Config, Database}`, defaulting to config.
- Require empty config rules in database mode to avoid mixing two rule catalogues.
- Resolve datasource settings through `query.metric_source()`.
- Validate configured datasource IDs and notification destinations.
- Allow whole-minute cadences from 60 to 86,400 seconds.
- Validate Loki expression/lookback/step/context fields and pending/delivery restrictions.

### `crates/observability/src/core/evaluator.rs`

- Add associated `type Evaluation` to `AlertEvaluator`.
- Preserve `prepare_request` and `evaluate` as the provider/evaluator boundary.
- Add shared `load_rules` / `load_selected_rules`:
  - Config mode loads matching configured rules.
  - Database preview loads active matching rules without claims.
  - Database notify selects due matching rules and claims eligible rules.
  - Delivery-disabled/pending-policy Loki rules remain blocked without a claim.
- Keep CloudWatch processing source-filtered.
- Keep existing CloudWatch notifier behavior.

A due-time CAS is scheduling only, not persistent delivery state. The current claim occurs before query/render/send; failures or a message-budget block can consume that scheduled run.

### `crates/observability/src/core/evaluator/cloudwatch.rs`

Rename to `crates/observability/src/core/evaluator/grafana_cloudwatch.rs` and adapt the existing evaluator to the shared trait/types. This is a restructure of the existing implementation, not a second CloudWatch evaluator.

### `crates/observability/src/domain_models/evaluator.rs`

Adapt runtime evaluator models to shared timestamped series and reject constructing a CloudWatch request from a different query variant.

### `crates/observability/src/services/chat.rs`

Adapt existing CloudWatch notification construction to the shared timestamped points. CloudWatch retains its own renderer; application messages use the new renderer below.

## 7. Loki evaluator and notification processing

### `crates/observability/src/core/evaluator/grafana_loki.rs`

Add:

- `ApplicationInstanceObservation`: labels, level, reduced value and optional notification.
- `ApplicationEvaluationBatch`: instances and explicit no-data flag.
- Rule/notification response models with status, message text, destination, fingerprint and receipt/error.
- `ApplicationNotifyRequest`: selected rule IDs and message budget.

Evaluation behavior:

1. Build the lookback and expanded LogQL expression.
2. Fetch through `GrafanaLoki`.
3. Reject more than 1,000 returned series, duplicate label identities or duplicate timestamps.
4. Filter samples to the window and sort chronologically.
5. Apply the configured reducer per series.
6. Select the highest-priority matching severity.
7. Preserve labels for rendering; distinguish normal, no-data, breaching and failed results.

`count` means returned sample count per series, not automatically event count. The LogQL expression and reducer must therefore be reviewed together.

Processing behavior:

- Query concurrency is bounded at 4.
- Preview renders breaching messages without delivery or scheduling changes.
- Notify accepts 1–31 selected rule IDs and a budget of 1–20 messages.
- If a rule exceeds the remaining budget, none of its instances are sent.
- Send each permitted breaching instance through existing `core::notifier::notify_chat`.
- Report delivered, refused and failed outcomes separately.
- Report absent/inactive/not-due/wrong-source selected IDs explicitly.
- No automatic retry or lifecycle state update is introduced.

### `crates/observability/src/routes/evaluator.rs`

Add thin handlers using the existing authentication wrapper:

```text
GET  /alerts/grafana/loki/evaluate
POST /alerts/grafana/loki/notify
```

Notify body:

```json
{"rule_ids":["reviewed-rule-id"],"max_messages":1}
```

### `crates/observability/src/routes/app.rs`

Register rule CRUD and Loki evaluation/notification endpoints under the existing alert scope. Keep CloudWatch endpoints unchanged in purpose and source selection.

## 8. Detailed message construction

### `crates/observability/src/services/application_alert.rs`

Add `render_application_alert(rule, instance, threshold, now, redactions)`.

Processing:

1. Merge static query labels with returned instance labels; returned labels take precedence.
2. Hash rule ID plus merged labels into an instance fingerprint.
3. Resolve canonical fields through explicit mappings or bounded aliases.
4. Construct one message for each breaching instance.
5. Sanitize configured secrets, sensitive lines, broadcast mentions and markup.
6. Reject messages exceeding 9,000 characters instead of silently truncating.

Message fields, in order:

```text
[APPLICATION ALERT] <rule name>
Severity: <level>
Rule: <rule ID>
Description: <threshold description>
Application: <if present>
Environment: <if present>
API path: <if present>
Method: <if present>
Flow: <if present>
Connector: <if present>
Error code: <if present>
Source: <if present>
Request ID: <if present>
Event time: <if present>

Error details:
<actual sanitized error, or explicit aggregate/no-details explanation>

Evaluated: <UTC timestamp>
Lookback: <seconds>
Condition: <reducer> <operator> <threshold>
Observed condition value: <value>
Instance: <fingerprint>
```

Canonical field mappings: `error`, `application`, `environment`, `path`, `method`, `flow`, `connector`, `file`, `request_id`, `event_time`, `error_code`.

The same rendered text is returned in preview and passed to the notifier. Aggregated queries that discard error context are explicitly identified; the renderer does not invent missing details.

`crates/observability/src/services.rs` exports the new module.

Current limitations:

- Grafana description templates are not expanded; imported placeholders remain literal.
- Fingerprints include all labels, including volatile ones; they are not a finished incident-grouping/dedup policy.
- Sanitization is defensive, not a complete sensitive-data classifier.
- Authenticated preview also contains returned observation labels; its output must be treated as sensitive.

## 9. Importer, local configuration and documentation

### `crates/observability/scripts/import-loki-rules.mjs`

Add loopback-only rule import from captured rule/group JSON:

- Preserve source UID, expression and verified group cadence.
- Convert supported threshold/reduction graphs.
- Record pending/no-data/execution-error/keep-firing policy blockers.
- Set every imported rule to delivery-disabled.
- Reject unsupported graphs, threshold shapes, offsets and cadences rather than guess.
- Upsert through the existing rule API and produce a per-rule import report.

This is not a scheduler, notifier or general Grafana expression translator.

### `crates/observability/scripts/loki-sandbox-smoke.mjs`

Add a dry-run-default diagnostic snapshot tool. Its optional summary notification is separate from the detailed application renderer and is not the acceptance path for detailed messages.

### `config/observability.loki.local.toml`

Add a loopback application fixture with a sample Loki query, log-only destination and lazy database pool defaults. This is example configuration, not a fully migrated alert catalogue.

### `crates/observability/README.md`

Document database/config rule sources, CRUD, Loki preview/notify usage, import behavior, bounds and incomplete lifecycle guarantees.

## 10. Tests and dependency changes

### Provider and shared model tests

- `crates/external_services/tests/grafana_loki.rs`: instant/range request shape, duration-string step, labeled response parsing and partial/error rejection.
- `crates/common_types/src/alert_rules.rs`: query/source behavior, macro expansion and existing shared model regression tests.
- `crates/common_types/src/alert_rules/stored.rs`: stored roundtrip, runtime conversion, version rejection and Loki pending/context validation.
- Existing provider unit tests protect CloudWatch behavior after shared transport/type restructuring.

### Application and API tests

- `crates/observability/tests/alert_rules.rs`: CRUD/auth/validation, active/due/config rule selection, concurrent claim skipping, no claims during preview, detailed per-instance messages, redaction, budget behavior and PostgreSQL persistence/claim/restart checks.
- `crates/observability/tests/loki.rs`: source filtering, authenticated preview, retained labels, explicit no-data/failures and notify request limits.
- `crates/observability/src/services/application_alert.rs`: full mapped error preservation, missing-detail explanation, oversize rejection and sanitization.
- `crates/observability/tests/notify.rs`: adapt existing CloudWatch/notifier tests without replacing their behavior.

### Existing test-store compatibility

These files add implementations for the new rule repository trait to existing test stores. They do not redesign the corresponding features:

- `crates/observability/tests/blacklist.rs`
- `crates/observability/tests/dictionary.rs`
- `crates/observability/tests/lifecycle_events.rs`
- `crates/observability/tests/metadata.rs`
- `crates/observability/tests/rule_toggles.rs`
- `crates/observability/tests/thresholds.rs`

### Script tests

- `crates/observability/scripts/import-loki-rules.test.mjs`: conversion fidelity, disabled delivery, policy blockers and graph/cadence rejection.
- `crates/observability/scripts/loki-sandbox-smoke.test.mjs`: diagnostic dry-run, empty-data handling, refusal handling and loopback-only application selection.

### Dependencies

- `crates/observability/Cargo.toml`: add `wiremock` as a dev dependency.
- `Cargo.lock`: record that test dependency.

Recorded verification: 178 Rust tests and 8 Node tests passed, including two explicitly run isolated PostgreSQL tests. Full router/v2 builds and six other ignored PostgreSQL repository tests were not run.

## 11. Remaining implementation required to complete the alert system

### A. Complete expression and cadence coverage

Affected files: importer/tests, common runtime/stored models, settings, Loki provider/evaluator, scheduling helper and repository tests.

Currently unsupported source definitions:

| Rule | Required implementation |
| --- | --- |
| 5xx alerts | Verified non-strict reducer semantics |
| 4xx alerts | Verified non-strict reducer semantics |
| Drainer Operation Error | Verified non-strict reducer semantics |
| Redis Token Backfilling Error | Exact threshold parameter interpretation |
| UCS 5xx | Ten-second cadence support across validation, scheduling and execution |

Do not merely loosen validation or silently round timing. For each rule preserve selector, window, offsets, macros, reducer, threshold, severity, cadence, pending duration, no-data/error/keep-firing policy and routing.

Current importer support is 26 of 31 captured enabled definitions. Complete acceptance requires every required detector; successful import/evaluation alone does not prove parity.

### B. Complete renderer semantics

Affected files: application renderer/tests, importer, stored context model and CRUD validation.

- Resolve supported annotation placeholders or convert descriptions to reviewed OP-native templates.
- Define whether aggregate rules require bounded diagnostic queries for actual error examples.
- Review retained labels, error payload content and destination/severity mapping.
- Define oversize attachment behavior using existing file-upload support if needed.

Acceptance: exact message tests for each alert family, aggregate, missing-field, sensitive-field and oversize case.

### C. Add generic persistent instance state

Add one generic `alert_instances` migration/model/repository and integrate it into evaluator orchestration. Do not repurpose business lifecycle rows or introduce separate per-source lifecycle engines.

Required contract:

- Stable incident identity and grouping policy.
- Labels/context and current state.
- Pending/firing/recovery timing across polls/restarts.
- No-data/error/keep-firing transitions.
- Per-destination attempt, delivery and thread references.
- Atomic claim/version rules and crash recovery.
- Explicit uncertain-send/retry policy; no unsupported exactly-once claim.
- Config update, deletion, inactivity and retention behavior.

Keep the provider transport and message renderer reusable. Specify durable delivery-intent handling before claiming outbox guarantees.

Acceptance tests: long pending, duplicate ticks, concurrency, restart, crash around send, refusal/rate limiting, uncertain outcome, recovery, destination changes and independent sink delivery.

### D. Complete end-to-end and parity tests

- Verify the exact submitted text and receipt through the existing chat notifier.
- Cover multiple instances, failures, refusals and oversize handling.
- Add a per-rule acceptance matrix for all required source definitions and policies.
- Verify quiet/internal/background paths and long pending rules; no-data is not a healthy/parity assertion.
- Keep existing source alerting until every required detector and lifecycle behavior is accepted.

## 12. Code navigation

Implementation reference: `57bfa87516`.

Key line anchors in that revision:

| File | Entry point |
| --- | --- |
| `crates/common_types/src/alert_rules.rs` | L17 query variants; L24 Loki query; L117 mode; L124 reducers |
| `crates/common_types/src/alert_rules/stored.rs` | L87 configuration; L111 stored query; L119 Loki version |
| `crates/external_services/src/metrics_provider.rs` | L13 common range; L19 series; L25 point; L33 provider trait |
| `crates/external_services/src/metrics_provider/grafana/loki.rs` | L115 provider implementation; L183 frame parser |
| `crates/observability/src/core/evaluator.rs` | L30 evaluator trait; L109 rule loading; L118 selected-rule loading |
| `crates/observability/src/core/evaluator/grafana_loki.rs` | L31 observation; L40 notification report; L68 notify request; L81 evaluator |
| `crates/observability/src/services/application_alert.rs` | L23 renderer; L160 sanitization; L218 tests |
| `crates/observability/src/domain_models/alert_rules.rs` | L95 scheduling helper; L145 runtime conversion |
| `crates/observability/src/core/alert_rules.rs` | L41 upsert; L81 deployment settings validation |
| `crates/observability/src/routes/evaluator.rs` | L16 Loki notify; L31 Loki preview |

For exact line-by-line additions/deletions:

```sh
# Entire implementation, including imported rule foundation and local restructuring:
git show --find-renames 57bfa87516

# One file's exact changed-line coordinates:
git diff 57bfa87516^ 57bfa87516 -- crates/observability/src/core/evaluator.rs

# Read a file at the revision used by the line anchors:
git show 57bfa87516:crates/observability/src/services/application_alert.rs
```

The change map covers all 53 changed files, counting the CloudWatch evaluator rename as one file change. This document describes code; it introduces no additional runtime behavior.
