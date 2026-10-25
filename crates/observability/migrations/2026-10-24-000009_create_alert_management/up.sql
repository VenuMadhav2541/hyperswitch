CREATE TABLE alert_tags (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tag_key         VARCHAR(64) NOT NULL,
    tag_value       VARCHAR(255) NOT NULL,
    configuration   JSONB NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (tag_key, tag_value)
);

CREATE TABLE alert_tag_mapper (
    alert_rule_id VARCHAR(64) NOT NULL
        REFERENCES alert_rules(id) ON DELETE CASCADE,
    tag_id UUID NOT NULL
        REFERENCES alert_tags(id) ON DELETE RESTRICT,

    PRIMARY KEY (alert_rule_id, tag_id)
);

CREATE INDEX alert_tag_mapper_tag_idx
    ON alert_tag_mapper (tag_id, alert_rule_id);

CREATE TABLE alert_silences (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    configuration   JSONB NOT NULL,
    starts_at       TIMESTAMP NOT NULL,
    ends_at         TIMESTAMP NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX alert_silences_end_idx
    ON alert_silences (ends_at);
