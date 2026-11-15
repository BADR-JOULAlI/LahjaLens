CREATE SCHEMA IF NOT EXISTS lahjalens;

CREATE TABLE IF NOT EXISTS lahjalens.inference_runs (
    run_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    preprocessing_version TEXT NOT NULL,
    source TEXT NOT NULL,
    gold_path TEXT NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    labels_available BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lahjalens.articles (
    article_id TEXT PRIMARY KEY,
    article_topic TEXT,
    article_title TEXT,
    article_date_raw TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lahjalens.comments (
    comment_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    article_id TEXT REFERENCES lahjalens.articles(article_id),
    source TEXT NOT NULL,
    article_topic TEXT,
    comment_text TEXT NOT NULL,
    source_file TEXT,
    source_record TEXT,
    article_title TEXT,
    article_date_raw TEXT,
    event_time TIMESTAMPTZ,
    language TEXT,
    source_version TEXT,
    label_status TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lahjalens.sentiment_predictions (
    event_id TEXT NOT NULL REFERENCES lahjalens.comments(event_id),
    comment_id TEXT NOT NULL REFERENCES lahjalens.comments(comment_id),
    inference_run_id TEXT NOT NULL REFERENCES lahjalens.inference_runs(run_id),
    predicted_sentiment TEXT NOT NULL CHECK (predicted_sentiment IN ('negative', 'neutral', 'positive')),
    prediction_index SMALLINT NOT NULL CHECK (prediction_index IN (0, 1, 2)),
    prob_negative DOUBLE PRECISION CHECK (prob_negative >= 0 AND prob_negative <= 1),
    prob_neutral DOUBLE PRECISION CHECK (prob_neutral >= 0 AND prob_neutral <= 1),
    prob_positive DOUBLE PRECISION CHECK (prob_positive >= 0 AND prob_positive <= 1),
    probabilities_calibrated BOOLEAN NOT NULL DEFAULT FALSE,
    model_version TEXT NOT NULL,
    preprocessing_version TEXT NOT NULL,
    model_label_order TEXT NOT NULL,
    predicted_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, inference_run_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_comments_article_topic
    ON lahjalens.comments(article_topic);
CREATE INDEX IF NOT EXISTS idx_comments_source
    ON lahjalens.comments(source);
CREATE INDEX IF NOT EXISTS idx_predictions_sentiment
    ON lahjalens.sentiment_predictions(predicted_sentiment);
CREATE INDEX IF NOT EXISTS idx_predictions_model_version
    ON lahjalens.sentiment_predictions(model_version);
CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at
    ON lahjalens.sentiment_predictions(predicted_at);

CREATE OR REPLACE VIEW lahjalens.v_latest_predictions AS
SELECT DISTINCT ON (p.event_id)
    p.event_id,
    p.comment_id,
    p.inference_run_id,
    p.predicted_sentiment,
    p.prediction_index,
    p.prob_negative,
    p.prob_neutral,
    p.prob_positive,
    p.probabilities_calibrated,
    p.model_version,
    p.preprocessing_version,
    p.model_label_order,
    p.predicted_at,
    c.article_id,
    c.source,
    c.article_topic,
    c.comment_text,
    c.language,
    c.article_title,
    c.article_date_raw,
    c.event_time,
    c.source_version,
    c.label_status
FROM lahjalens.sentiment_predictions AS p
JOIN lahjalens.comments AS c ON c.event_id = p.event_id
ORDER BY p.event_id, p.predicted_at DESC, p.loaded_at DESC;

CREATE OR REPLACE VIEW lahjalens.v_overview AS
SELECT
    COUNT(*)::BIGINT AS comments_analyzed,
    COUNT(*) FILTER (WHERE predicted_sentiment = 'positive')::BIGINT AS positive_predictions,
    COUNT(*) FILTER (WHERE predicted_sentiment = 'negative')::BIGINT AS negative_predictions,
    COUNT(*) FILTER (WHERE predicted_sentiment = 'neutral')::BIGINT AS neutral_predictions,
    COUNT(DISTINCT article_topic)::BIGINT AS categories_covered,
    COUNT(DISTINCT model_version)::BIGINT AS model_versions,
    COUNT(DISTINCT source)::BIGINT AS sources_covered
FROM lahjalens.v_latest_predictions;

CREATE OR REPLACE VIEW lahjalens.v_sentiment_distribution AS
SELECT predicted_sentiment AS sentiment, COUNT(*)::BIGINT AS rows
FROM lahjalens.v_latest_predictions
GROUP BY predicted_sentiment
ORDER BY predicted_sentiment;

CREATE OR REPLACE VIEW lahjalens.v_topic_sentiment AS
SELECT article_topic, predicted_sentiment AS sentiment, COUNT(*)::BIGINT AS rows
FROM lahjalens.v_latest_predictions
GROUP BY article_topic, predicted_sentiment
ORDER BY article_topic, predicted_sentiment;

CREATE OR REPLACE VIEW lahjalens.v_model_runs AS
SELECT
    r.run_id,
    r.model_version,
    r.preprocessing_version,
    r.source,
    r.gold_path,
    r.row_count,
    r.status,
    r.labels_available,
    r.started_at,
    r.completed_at,
    r.last_error
FROM lahjalens.inference_runs AS r
ORDER BY r.started_at DESC;

CREATE OR REPLACE VIEW lahjalens.v_data_quality AS
SELECT 'missing_comment_text' AS check_name,
       COUNT(*)::BIGINT AS rows
FROM lahjalens.comments
WHERE comment_text IS NULL OR BTRIM(comment_text) = ''
UNION ALL
SELECT 'missing_article_id', COUNT(*)::BIGINT
FROM lahjalens.comments
WHERE article_id IS NULL OR BTRIM(article_id) = ''
UNION ALL
SELECT 'invalid_probability', COUNT(*)::BIGINT
FROM lahjalens.sentiment_predictions
WHERE prob_negative IS NULL OR prob_neutral IS NULL OR prob_positive IS NULL
   OR prob_negative < 0 OR prob_neutral < 0 OR prob_positive < 0
   OR prob_negative > 1 OR prob_neutral > 1 OR prob_positive > 1
UNION ALL
SELECT 'prediction_rows', COUNT(*)::BIGINT
FROM lahjalens.sentiment_predictions
UNION ALL
SELECT 'failed_runs', COUNT(*)::BIGINT
FROM lahjalens.inference_runs
WHERE status = 'failed';
