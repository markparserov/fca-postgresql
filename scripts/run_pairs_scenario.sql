\set ON_ERROR_STOP on

-- готовый сценарий запуска для pairs.csv
-- запуск:
--   psql -d fca_db -f "./scripts/run_pairs_scenario.sql"

\ir ./inclose5_postgresql.sql

BEGIN;

TRUNCATE TABLE
    fca.concept_hasse_edges,
    fca.concept_extent,
    fca.concept_intent,
    fca.concepts,
    fca.inclose_runs,
    fca.context,
    fca.attr_extent,
    fca.objects,
    fca.attributes
RESTART IDENTITY CASCADE;

CREATE TEMP TABLE stage_pairs (
    obj_key TEXT NOT NULL,
    attr_key TEXT NOT NULL
);

\copy stage_pairs(obj_key, attr_key) FROM './data/pairs.csv' WITH (FORMAT csv, HEADER true)

INSERT INTO fca.objects (obj_key)
SELECT DISTINCT s.obj_key
FROM stage_pairs s
ORDER BY s.obj_key;

INSERT INTO fca.attributes (attr_key)
SELECT DISTINCT s.attr_key
FROM stage_pairs s
ORDER BY s.attr_key;

INSERT INTO fca.context (obj_id, attr_id)
SELECT o.obj_id, a.attr_id
FROM stage_pairs s
JOIN fca.objects o ON o.obj_key = s.obj_key
JOIN fca.attributes a ON a.attr_key = s.attr_key
ON CONFLICT DO NOTHING;

COMMIT;

SELECT fca.rebuild_attr_extent();

SELECT fca.run_inclose(
    p_min_extent => 0,
    p_min_intent => 0,
    p_max_concepts => 50000,
    p_max_runtime => INTERVAL '5 minutes',
    p_store_extent => true,
    p_rebuild_vertical => false
) AS run_id \gset

SELECT :run_id::BIGINT AS run_id;

SELECT fca.build_hasse_edges(:run_id::BIGINT) AS hasse_edges_inserted;

SELECT *
FROM fca.v_run_summary
WHERE run_id = :run_id::BIGINT;

SELECT
    c.concept_id,
    c.intent_size,
    c.extent_size,
    COALESCE(
        array_agg(a.attr_key ORDER BY a.attr_key) FILTER (WHERE a.attr_key IS NOT NULL),
        '{}'::TEXT[]
    ) AS intent_attr_keys
FROM fca.concepts c
LEFT JOIN fca.concept_intent ci ON ci.concept_id = c.concept_id
LEFT JOIN fca.attributes a ON a.attr_id = ci.attr_id
WHERE c.run_id = :run_id::BIGINT
GROUP BY c.concept_id, c.intent_size, c.extent_size
ORDER BY c.concept_id;

SELECT *
FROM fca.v_hasse_edges
WHERE run_id = :run_id::BIGINT
ORDER BY parent_concept_id, child_concept_id;