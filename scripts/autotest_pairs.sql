\set ON_ERROR_STOP on

-- автоматический регрессионный тест для pairs.csv
-- останавливается с error при провале любой проверки
-- запуск:
--   psql -d fca_db -f "./scripts/autotest_pairs.sql"

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

SELECT fca.build_hasse_edges(:run_id::BIGINT);

DO $$
DECLARE
    v_run_id BIGINT := :run_id::BIGINT;
    v_status TEXT;
    v_concepts BIGINT;
    v_edges BIGINT;
    v_missing_concepts INTEGER;
    v_extra_concepts INTEGER;
    v_bad_roundtrip INTEGER;
BEGIN
    SELECT r.status, r.concepts_written
    INTO v_status, v_concepts
    FROM fca.inclose_runs r
    WHERE r.run_id = v_run_id;

    IF v_status <> 'finished' THEN
        RAISE EXCEPTION 'Expected status=finished, got %', v_status;
    END IF;

    IF v_concepts <> 6 THEN
        RAISE EXCEPTION 'Expected 6 concepts, got %', v_concepts;
    END IF;

    SELECT COUNT(*) INTO v_edges
    FROM fca.concept_hasse_edges e
    WHERE e.run_id = v_run_id;

    IF v_edges <> 7 THEN
        RAISE EXCEPTION 'Expected 7 Hasse edges, got %', v_edges;
    END IF;

    WITH expected(intent) AS (
        VALUES
            ('{}'::TEXT[]),
            ('{a1}'::TEXT[]),
            ('{a2}'::TEXT[]),
            ('{a1,a2}'::TEXT[]),
            ('{a2,a3}'::TEXT[]),
            ('{a1,a2,a3}'::TEXT[])
    ),
    actual AS (
        SELECT COALESCE(array_agg(a.attr_key ORDER BY a.attr_key), '{}'::TEXT[]) AS intent
        FROM fca.concepts c
        LEFT JOIN fca.concept_intent ci ON ci.concept_id = c.concept_id
        LEFT JOIN fca.attributes a ON a.attr_id = ci.attr_id
        WHERE c.run_id = v_run_id
        GROUP BY c.concept_id
    )
    SELECT COUNT(*) INTO v_missing_concepts
    FROM expected e
    LEFT JOIN actual a ON a.intent = e.intent
    WHERE a.intent IS NULL;

    IF v_missing_concepts <> 0 THEN
        RAISE EXCEPTION 'Missing expected intents: %', v_missing_concepts;
    END IF;

    WITH expected(intent) AS (
        VALUES
            ('{}'::TEXT[]),
            ('{a1}'::TEXT[]),
            ('{a2}'::TEXT[]),
            ('{a1,a2}'::TEXT[]),
            ('{a2,a3}'::TEXT[]),
            ('{a1,a2,a3}'::TEXT[])
    ),
    actual AS (
        SELECT COALESCE(array_agg(a.attr_key ORDER BY a.attr_key), '{}'::TEXT[]) AS intent
        FROM fca.concepts c
        LEFT JOIN fca.concept_intent ci ON ci.concept_id = c.concept_id
        LEFT JOIN fca.attributes a ON a.attr_id = ci.attr_id
        WHERE c.run_id = v_run_id
        GROUP BY c.concept_id
    )
    SELECT COUNT(*) INTO v_extra_concepts
    FROM actual a
    LEFT JOIN expected e ON e.intent = a.intent
    WHERE e.intent IS NULL;

    IF v_extra_concepts <> 0 THEN
        RAISE EXCEPTION 'Found unexpected intents: %', v_extra_concepts;
    END IF;

    -- проверка roundtrip-замыкания: intent -> extent -> intent
    WITH concept_intents AS (
        SELECT
            c.concept_id,
            COALESCE(array_agg(ci.attr_id ORDER BY ci.attr_id), '{}'::BIGINT[]) AS intent
        FROM fca.concepts c
        LEFT JOIN fca.concept_intent ci ON ci.concept_id = c.concept_id
        WHERE c.run_id = v_run_id
        GROUP BY c.concept_id
    )
    SELECT COUNT(*) INTO v_bad_roundtrip
    FROM concept_intents x
    WHERE fca.intent_for_extent(fca.extent_for_intent(x.intent)) <> x.intent;

    IF v_bad_roundtrip <> 0 THEN
        RAISE EXCEPTION 'Intent roundtrip failed for % concepts', v_bad_roundtrip;
    END IF;

    RAISE NOTICE 'All FCA tests passed for run_id=%', v_run_id;
END $$;
