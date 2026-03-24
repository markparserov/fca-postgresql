\set ON_ERROR_STOP on
\set QUIET 1

-- экспорт диаграммы хассе в формат graphviz dot
-- если run_id не передан, берётся последний запуск
-- пример:
--   psql -qAt -d fca_db -v run_id=1 -f "./scripts/export_hasse_dot.sql" > "./hasse/hasse_run1_auto.dot"

\if :{?run_id}
\else
SELECT COALESCE(MAX(r.run_id), 0) AS run_id
FROM fca.inclose_runs r
\gset
\endif

SELECT CASE
           WHEN EXISTS (
               SELECT 1
               FROM fca.inclose_runs r
               WHERE r.run_id = :run_id::BIGINT
           ) THEN 1
           ELSE 0
       END AS run_exists
\gset

\if :run_exists
\else
\echo 'ERROR: run_id не найден в fca.inclose_runs'
\quit 1
\endif

\pset tuples_only on
\pset format unaligned

WITH concept_sets AS (
    SELECT
        c.concept_id,
        COALESCE(
            array_agg(ci.attr_id ORDER BY ci.attr_id) FILTER (WHERE ci.attr_id IS NOT NULL),
            '{}'::BIGINT[]
        ) AS intent_ids
    FROM fca.concepts c
    LEFT JOIN fca.concept_intent ci ON ci.concept_id = c.concept_id
    WHERE c.run_id = :run_id::BIGINT
    GROUP BY c.concept_id
),
concept_labels AS (
    SELECT
        cs.concept_id,
        (
            SELECT COALESCE(array_agg(a.attr_key ORDER BY a.attr_key), '{}'::TEXT[])
            FROM unnest(cs.intent_ids) AS i(attr_id)
            JOIN fca.attributes a ON a.attr_id = i.attr_id
        ) AS intent_keys,
        (
            SELECT COALESCE(array_agg(o.obj_key ORDER BY o.obj_key), '{}'::TEXT[])
            FROM unnest(fca.extent_for_intent(cs.intent_ids)) AS e(obj_id)
            JOIN fca.objects o ON o.obj_id = e.obj_id
        ) AS extent_keys
    FROM concept_sets cs
),
lines AS (
    SELECT 1 AS ord, 0::BIGINT AS sort_key, 'digraph FCA_Hasse {' AS line
    UNION ALL
    SELECT 2, 0, '    rankdir=BT;'
    UNION ALL
    SELECT 3, 0, format('    graph [label="Диаграмма Хассе (run_id=%s)", labelloc=t, fontsize=18];', :run_id)
    UNION ALL
    SELECT 4, 0, '    node [shape=box, fontsize=11];'
    UNION ALL
    SELECT
        5 AS ord,
        cl.concept_id AS sort_key,
        format(
            '    C%s [label="C%s\nintent={%s}\nextent={%s}"];',
            cl.concept_id,
            cl.concept_id,
            array_to_string(cl.intent_keys, ','),
            array_to_string(cl.extent_keys, ',')
        ) AS line
    FROM concept_labels cl
    UNION ALL
    SELECT 6, 0, ''
    UNION ALL
    SELECT
        7 AS ord,
        (e.parent_concept_id * 1000000 + e.child_concept_id) AS sort_key,
        format('    C%s -> C%s;', e.parent_concept_id, e.child_concept_id) AS line
    FROM fca.concept_hasse_edges e
    WHERE e.run_id = :run_id::BIGINT
    UNION ALL
    SELECT 8, 0, '}'
)
SELECT line
FROM lines
ORDER BY ord, sort_key;