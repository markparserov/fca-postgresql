-- реализация в стиле in-close5 для postgresql
-- фокус: вертикальное представление контекста, partial-closure каноничность,
-- dfs-перебор, хранение понятий, построение рёбер хассе и метрики запуска

BEGIN;

CREATE SCHEMA IF NOT EXISTS fca;

-- 1) исходный формальный контекст

CREATE TABLE IF NOT EXISTS fca.objects (
    obj_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    obj_key TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS fca.attributes (
    attr_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attr_key TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS fca.context (
    obj_id BIGINT NOT NULL REFERENCES fca.objects (obj_id) ON DELETE CASCADE,
    attr_id BIGINT NOT NULL REFERENCES fca.attributes (attr_id) ON DELETE CASCADE,
    PRIMARY KEY (obj_id, attr_id)
);

CREATE INDEX IF NOT EXISTS idx_context_attr_obj ON fca.context (attr_id, obj_id);
CREATE INDEX IF NOT EXISTS idx_context_obj_attr ON fca.context (obj_id, attr_id);

-- вертикальное представление: для каждого атрибута храним отсортированный extent (объекты)
CREATE TABLE IF NOT EXISTS fca.attr_extent (
    attr_id BIGINT PRIMARY KEY REFERENCES fca.attributes (attr_id) ON DELETE CASCADE,
    obj_set BIGINT[] NOT NULL DEFAULT '{}'::BIGINT[],
    obj_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_attr_extent_count ON fca.attr_extent (obj_count);

-- 2) служебные операции над массивами (пересечение, включение, нормализация)

CREATE OR REPLACE FUNCTION fca.normalize_bigint_array(input BIGINT[])
RETURNS BIGINT[]
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(array_agg(DISTINCT v ORDER BY v), '{}'::BIGINT[])
    FROM unnest(COALESCE(input, '{}'::BIGINT[])) AS t(v);
$$;

CREATE OR REPLACE FUNCTION fca.array_intersect_sorted(a BIGINT[], b BIGINT[])
RETURNS BIGINT[]
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(array_agg(x ORDER BY x), '{}'::BIGINT[])
    FROM (
        SELECT DISTINCT ua.v AS x
        FROM unnest(COALESCE(a, '{}'::BIGINT[])) AS ua(v)
        INNER JOIN unnest(COALESCE(b, '{}'::BIGINT[])) AS ub(v)
            ON ua.v = ub.v
    ) i;
$$;

CREATE OR REPLACE FUNCTION fca.array_contains_all(superset_arr BIGINT[], subset_arr BIGINT[])
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT NOT EXISTS (
        SELECT 1
        FROM unnest(COALESCE(subset_arr, '{}'::BIGINT[])) s(v)
        WHERE NOT (s.v = ANY(COALESCE(superset_arr, '{}'::BIGINT[])))
    );
$$;

CREATE OR REPLACE FUNCTION fca.array_minus(a BIGINT[], b BIGINT[])
RETURNS BIGINT[]
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(array_agg(v ORDER BY v), '{}'::BIGINT[])
    FROM (
        SELECT DISTINCT ua.v
        FROM unnest(COALESCE(a, '{}'::BIGINT[])) ua(v)
        WHERE NOT (ua.v = ANY(COALESCE(b, '{}'::BIGINT[])))
    ) d;
$$;

-- 3) поддержка вертикальной материализации

CREATE OR REPLACE FUNCTION fca.rebuild_attr_extent()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO fca.attr_extent (attr_id, obj_set, obj_count, updated_at)
    SELECT
        a.attr_id,
        COALESCE(array_agg(c.obj_id ORDER BY c.obj_id), '{}'::BIGINT[]) AS obj_set,
        COUNT(c.obj_id)::INTEGER AS obj_count,
        now()
    FROM fca.attributes a
    LEFT JOIN fca.context c ON c.attr_id = a.attr_id
    GROUP BY a.attr_id
    ON CONFLICT (attr_id) DO UPDATE
    SET obj_set = EXCLUDED.obj_set,
        obj_count = EXCLUDED.obj_count,
        updated_at = now();
END;
$$;

CREATE OR REPLACE FUNCTION fca.sync_attr_extent_on_context()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    touched_attr BIGINT;
BEGIN
    touched_attr := COALESCE(NEW.attr_id, OLD.attr_id);
    INSERT INTO fca.attr_extent (attr_id, obj_set, obj_count, updated_at)
    SELECT
        touched_attr,
        COALESCE(array_agg(c.obj_id ORDER BY c.obj_id), '{}'::BIGINT[]) AS obj_set,
        COUNT(c.obj_id)::INTEGER AS obj_count,
        now()
    FROM fca.context c
    WHERE c.attr_id = touched_attr
    ON CONFLICT (attr_id) DO UPDATE
    SET obj_set = EXCLUDED.obj_set,
        obj_count = EXCLUDED.obj_count,
        updated_at = now();
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_attr_extent_after_ins ON fca.context;
CREATE TRIGGER trg_sync_attr_extent_after_ins
AFTER INSERT ON fca.context
FOR EACH ROW
EXECUTE FUNCTION fca.sync_attr_extent_on_context();

DROP TRIGGER IF EXISTS trg_sync_attr_extent_after_del ON fca.context;
CREATE TRIGGER trg_sync_attr_extent_after_del
AFTER DELETE ON fca.context
FOR EACH ROW
EXECUTE FUNCTION fca.sync_attr_extent_on_context();

-- 4) базовые операции in-close: пересечения extent и замыкания

CREATE OR REPLACE FUNCTION fca.all_objects_extent()
RETURNS BIGINT[]
LANGUAGE SQL
STABLE
AS $$
    SELECT COALESCE(array_agg(o.obj_id ORDER BY o.obj_id), '{}'::BIGINT[])
    FROM fca.objects o;
$$;

-- extent(b): объекты, содержащие все атрибуты из b
-- использует вертикальные пересечения по fca.attr_extent
CREATE OR REPLACE FUNCTION fca.extent_for_intent(intent BIGINT[])
RETURNS BIGINT[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    norm_intent BIGINT[] := fca.normalize_bigint_array(intent);
    current_extent BIGINT[] := NULL;
    attr BIGINT;
    attr_extent BIGINT[];
BEGIN
    IF COALESCE(array_length(norm_intent, 1), 0) = 0 THEN
        RETURN fca.all_objects_extent();
    END IF;

    FOREACH attr IN ARRAY norm_intent LOOP
        SELECT ae.obj_set INTO attr_extent
        FROM fca.attr_extent ae
        WHERE ae.attr_id = attr;

        IF attr_extent IS NULL THEN
            RETURN '{}'::BIGINT[];
        END IF;

        IF current_extent IS NULL THEN
            current_extent := attr_extent;
        ELSE
            current_extent := fca.array_intersect_sorted(current_extent, attr_extent);
        END IF;

        IF COALESCE(array_length(current_extent, 1), 0) = 0 THEN
            RETURN '{}'::BIGINT[];
        END IF;
    END LOOP;

    RETURN COALESCE(current_extent, '{}'::BIGINT[]);
END;
$$;

CREATE OR REPLACE FUNCTION fca.extent_cardinality_for_intent(intent BIGINT[])
RETURNS INTEGER
LANGUAGE SQL
STABLE
AS $$
    SELECT COALESCE(array_length(fca.extent_for_intent(intent), 1), 0);
$$;

-- intent(a): атрибуты, общие для всех объектов из a
CREATE OR REPLACE FUNCTION fca.intent_for_extent(extent BIGINT[])
RETURNS BIGINT[]
LANGUAGE SQL
STABLE
AS $$
    WITH norm AS (
        SELECT fca.normalize_bigint_array(extent) AS e
    )
    SELECT COALESCE(array_agg(a.attr_id ORDER BY a.attr_id), '{}'::BIGINT[])
    FROM fca.attr_extent a, norm
    WHERE fca.array_contains_all(a.obj_set, norm.e);
$$;

-- префиксное замыкание только для частичного теста каноничности
CREATE OR REPLACE FUNCTION fca.intent_prefix_for_extent(extent BIGINT[], pivot_attr BIGINT)
RETURNS BIGINT[]
LANGUAGE SQL
STABLE
AS $$
    WITH norm AS (
        SELECT fca.normalize_bigint_array(extent) AS e
    )
    SELECT COALESCE(array_agg(a.attr_id ORDER BY a.attr_id), '{}'::BIGINT[])
    FROM fca.attr_extent a, norm
    WHERE a.attr_id < pivot_attr
      AND fca.array_contains_all(a.obj_set, norm.e);
$$;

CREATE OR REPLACE FUNCTION fca.intent_prefix(intent BIGINT[], pivot_attr BIGINT)
RETURNS BIGINT[]
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(array_agg(v ORDER BY v), '{}'::BIGINT[])
    FROM unnest(COALESCE(intent, '{}'::BIGINT[])) AS t(v)
    WHERE v < pivot_attr;
$$;

CREATE OR REPLACE FUNCTION fca.partial_closure_is_canonical(
    current_intent BIGINT[],
    pivot_attr BIGINT,
    candidate_extent BIGINT[]
)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
AS $$
    SELECT fca.intent_prefix_for_extent(candidate_extent, pivot_attr)
           = fca.intent_prefix(current_intent, pivot_attr);
$$;

CREATE OR REPLACE FUNCTION fca.full_canonicity_holds(
    current_intent BIGINT[],
    pivot_attr BIGINT,
    closed_candidate_intent BIGINT[]
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT NOT EXISTS (
        SELECT 1
        FROM unnest(COALESCE(closed_candidate_intent, '{}'::BIGINT[])) d(v)
        WHERE d.v < pivot_attr
          AND NOT (d.v = ANY(COALESCE(current_intent, '{}'::BIGINT[])))
    );
$$;

-- 5) учёт запусков и хранение понятий и рёбер хассе

CREATE TABLE IF NOT EXISTS fca.inclose_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    min_extent INTEGER NOT NULL,
    min_intent INTEGER NOT NULL,
    max_concepts BIGINT NOT NULL,
    max_runtime INTERVAL NOT NULL,
    concepts_written BIGINT NOT NULL DEFAULT 0,
    candidates_tested BIGINT NOT NULL DEFAULT 0,
    pruned_by_extent BIGINT NOT NULL DEFAULT 0,
    pruned_by_partial BIGINT NOT NULL DEFAULT 0,
    pruned_by_full BIGINT NOT NULL DEFAULT 0,
    full_closure_calls BIGINT NOT NULL DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS fca.concepts (
    concept_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES fca.inclose_runs (run_id) ON DELETE CASCADE,
    intent_hash TEXT NOT NULL,
    intent_size INTEGER NOT NULL,
    extent_size INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, intent_hash)
);

CREATE TABLE IF NOT EXISTS fca.concept_intent (
    concept_id BIGINT NOT NULL REFERENCES fca.concepts (concept_id) ON DELETE CASCADE,
    attr_id BIGINT NOT NULL REFERENCES fca.attributes (attr_id) ON DELETE CASCADE,
    PRIMARY KEY (concept_id, attr_id)
);

CREATE TABLE IF NOT EXISTS fca.concept_extent (
    concept_id BIGINT NOT NULL REFERENCES fca.concepts (concept_id) ON DELETE CASCADE,
    obj_id BIGINT NOT NULL REFERENCES fca.objects (obj_id) ON DELETE CASCADE,
    PRIMARY KEY (concept_id, obj_id)
);

CREATE INDEX IF NOT EXISTS idx_concepts_run_intent_size ON fca.concepts (run_id, intent_size);
CREATE INDEX IF NOT EXISTS idx_concepts_run_extent_size ON fca.concepts (run_id, extent_size);
CREATE INDEX IF NOT EXISTS idx_concept_intent_attr ON fca.concept_intent (attr_id, concept_id);
CREATE INDEX IF NOT EXISTS idx_concept_extent_obj ON fca.concept_extent (obj_id, concept_id);

CREATE TABLE IF NOT EXISTS fca.concept_hasse_edges (
    run_id BIGINT NOT NULL REFERENCES fca.inclose_runs (run_id) ON DELETE CASCADE,
    parent_concept_id BIGINT NOT NULL REFERENCES fca.concepts (concept_id) ON DELETE CASCADE,
    child_concept_id BIGINT NOT NULL REFERENCES fca.concepts (concept_id) ON DELETE CASCADE,
    PRIMARY KEY (run_id, parent_concept_id, child_concept_id),
    CHECK (parent_concept_id <> child_concept_id)
);

CREATE OR REPLACE FUNCTION fca.intent_hash(intent BIGINT[])
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT md5(array_to_string(fca.normalize_bigint_array(intent), ','));
$$;

CREATE OR REPLACE FUNCTION fca.store_concept(
    p_run_id BIGINT,
    p_intent BIGINT[],
    p_extent BIGINT[],
    p_store_extent BOOLEAN DEFAULT FALSE
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    norm_intent BIGINT[] := fca.normalize_bigint_array(p_intent);
    norm_extent BIGINT[] := fca.normalize_bigint_array(p_extent);
    p_hash TEXT := fca.intent_hash(norm_intent);
    cid BIGINT;
BEGIN
    INSERT INTO fca.concepts (run_id, intent_hash, intent_size, extent_size)
    VALUES (
        p_run_id,
        p_hash,
        COALESCE(array_length(norm_intent, 1), 0),
        COALESCE(array_length(norm_extent, 1), 0)
    )
    ON CONFLICT (run_id, intent_hash) DO NOTHING
    RETURNING concept_id INTO cid;

    IF cid IS NULL THEN
        RETURN NULL;
    END IF;

    INSERT INTO fca.concept_intent (concept_id, attr_id)
    SELECT cid, v
    FROM unnest(norm_intent) AS t(v)
    ON CONFLICT DO NOTHING;

    IF p_store_extent THEN
        INSERT INTO fca.concept_extent (concept_id, obj_id)
        SELECT cid, v
        FROM unnest(norm_extent) AS t(v)
        ON CONFLICT DO NOTHING;
    END IF;

    UPDATE fca.inclose_runs
    SET concepts_written = concepts_written + 1
    WHERE run_id = p_run_id;

    RETURN cid;
END;
$$;

-- 6) dfs-перечислитель в стиле in-close с partial-closure отсечением

CREATE OR REPLACE FUNCTION fca.inclose_dfs(
    p_run_id BIGINT,
    p_current_intent BIGINT[],
    p_current_extent BIGINT[],
    p_last_attr BIGINT,
    p_min_extent INTEGER,
    p_min_intent INTEGER,
    p_max_concepts BIGINT,
    p_deadline TIMESTAMPTZ,
    p_store_extent BOOLEAN
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    candidate_attr BIGINT;
    candidate_extent BIGINT[];
    candidate_intent BIGINT[];
    attr_extent BIGINT[];
    concept_id BIGINT;
    current_written BIGINT;
BEGIN
    IF clock_timestamp() > p_deadline THEN
        UPDATE fca.inclose_runs
        SET status = 'timeout',
            notes = COALESCE(notes, '') || E'\nTimeout reached in DFS.'
        WHERE run_id = p_run_id;
        RETURN;
    END IF;

    SELECT concepts_written INTO current_written
    FROM fca.inclose_runs
    WHERE run_id = p_run_id;

    IF current_written >= p_max_concepts THEN
        UPDATE fca.inclose_runs
        SET status = 'max_concepts',
            notes = COALESCE(notes, '') || E'\nStopped by max_concepts.'
        WHERE run_id = p_run_id;
        RETURN;
    END IF;

    IF COALESCE(array_length(p_current_extent, 1), 0) >= p_min_extent
       AND COALESCE(array_length(p_current_intent, 1), 0) >= p_min_intent THEN
        concept_id := fca.store_concept(
            p_run_id,
            p_current_intent,
            p_current_extent,
            p_store_extent
        );
    END IF;

    FOR candidate_attr IN
        SELECT a.attr_id
        FROM fca.attributes a
        WHERE a.attr_id > p_last_attr
          AND NOT (a.attr_id = ANY(COALESCE(p_current_intent, '{}'::BIGINT[])))
        ORDER BY a.attr_id
    LOOP
        UPDATE fca.inclose_runs
        SET candidates_tested = candidates_tested + 1
        WHERE run_id = p_run_id;

        SELECT ae.obj_set INTO attr_extent
        FROM fca.attr_extent ae
        WHERE ae.attr_id = candidate_attr;

        IF attr_extent IS NULL THEN
            CONTINUE;
        END IF;

        candidate_extent := fca.array_intersect_sorted(p_current_extent, attr_extent);

        IF COALESCE(array_length(candidate_extent, 1), 0) < p_min_extent THEN
            UPDATE fca.inclose_runs
            SET pruned_by_extent = pruned_by_extent + 1
            WHERE run_id = p_run_id;
            CONTINUE;
        END IF;

        IF NOT fca.partial_closure_is_canonical(
            p_current_intent,
            candidate_attr,
            candidate_extent
        ) THEN
            UPDATE fca.inclose_runs
            SET pruned_by_partial = pruned_by_partial + 1
            WHERE run_id = p_run_id;
            CONTINUE;
        END IF;

        UPDATE fca.inclose_runs
        SET full_closure_calls = full_closure_calls + 1
        WHERE run_id = p_run_id;

        candidate_intent := fca.intent_for_extent(candidate_extent);

        IF NOT fca.full_canonicity_holds(
            p_current_intent,
            candidate_attr,
            candidate_intent
        ) THEN
            UPDATE fca.inclose_runs
            SET pruned_by_full = pruned_by_full + 1
            WHERE run_id = p_run_id;
            CONTINUE;
        END IF;

        PERFORM fca.inclose_dfs(
            p_run_id,
            candidate_intent,
            candidate_extent,
            candidate_attr,
            p_min_extent,
            p_min_intent,
            p_max_concepts,
            p_deadline,
            p_store_extent
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION fca.run_inclose(
    p_min_extent INTEGER DEFAULT 1,
    p_min_intent INTEGER DEFAULT 0,
    p_max_concepts BIGINT DEFAULT 100000,
    p_max_runtime INTERVAL DEFAULT INTERVAL '10 minutes',
    p_store_extent BOOLEAN DEFAULT FALSE,
    p_rebuild_vertical BOOLEAN DEFAULT TRUE
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_id BIGINT;
    root_extent BIGINT[];
    root_intent BIGINT[];
    deadline TIMESTAMPTZ;
BEGIN
    IF p_rebuild_vertical THEN
        PERFORM fca.rebuild_attr_extent();
    END IF;

    INSERT INTO fca.inclose_runs (
        min_extent, min_intent, max_concepts, max_runtime, status
    )
    VALUES (
        GREATEST(p_min_extent, 0),
        GREATEST(p_min_intent, 0),
        GREATEST(p_max_concepts, 1),
        p_max_runtime,
        'running'
    )
    RETURNING run_id INTO v_run_id;

    deadline := clock_timestamp() + p_max_runtime;
    root_extent := fca.all_objects_extent();
    root_intent := fca.intent_for_extent(root_extent);

    PERFORM fca.inclose_dfs(
        v_run_id,
        root_intent,
        root_extent,
        0,
        GREATEST(p_min_extent, 0),
        GREATEST(p_min_intent, 0),
        GREATEST(p_max_concepts, 1),
        deadline,
        p_store_extent
    );

    UPDATE fca.inclose_runs
    SET finished_at = now(),
        status = CASE
            WHEN status = 'running' THEN 'finished'
            ELSE status
        END
    WHERE inclose_runs.run_id = v_run_id;

    RETURN v_run_id;
END;
$$;

-- 7) построение рёбер хассе (отношение покрытия)

CREATE OR REPLACE FUNCTION fca.intent_subset(a_concept BIGINT, b_concept BIGINT)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
AS $$
    SELECT NOT EXISTS (
        SELECT 1
        FROM fca.concept_intent ai
        WHERE ai.concept_id = a_concept
          AND NOT EXISTS (
              SELECT 1
              FROM fca.concept_intent bi
              WHERE bi.concept_id = b_concept
                AND bi.attr_id = ai.attr_id
          )
    );
$$;

CREATE OR REPLACE FUNCTION fca.build_hasse_edges(p_run_id BIGINT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    inserted_count INTEGER := 0;
BEGIN
    DELETE FROM fca.concept_hasse_edges e
    WHERE e.run_id = p_run_id;

    WITH pairs AS (
        SELECT
            a.concept_id AS parent_id,
            b.concept_id AS child_id
        FROM fca.concepts a
        JOIN fca.concepts b
            ON a.run_id = b.run_id
           AND a.concept_id <> b.concept_id
           AND a.intent_size < b.intent_size
        WHERE a.run_id = p_run_id
          AND fca.intent_subset(a.concept_id, b.concept_id)
    ),
    covers AS (
        SELECT p.parent_id, p.child_id
        FROM pairs p
        WHERE NOT EXISTS (
            SELECT 1
            FROM pairs q
            WHERE q.parent_id = p.parent_id
              AND q.child_id <> p.child_id
              AND fca.intent_subset(q.child_id, p.child_id)
        )
    )
    INSERT INTO fca.concept_hasse_edges (run_id, parent_concept_id, child_concept_id)
    SELECT p_run_id, c.parent_id, c.child_id
    FROM covers c
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;

-- 8) представления для экспорта и визуализации

CREATE OR REPLACE VIEW fca.v_concepts AS
SELECT
    c.run_id,
    c.concept_id,
    c.intent_size,
    c.extent_size,
    array_agg(ci.attr_id ORDER BY ci.attr_id) AS intent
FROM fca.concepts c
LEFT JOIN fca.concept_intent ci ON ci.concept_id = c.concept_id
GROUP BY c.run_id, c.concept_id, c.intent_size, c.extent_size;

CREATE OR REPLACE VIEW fca.v_hasse_edges AS
SELECT
    e.run_id,
    e.parent_concept_id,
    e.child_concept_id
FROM fca.concept_hasse_edges e;

CREATE OR REPLACE VIEW fca.v_run_summary AS
SELECT
    r.run_id,
    r.started_at,
    r.finished_at,
    r.status,
    r.min_extent,
    r.min_intent,
    r.max_concepts,
    r.max_runtime,
    r.concepts_written,
    r.candidates_tested,
    r.pruned_by_extent,
    r.pruned_by_partial,
    r.pruned_by_full,
    r.full_closure_calls,
    r.notes
FROM fca.inclose_runs r;

COMMIT;

-- пример использования:
--   select fca.rebuild_attr_extent();
--   select fca.run_inclose(
--       p_min_extent => 2,
--       p_min_intent => 0,
--       p_max_concepts => 50000,
--       p_max_runtime => interval '5 minutes',
--       p_store_extent => false
--   ) as run_id;
--   select fca.build_hasse_edges(<run_id>);
--   select * from fca.v_run_summary order by run_id desc limit 1;
--   select * from fca.v_concepts where run_id = <run_id> order by concept_id;
--   select * from fca.v_hasse_edges where run_id = <run_id>;