WITH raw_stage AS (
    SELECT 
        snapshot_type_cd,
        client_cd,
        processing_region_cd,
        business_dt::timestamp AS business_dt,
        MIN(created_at) AS raw_started,
        MAX(created_at) AS raw_completed
    FROM markers
    WHERE business_dt >= '2024-10-09'
      AND marker_type_cd ILIKE '%Raws%'
    GROUP BY snapshot_type_cd, client_cd, processing_region_cd, business_dt
),

enrich_stage AS (
    SELECT 
        snapshot_type_cd,
        client_cd,
        processing_region_cd,
        business_dt::timestamp AS business_dt,
        MAX(created_at) AS enrich_completed
    FROM markers
    WHERE business_dt >= '2024-10-09'
      AND marker_type_cd ILIKE '%Enrich%'
    GROUP BY snapshot_type_cd, client_cd, processing_region_cd, business_dt
),

rollup_stage AS (
    SELECT 
        snapshot_type_cd,
        client_cd,
        processing_region_cd,
        business_dt::timestamp AS business_dt,
        MAX(created_at) AS rollup_completed
    FROM markers
    WHERE business_dt >= '2024-10-09'
      AND marker_type_cd ILIKE '%Roll%'
    GROUP BY snapshot_type_cd, client_cd, processing_region_cd, business_dt
),

martload_stage AS (
    SELECT 
        snapshot_type_cd,
        client_cd,
        processing_region_cd,
        business_dt::timestamp AS business_dt,
        MAX(created_at) AS martload_started
    FROM markers
    WHERE business_dt >= '2024-10-09'
      AND marker_type_cd ILIKE '%Marts%'
    GROUP BY snapshot_type_cd, client_cd, processing_region_cd, business_dt
),

final_stage AS (
    SELECT 
        marker->'payload'->>'snapshot_type_cd' AS snapshot_type_cd,
        marker->'header'->>'party_cd' AS client_cd,
        marker->'header'->>'processing_region_cd' AS processing_region_cd,
        (marker->'payload'->>'business_date')::timestamp AS business_dt,
        MAX(created_at) AS final_completed
    FROM final_markers
    WHERE (marker->'payload'->>'business_date')::date >= '2024-10-09'
    GROUP BY 
        marker->'payload'->>'snapshot_type_cd',
        marker->'header'->>'party_cd',
        marker->'header'->>'processing_region_cd',
        (marker->'payload'->>'business_date')::timestamp
)

-- RAW stage
SELECT 
    CAST(r.snapshot_type_cd || '_raw' AS varchar) AS snapshot_stage,
    CAST(r.client_cd AS varchar) AS client_cd,
    CAST(r.processing_region_cd AS varchar) AS processing_region_cd,
    r.business_dt,
    r.raw_started AS started,
    r.raw_completed AS completed,
    (r.raw_completed - r.raw_started)::interval AS duration
FROM raw_stage r

UNION ALL

-- ENRICH stage
SELECT 
    CAST(r.snapshot_type_cd || '_enrichment' AS varchar) AS snapshot_stage,
    CAST(r.client_cd AS varchar) AS client_cd,
    CAST(r.processing_region_cd AS varchar) AS processing_region_cd,
    r.business_dt,
    r.raw_completed AS started,
    e.enrich_completed AS completed,
    (e.enrich_completed - r.raw_completed)::interval AS duration
FROM raw_stage r
JOIN enrich_stage e 
    ON r.snapshot_type_cd = e.snapshot_type_cd
   AND r.client_cd = e.client_cd
   AND r.processing_region_cd = e.processing_region_cd
   AND r.business_dt = e.business_dt

UNION ALL

-- ROLLUP stage
SELECT 
    CAST(r.snapshot_type_cd || '_rollup' AS varchar) AS snapshot_stage,
    CAST(r.client_cd AS varchar) AS client_cd,
    CAST(r.processing_region_cd AS varchar) AS processing_region_cd,
    r.business_dt,
    e.enrich_completed AS started,
    ro.rollup_completed AS completed,
    (ro.rollup_completed - e.enrich_completed)::interval AS duration
FROM raw_stage r
JOIN enrich_stage e 
    ON r.snapshot_type_cd = e.snapshot_type_cd
   AND r.client_cd = e.client_cd
   AND r.processing_region_cd = e.processing_region_cd
   AND r.business_dt = e.business_dt
JOIN rollup_stage ro 
    ON r.snapshot_type_cd = ro.snapshot_type_cd
   AND r.client_cd = ro.client_cd
   AND r.processing_region_cd = ro.processing_region_cd
   AND r.business_dt = ro.business_dt

UNION ALL

-- MARTLOAD stage
SELECT 
    CAST(r.snapshot_type_cd || '_martload' AS varchar) AS snapshot_stage,
    CAST(r.client_cd AS varchar) AS client_cd,
    CAST(r.processing_region_cd AS varchar) AS processing_region_cd,
    r.business_dt,
    ro.rollup_completed AS started,
    m.martload_started AS completed,
    (m.martload_started - ro.rollup_completed)::interval AS duration
FROM raw_stage r
JOIN rollup_stage ro 
    ON r.snapshot_type_cd = ro.snapshot_type_cd
   AND r.client_cd = ro.client_cd
   AND r.processing_region_cd = ro.processing_region_cd
   AND r.business_dt = ro.business_dt
JOIN martload_stage m 
    ON r.snapshot_type_cd = m.snapshot_type_cd
   AND r.client_cd = m.client_cd
   AND r.processing_region_cd = m.processing_region_cd
   AND r.business_dt = m.business_dt

UNION ALL

-- FINAL stage
SELECT 
    CAST(f.snapshot_type_cd || '_final' AS varchar) AS snapshot_stage,
    CAST(f.client_cd AS varchar) AS client_cd,
    CAST(f.processing_region_cd AS varchar) AS processing_region_cd,
    f.business_dt,
    m.martload_started AS started,
    f.final_completed AS completed,
    (f.final_completed - m.martload_started)::interval AS duration
FROM final_stage f
JOIN martload_stage m 
    ON f.snapshot_type_cd = m.snapshot_type_cd
   AND f.client_cd = m.client_cd
   AND f.processing_region_cd = m.processing_region_cd
   AND f.business_dt = m.business_dt

ORDER BY 
    duration DESC,
    client_cd,
    business_dt,
    snapshot_stage;
