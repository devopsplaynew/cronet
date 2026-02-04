WITH markers_cte AS (
    SELECT 
        accounting_event_id,
        marker_type,
        subject_area_cd,
        created_at AS created_ts
    FROM markers
),

start_times AS (
    SELECT 
        snapshot_type_cd, 
        client_cd, 
        processing_region_cd, 
        business_dt::timestamp AS business_dt, 
        MIN(created_at) AS started
    FROM accounting_events
    WHERE business_dt >= '2024-10-01'
    GROUP BY snapshot_type_cd, client_cd, processing_region_cd, business_dt
),

end_times AS (
    SELECT 
        ae.snapshot_type_cd, 
        ae.client_cd, 
        ae.processing_region_cd, 
        ae.business_dt::timestamp AS business_dt, 
        MAX(m.created_ts) AS completed
    FROM markers_cte m
    JOIN accounting_events ae 
        ON m.accounting_event_id = ae.id
    WHERE ae.business_dt >= '2024-10-01'
    GROUP BY ae.snapshot_type_cd, ae.client_cd, ae.processing_region_cd, ae.business_dt
)

-- EOD / SOD processes using ars_events
SELECT
    CAST('EODARS' AS varchar) AS process_type,
    ar.client_cd,
    ar.processing_region_cd,
    ar.business_dt::timestamp AS business_dt,
    ar.created_at AS started,
    ar.created_at AS completed,
    ar.created_at - ar.created_at AS duration
FROM ars_events ar
WHERE ar.trigger_marker_type_cd = 'opsRegionEodSignoff'

UNION ALL

SELECT
    CAST('SODARS' AS varchar) AS process_type,
    ar.client_cd,
    ar.processing_region_cd,
    ar.business_dt::timestamp AS business_dt,
    ar.created_at AS started,
    ar.created_at AS completed,
    ar.created_at - ar.created_at AS duration
FROM ars_events ar
WHERE ar.trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'

UNION ALL

-- Snapshot processes from accounting_events + markers
SELECT
    CAST(s.snapshot_type_cd AS varchar) AS process_type,
    s.client_cd,
    s.processing_region_cd,
    s.business_dt,
    s.started,
    e.completed,
    (e.completed - s.started) AS duration
FROM start_times s
JOIN end_times e 
    ON s.snapshot_type_cd = e.snapshot_type_cd
   AND s.client_cd = e.client_cd
   AND s.processing_region_cd = e.processing_region_cd
   AND s.business_dt = e.business_dt

ORDER BY process_type, client_cd, business_dt;
