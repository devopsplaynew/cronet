
-- trading_ars
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'trading_ars' AS workflow_type,
    CASE WHEN received_eagle_responses IS NOT NULL AND published_markers IS NOT NULL THEN 'completed'
         WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 'inprogress'
         ELSE 'pending' END AS status,
    CASE WHEN received_eagle_responses IS NOT NULL AND published_markers IS NOT NULL THEN 'completed'
         WHEN (received_eagle_responses IS NULL OR published_markers IS NULL) AND 
              (EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 15) THEN 'long_running'
         WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 'inprogress'
         ELSE 'pending' END AS status_with_long_running,
    MAX(created_at) AS last_updated,
    '{business_date}' AS business_dt
FROM ars_events
WHERE trigger_marker_type_cd = 'opsRegionEODTradingSignoff'
  AND client_cd = '{client}' 
  AND processing_region_cd = '{region}'
  AND business_dt = '{business_date}'
GROUP BY status, status_with_long_running
LIMIT 1;

-- pricing_ars
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'pricing_ars' AS workflow_type,
    CASE WHEN received_eagle_responses IS NOT NULL AND published_markers IS NOT NULL THEN 'completed'
         WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 'inprogress'
         ELSE 'pending' END AS status,
    CASE WHEN received_eagle_responses IS NOT NULL AND published_markers IS NOT NULL THEN 'completed'
         WHEN (received_eagle_responses IS NULL OR published_markers IS NULL) AND 
              (EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 15) THEN 'long_running'
         WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 'inprogress'
         ELSE 'pending' END AS status_with_long_running,
    MAX(created_at) AS last_updated,
    '{business_date}' AS business_dt
FROM ars_events
WHERE trigger_marker_type_cd = 'opsRegionEODPricingSignoff'
  AND client_cd = '{client}' 
  AND processing_region_cd = '{region}'
  AND business_dt = '{business_date}'
GROUP BY status, status_with_long_running
LIMIT 1;

-- pricing_marker
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'pricing_marker' AS workflow_type,
    CASE WHEN pm.pricing_event_id IS NULL THEN 'pending'
         WHEN pm.marker_type_cd = 'eodPXRegionSubjectAreaTransformed' THEN 'completed'
         ELSE 'inprogress' END AS status,
    CASE WHEN pm.marker_type_cd = 'eodPXRegionSubjectAreaTransformed' THEN 'completed'
         WHEN pm.pricing_event_id IS NULL AND 
              (EXTRACT(EPOCH FROM (NOW() - pe.created_at)/60) > 15) THEN 'long_running'
         WHEN pm.pricing_event_id IS NOT NULL THEN 'inprogress'
         ELSE 'pending' END AS status_with_long_running,
    MAX(pe.created_at) AS last_updated,
    '{business_date}' AS business_dt
FROM pricing_events pe
LEFT JOIN pricing_markers pm ON pe.id = pm.pricing_event_id
    AND pm.marker_type_cd = 'eodPXRegionSubjectAreaTransformed'
WHERE pe.client_cd = '{client}' 
  AND pe.processing_region_cd = '{region}'
  AND pe.business_dt = '{business_date}'
GROUP BY status, status_with_long_running
LIMIT 1;

-- eod_ars
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'eod_ars' AS workflow_type,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND business_dt = '{business_date}'
        ) THEN 'pending'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{business_date}'
        ) AND EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'accountingRegionEodclose'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{business_date}'
        ) THEN 'completed'
        ELSE 'inprogress' 
    END AS status,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND business_dt = '{business_date}'
        ) THEN 'pending'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{business_date}'
        ) AND EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'accountingRegionEodclose'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{business_date}'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
              AND business_dt = '{business_date}'
        ) AND (
            NOT EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = '{client}' 
                  AND processing_region_cd = '{region}'
                  AND trigger_marker_type_cd = 'accountingRegionEodclose'
                  AND received_eagle_responses IS NOT NULL
                  AND business_dt = '{business_date}'
            ) OR EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = '{client}' 
                  AND processing_region_cd = '{region}'
                  AND trigger_marker_type_cd = 'accountingRegionEodclose'
                  AND received_eagle_responses IS NULL
                  AND business_dt = '{business_date}'
            )
        ) THEN 'long_running'
        ELSE 'inprogress' 
    END AS status_with_long_running,
    (SELECT MAX(created_at) FROM ars_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND trigger_marker_type_cd IN ('opsRegionEodSignoff', 'accountingRegionEodclose')
       AND business_dt = '{business_date}'
    ) AS last_updated,
    '{business_date}' AS business_dt;

-- eod
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'eod' AS workflow_type,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND snapshot_type_cd='EOD'
          AND business_dt = '{business_date}'
    ) THEN 'completed'
    ELSE 'pending' END AS status,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND snapshot_type_cd='EOD'
          AND business_dt = '{business_date}'
    ) THEN 'completed'
    WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND snapshot_type_cd='EOD'
          AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
          AND business_dt = '{business_date}'
    ) THEN 'long_running'
    ELSE 'pending' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM accounting_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND snapshot_type_cd='EOD'
       AND business_dt = '{business_date}'
    ) AS last_updated,
    '{business_date}' AS business_dt;

-- eod_marker
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'eod_marker' AS workflow_type,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events ae
        JOIN markers m ON ae.id = m.accounting_event_id
        WHERE ae.client_cd = '{client}' 
          AND ae.processing_region_cd = '{region}'
          AND ae.snapshot_type_cd='EOD'
          AND m.marker_type = 'eodRegionSubjectAreaTransformed'
          AND ae.business_dt = '{business_date}'
    ) THEN 
        CASE WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers m
            JOIN accounting_events ae ON m.accounting_event_id = ae.id
            WHERE ae.client_cd = '{client}'
              AND ae.processing_region_cd = '{region}'
              AND ae.snapshot_type_cd='EOD'
              AND m.marker_type = 'eodRegionSubjectAreaTransformed'
              AND m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements')
              AND ae.business_dt = '{business_date}'
        ) = 6 THEN 'completed'
        ELSE 'inprogress' END
    ELSE 'pending' END AS status,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM accounting_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND snapshot_type_cd='EOD'
              AND business_dt = '{business_date}'
        ) THEN 'pending'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers m
            JOIN accounting_events ae ON m.accounting_event_id = ae.id
            WHERE ae.client_cd = '{client}'
              AND ae.processing_region_cd = '{region}'
              AND ae.snapshot_type_cd='EOD'
              AND m.marker_type = 'eodRegionSubjectAreaTransformed'
              AND m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements')
              AND ae.business_dt = '{business_date}'
        ) = 6 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM accounting_events
            WHERE client_cd = '{client}'
              AND processing_region_cd = '{region}'
              AND snapshot_type_cd='EOD'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
              AND business_dt = '{business_date}'
        ) THEN 'long_running'
        ELSE 'inprogress' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM accounting_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND snapshot_type_cd='EOD'
       AND business_dt = '{business_date}'
    ) AS last_updated,
    '{business_date}' AS business_dt;

-- asof_events
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'asof_events' AS workflow_type,
    CASE WHEN EXISTS (
        SELECT 1 FROM as_of_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
    ) THEN 'completed'
    ELSE 'pending' END AS status,
    CASE WHEN EXISTS (
        SELECT 1 FROM as_of_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
    ) THEN 'completed'
    ELSE 'pending' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM as_of_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND business_dt = '{business_date}'
    ) AS last_updated,
    '{business_date}' AS business_dt;

-- asof_marker
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'asof_marker' AS workflow_type,
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM as_of_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
    ) THEN 'pending'
    WHEN EXISTS (
        SELECT 1 FROM as_of_events ae
        JOIN as_of_request_markers arm ON ae.id = arm.as_of_event_id
        WHERE ae.client_cd = '{client}' 
          AND ae.processing_region_cd = '{region}'
          AND ae.business_dt = '{business_date}'
    ) THEN 'completed'
    ELSE 'inprogress' END AS status,
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM as_of_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
    ) THEN 'pending'
    WHEN EXISTS (
        SELECT 1 FROM as_of_events ae
        JOIN as_of_request_markers arm ON ae.id = arm.as_of_event_id
        WHERE ae.client_cd = '{client}' 
          AND ae.processing_region_cd = '{region}'
          AND ae.business_dt = '{business_date}'
    ) THEN 'completed'
    WHEN EXISTS (
        SELECT 1 FROM as_of_events
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
    ) THEN 'long_running'
    ELSE 'inprogress' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM as_of_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND business_dt = '{business_date}'
    ) AS last_updated,
    '{business_date}' AS business_dt;

-- aod
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'aod' AS workflow_type,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND snapshot_type_cd = 'AOD'
    ) THEN 'completed'
    ELSE 'pending' END AS status,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND snapshot_type_cd = 'AOD'
    ) THEN 'completed'
    ELSE 'pending' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM accounting_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND business_dt = '{business_date}'
       AND snapshot_type_cd = 'AOD'
    ) AS last_updated,
    '{business_date}' AS business_dt;

-- aod_marker
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'aod_marker' AS workflow_type,
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND snapshot_type_cd = 'AOD'
    ) THEN 'pending'
    WHEN (
        SELECT COUNT(DISTINCT ae.id) FROM accounting_events ae
        JOIN markers m ON ae.id = m.accounting_event_id
        WHERE ae.client_cd = '{client}'
          AND ae.processing_region_cd = '{region}'
          AND ae.business_dt = '{business_date}'
          AND ae.snapshot_type_cd = 'AOD'
          AND m.marker_type = 'asOfRegionSubjectAreaTransformed'
          AND m.subject_area_cd IN ('positions', 'taxlots')
        HAVING COUNT(DISTINCT m.subject_area_cd) = 2
    ) = (
        SELECT COUNT(*) FROM accounting_events
        WHERE client_cd = '{client}'
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND snapshot_type_cd = 'AOD'
    ) THEN 'completed'
    ELSE 'inprogress' END AS status,
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND snapshot_type_cd = 'AOD'
    ) THEN 'pending'
    WHEN (
        SELECT COUNT(DISTINCT ae.id) FROM accounting_events ae
        JOIN markers m ON ae.id = m.accounting_event_id
        WHERE ae.client_cd = '{client}'
          AND ae.processing_region_cd = '{region}'
          AND ae.business_dt = '{business_date}'
          AND ae.snapshot_type_cd = 'AOD'
          AND m.marker_type = 'asOfRegionSubjectAreaTransformed'
          AND m.subject_area_cd IN ('positions', 'taxlots')
        HAVING COUNT(DISTINCT m.subject_area_cd) = 2
    ) = (
        SELECT COUNT(*) FROM accounting_events
        WHERE client_cd = '{client}'
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND snapshot_type_cd = 'AOD'
    ) THEN 'completed'
    WHEN EXISTS (
        SELECT 1 FROM accounting_events
        WHERE client_cd = '{client}'
          AND processing_region_cd = '{region}'
          AND business_dt = '{business_date}'
          AND snapshot_type_cd = 'AOD'
          AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
    ) THEN 'long_running'
    ELSE 'inprogress' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM accounting_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND business_dt = '{business_date}'
       AND snapshot_type_cd = 'AOD'
    ) AS last_updated,
    '{business_date}' AS business_dt,
    (SELECT COUNT(*) FROM accounting_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND business_dt = '{business_date}'
       AND snapshot_type_cd = 'AOD'
    ) AS aod_entry_count,
    (SELECT COUNT(DISTINCT ae.id) FROM accounting_events ae
     JOIN markers m ON ae.id = m.accounting_event_id
     WHERE ae.client_cd = '{client}'
       AND ae.processing_region_cd = '{region}'
       AND ae.business_dt = '{business_date}'
       AND ae.snapshot_type_cd = 'AOD'
       AND m.marker_type = 'asOfRegionSubjectAreaTransformed'
       AND m.subject_area_cd = 'positions'
    ) AS positions_marker_count,
    (SELECT COUNT(DISTINCT ae.id) FROM accounting_events ae
     JOIN markers m ON ae.id = m.accounting_event_id
     WHERE ae.client_cd = '{client}'
       AND ae.processing_region_cd = '{region}'
       AND ae.business_dt = '{business_date}'
       AND ae.snapshot_type_cd = 'AOD'
       AND m.marker_type = 'asOfRegionSubjectAreaTransformed'
       AND m.subject_area_cd = 'taxlots'
    ) AS taxlots_marker_count;

-- sod_ars
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'sod_ars' AS workflow_type,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
              AND business_dt = '{sod_date}'
        ) THEN 'pending'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{sod_date}'
        ) AND EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'sodRegionGlobalProcessDone'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{sod_date}'
        ) THEN 'completed'
        ELSE 'inprogress' 
    END AS status,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
              AND business_dt = '{sod_date}'
        ) THEN 'pending'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{sod_date}'
        ) AND EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'sodRegionGlobalProcessDone'
              AND received_eagle_responses IS NOT NULL
              AND business_dt = '{sod_date}'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = '{client}' 
              AND processing_region_cd = '{region}'
              AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
              AND business_dt = '{sod_date}'
        ) AND (
            NOT EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = '{client}' 
                  AND processing_region_cd = '{region}'
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessDone'
                  AND received_eagle_responses IS NOT NULL
                  AND business_dt = '{sod_date}'
            ) OR EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = '{client}' 
                  AND processing_region_cd = '{region}'
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessDone'
                  AND received_eagle_responses IS NULL
                  AND business_dt = '{sod_date}'
            )
        ) THEN 'long_running'
        ELSE 'inprogress' 
    END AS status_with_long_running,
    (SELECT MAX(created_at) FROM ars_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND trigger_marker_type_cd IN ('sodRegionGlobalProcessTrigger', 'sodRegionGlobalProcessDone')
       AND business_dt = '{sod_date}'
    ) AS last_updated,
    '{sod_date}' AS business_dt;

-- sod
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'sod' AS workflow_type,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND snapshot_type_cd='SOD'
          AND business_dt = '{sod_date}'
    ) THEN 'completed'
    ELSE 'pending' END AS status,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND snapshot_type_cd='SOD'
          AND business_dt = '{sod_date}'
    ) THEN 'completed'
    WHEN EXISTS (
        SELECT 1 FROM accounting_events 
        WHERE client_cd = '{client}' 
          AND processing_region_cd = '{region}'
          AND snapshot_type_cd='SOD'
          AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
          AND business_dt = '{sod_date}'
    ) THEN 'long_running'
    ELSE 'pending' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM accounting_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND snapshot_type_cd='SOD'
       AND business_dt = '{sod_date}'
    ) AS last_updated,
    '{sod_date}' AS business_dt;

-- sod_marker
SELECT 
    '{client}' AS client_cd,
    '{region}' AS processing_region_cd,
    'sod_marker' AS workflow_type,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events ae
        JOIN markers m ON ae.id = m.accounting_event_id
        WHERE ae.client_cd = '{client}' 
          AND ae.processing_region_cd = '{region}'
          AND ae.snapshot_type_cd='SOD'
          AND m.marker_type = 'sodRegionSubjectAreaTransformed'
          AND ae.business_dt = '{sod_date}'
    ) THEN 
        CASE WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers m
            JOIN accounting_events ae ON m.accounting_event_id = ae.id
            WHERE ae.client_cd = '{client}'
              AND ae.processing_region_cd = '{region}'
              AND ae.snapshot_type_cd='SOD'
              AND m.marker_type = 'sodRegionSubjectAreaTransformed'
              AND m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements')
              AND ae.business_dt = '{sod_date}'
        ) = 6 THEN 'completed'
        ELSE 'inprogress' END
    ELSE 'pending' END AS status,
    CASE WHEN EXISTS (
        SELECT 1 FROM accounting_events ae
        JOIN markers m ON ae.id = m.accounting_event_id
        WHERE ae.client_cd = '{client}' 
          AND ae.processing_region_cd = '{region}'
          AND ae.snapshot_type_cd='SOD'
          AND m.marker_type = 'sodRegionSubjectAreaTransformed'
          AND ae.business_dt = '{sod_date}'
    ) THEN 
        CASE WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers m
            JOIN accounting_events ae ON m.accounting_event_id = ae.id
            WHERE ae.client_cd = '{client}'
              AND ae.processing_region_cd = '{region}'
              AND ae.snapshot_type_cd='SOD'
              AND m.marker_type = 'sodRegionSubjectAreaTransformed'
              AND m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements')
              AND ae.business_dt = '{sod_date}'
        ) = 6 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM accounting_events
            WHERE client_cd = '{client}'
              AND processing_region_cd = '{region}'
              AND snapshot_type_cd='SOD'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
              AND business_dt = '{sod_date}'
        ) THEN 'long_running'
        ELSE 'inprogress' END
    ELSE 'pending' END AS status_with_long_running,
    (SELECT MAX(created_at) FROM accounting_events 
     WHERE client_cd = '{client}' 
       AND processing_region_cd = '{region}'
       AND snapshot_type_cd='SOD'
       AND business_dt = '{sod_date}'
    ) AS last_updated,
    '{sod_date}' AS business_dt;

-- Original Message ID Queries

-- accounting_events_original_message_id
SELECT original_message_id 
FROM accounting_events 
WHERE client_cd = '{client}' 
  AND processing_region_cd = '{region}'
  AND business_dt = '{business_date}'
  AND snapshot_type_cd = 'EOD';

-- pricing_message_ids (ATLS DB)
SELECT 
    original_message_id,
    business_dt
FROM pricing_events
WHERE client_cd = '{client}'
  AND processing_region_cd = '{region}'
  AND business_dt = '{business_date}';