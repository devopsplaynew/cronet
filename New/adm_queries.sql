-- pricing_raw_status (ADM DB)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'raw_statement_loader'
        ) THEN 'failed'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete'
    ) AS started_at;

-- pricing_enrich_status (ADM DB)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxRegionSubjectAreaEnriched'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'enriched_statement_loader'
        ) THEN 'failed'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxAccountSubjectAreaEnriched'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionSubjectAreaEnriched'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionSubjectAreaEnrichStarted'
    ) AS started_at;

-- pricing_roll_status (ADM DB)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxRegionSubjectAreaRollupComplete'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'rollup'
        ) THEN 'failed'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxAccountSubjectAreaRollupComplete'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionSubjectAreaRollupComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionSubjectAreaRollupStarted'
    ) AS started_at;

-- pricing_mart_status (ADM DB)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxRegionValuationPricesMartLoadComplete'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'mln'
        ) THEN 'failed'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodpxAccountSubjectAreaMartLoadComplete'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionValuationPricesMartLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodpxRegionSubjectAreaMartLoadStarted'
    ) AS started_at;

-- eod_raw_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'eod_raw_loader'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete'
    ) AS started_at;

-- eod_enrich_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaEnriched'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'eod_enrichment_service'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaEnriched'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaEnrichStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionSubjectAreaEnriched'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionSubjectAreaEnrichStarted'
    ) AS started_at;

-- eod_roll_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaRollupComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'eod_rollup_service'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaRollupComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionSubjectAreaRollupStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionSubjectAreaRollupComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionSubjectAreaRollupStarted'
    ) AS started_at;

-- eod_mart_status (ADM DB)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionMartLoadComplete'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'eod_mart_loader'
        ) THEN 'failed'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'eodRegionMartloadStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionMartLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) from markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'eodRegionMartloadStarted'
    ) AS started_at;

-- eod_final_status (ADM DB)
SELECT 
    CASE 
        -- Check if eodRegionStatementsPublished exists AND eodRegionSubjectAreaPublished exists for all 5 subject areas
        WHEN EXISTS (
            SELECT 1 FROM final_markers 
            WHERE original_message_id = {message_id}
              AND marker_type = 'eodRegionStatementsPublished'
        ) AND (
            SELECT COUNT(DISTINCT marker->'payload'->>'subject_area_cd') 
            FROM final_markers 
            WHERE original_message_id = {message_id}
              AND marker_type = 'eodRegionSubjectAreaPublished'
              AND marker->'payload'->>'subject_area_cd' IN ('positions', 'disposal_lots', 'cash_settlements', 'transactions', 'taxlots')
        ) = 5 THEN 'completed'
        
        -- Check for errors
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'eod_final_publisher'
        ) THEN 'failed'
        
        -- Check if any markers exist but not all required ones
        WHEN EXISTS (
            SELECT 1 FROM final_markers 
            WHERE original_message_id = {message_id}
              AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished')
        ) THEN 'inprogress'
        
        -- Check for long running (more than 30 minutes since first marker started)
        WHEN EXISTS (
            SELECT 1 FROM final_markers
            WHERE original_message_id = {message_id}
              AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished')
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        
        ELSE 'pending'
    END AS status,
    
    -- Get the latest update time from any completed marker
    (SELECT MAX(created_at) FROM final_markers 
     WHERE original_message_id = {message_id}
       AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished')
    ) AS last_updated,
    
    -- Get the earliest start time from any marker
    (SELECT MIN(created_at) FROM final_markers
     WHERE original_message_id = {message_id}
       AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished')
    ) AS started_at;
	
-- sod_raw_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaRawLoadComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'sod_raw_loader'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaRawLoadComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaRawloadStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionSubjectAreaRawLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionSubjectAreaRawloadStarted'
    ) AS started_at;

-- sod_enrich_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaEnriched'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'sod_enrichment_service'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaEnriched'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaEnrichStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionSubjectAreaEnriched'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionSubjectAreaEnrichStarted'
    ) AS started_at;

-- sod_roll_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaRollupComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'sod_rollup_service'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaRollupComplete'
              AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionSubjectAreaRollupStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionSubjectAreaRollupComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionSubjectAreaRollupStarted'
    ) AS started_at;

-- sod_mart_status (ADM DB)
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM markers 
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionMartLoadComplete'
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'sod_mart_loader'
        ) THEN 'failed'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id = {message_id}
              AND marker_type_cd = 'sodRegionMartloadStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionMartLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id = {message_id}
       AND marker_type_cd = 'sodRegionMartloadStarted'
    ) AS started_at;

-- aod_raw_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete'
              AND m.subject_area_cd = 'positions'
        ) = {total_count} 
        AND (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete'
              AND m.subject_area_cd = 'taxlots'
        ) = {total_count} THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id IN ({message_ids_placeholder})
              AND service_nm = 'aod_raw_loader'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete'
              AND m.subject_area_cd IN ('positions', 'taxlots')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id IN ({message_ids_placeholder})
              AND marker_type_cd = 'asOfRegionSubjectAreaRawLoadStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaRawLoadStarted'
    ) AS started_at,
    -- Progress counts: total:positions:taxlots
    {total_count} AS total_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete'
       AND m.subject_area_cd = 'positions'
    ) AS positions_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete'
       AND m.subject_area_cd = 'taxlots'
    ) AS taxlots_count;

-- aod_enrich_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaEnriched'
              AND m.subject_area_cd = 'positions'
        ) = {total_count} 
        AND (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaEnriched'
              AND m.subject_area_cd = 'taxlots'
        ) = {total_count} THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id IN ({message_ids_placeholder})
              AND service_nm = 'aod_enrichment_service'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaEnriched'
              AND m.subject_area_cd IN ('positions', 'taxlots')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id IN ({message_ids_placeholder})
              AND marker_type_cd = 'asOfRegionSubjectAreaEnrichStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaEnriched'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaEnrichStarted'
    ) AS started_at,
    -- Progress counts: total:positions:taxlots
    {total_count} AS total_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaEnriched'
       AND m.subject_area_cd = 'positions'
    ) AS positions_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaEnriched'
       AND m.subject_area_cd = 'taxlots'
    ) AS taxlots_count;

-- aod_roll_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaRollupComplete'
              AND m.subject_area_cd = 'positions'
        ) = {total_count} 
        AND (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaRollupComplete'
              AND m.subject_area_cd = 'taxlots'
        ) = {total_count} THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id IN ({message_ids_placeholder})
              AND service_nm = 'aod_rollup_service'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaRollupComplete'
              AND m.subject_area_cd IN ('positions', 'taxlots')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id IN ({message_ids_placeholder})
              AND marker_type_cd = 'asOfRegionSubjectAreaRollupStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaRollupComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaRollupStarted'
    ) AS started_at,
    -- Progress counts: total:positions:taxlots
    {total_count} AS total_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaRollupComplete'
       AND m.subject_area_cd = 'positions'
    ) AS positions_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaRollupComplete'
       AND m.subject_area_cd = 'taxlots'
    ) AS taxlots_count;

-- aod_mart_status (ADM DB)
SELECT 
    CASE 
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete'
              AND m.subject_area_cd = 'positions'
        ) = {total_count} 
        AND (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete'
              AND m.subject_area_cd = 'taxlots'
        ) = {total_count} THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id IN ({message_ids_placeholder})
              AND service_nm = 'aod_mart_loader'
        ) THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT m.original_message_id) 
            FROM markers m
            WHERE m.original_message_id IN ({message_ids_placeholder})
              AND m.marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete'
              AND m.subject_area_cd IN ('positions', 'taxlots')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (
            SELECT 1 FROM markers
            WHERE original_message_id IN ({message_ids_placeholder})
              AND marker_type_cd = 'asOfRegionSubjectAreaMartLoadStarted'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(created_at) FROM markers 
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete'
    ) AS last_updated,
    (SELECT MAX(created_at) FROM markers
     WHERE original_message_id IN ({message_ids_placeholder})
       AND marker_type_cd = 'asOfRegionSubjectAreaMartLoadStarted'
    ) AS started_at,
    -- Progress counts: total:positions:taxlots
    {total_count} AS total_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete'
       AND m.subject_area_cd = 'positions'
    ) AS positions_count,
    (SELECT COUNT(DISTINCT m.original_message_id) 
     FROM markers m
     WHERE m.original_message_id IN ({message_ids_placeholder})
       AND m.marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete'
       AND m.subject_area_cd = 'taxlots'
    ) AS taxlots_count;

-- sod_final_status (ADM DB)
SELECT 
    CASE 
        -- Check if sodRegionStatementsPublished exists AND sodRegionSubjectAreaPublished exists for all 5 subject areas
        WHEN EXISTS (
            SELECT 1 FROM final_markers 
            WHERE original_message_id = {message_id}
              AND marker_type = 'sodRegionStatementsPublished'
        ) AND (
            SELECT COUNT(DISTINCT marker->'payload'->>'subject_area_cd') 
            FROM final_markers 
            WHERE original_message_id = {message_id}
              AND marker_type = 'sodRegionSubjectAreaPublished'
              AND marker->'payload'->>'subject_area_cd' IN ('positions', 'disposal_lots', 'cash_settlements', 'transactions', 'taxlots')
        ) = 5 THEN 'completed'
        
        -- Check for errors
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id = {message_id}
              AND service_nm = 'sod_final_publisher'
        ) THEN 'failed'
        
        -- Check if any markers exist but not all required ones
        WHEN EXISTS (
            SELECT 1 FROM final_markers 
            WHERE original_message_id = {message_id}
              AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished')
        ) THEN 'inprogress'
        
        -- Check for long running (more than 30 minutes since first marker started)
        WHEN EXISTS (
            SELECT 1 FROM final_markers
            WHERE original_message_id = {message_id}
              AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished')
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        
        ELSE 'pending'
    END AS status,
    
    -- Get the latest update time from any completed marker
    (SELECT MAX(created_at) FROM final_markers 
     WHERE original_message_id = {message_id}
       AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished')
    ) AS last_updated,
    
    -- Get the earliest start time from any marker
    (SELECT MIN(created_at) FROM final_markers
     WHERE original_message_id = {message_id}
       AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished')
    ) AS started_at;

-- aod_final_status (ADM DB)
SELECT 
    CASE 
        -- Check if asOfRegionsStatementsPublished exists for all AOD message IDs
        WHEN (
            SELECT COUNT(DISTINCT original_message_id) 
            FROM final_markers 
            WHERE original_message_id IN ({message_ids_placeholder})
              AND marker_type = 'asOfRegionsStatementsPublished'
        ) = {total_count} 
        -- Check if eodAllRegionStatementsPublished exists with the parent_original_message_id
        AND EXISTS (
            SELECT 1 FROM final_markers
            WHERE parent_original_message_id = '{parent_message_id}'
              AND marker_type = 'eodAllRegionStatementsPublished'
        ) THEN 'completed'
        
        -- Check for errors
        WHEN EXISTS (
            SELECT 1 FROM error_logs
            WHERE original_message_id IN ({message_ids_placeholder})
              AND service_nm = 'aod_final_publisher'
        ) THEN 'failed'
        
        -- Check if any markers exist but not all required ones
        WHEN EXISTS (
            SELECT 1 FROM final_markers 
            WHERE (original_message_id IN ({message_ids_placeholder}) 
                   AND marker_type = 'asOfRegionsStatementsPublished')
            OR (parent_original_message_id = '{parent_message_id}'
                AND marker_type = 'eodAllRegionStatementsPublished')
        ) THEN 'inprogress'
        
        -- Check for long running (more than 30 minutes since first marker started)
        WHEN EXISTS (
            SELECT 1 FROM final_markers
            WHERE (original_message_id IN ({message_ids_placeholder}) 
                   AND marker_type = 'asOfRegionsStatementsPublished')
            OR (parent_original_message_id = '{parent_message_id}'
                AND marker_type = 'eodAllRegionStatementsPublished')
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
        ) THEN 'long_running'
        
        ELSE 'pending'
    END AS status,
    
    -- Get the latest update time from any completed marker
    (SELECT MAX(created_at) FROM final_markers 
     WHERE (original_message_id IN ({message_ids_placeholder}) 
            AND marker_type = 'asOfRegionsStatementsPublished')
        OR (parent_original_message_id = '{parent_message_id}'
            AND marker_type = 'eodAllRegionStatementsPublished')
    ) AS last_updated,
    
    -- Get the earliest start time from any marker
    (SELECT MIN(created_at) FROM final_markers
     WHERE (original_message_id IN ({message_ids_placeholder}) 
            AND marker_type = 'asOfRegionsStatementsPublished')
        OR (parent_original_message_id = '{parent_message_id}'
            AND marker_type = 'eodAllRegionStatementsPublished')
    ) AS started_at;