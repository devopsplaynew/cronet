WITH marker_data AS (
    SELECT
        original_message_id,
        marker_type_cd,
        subject_area_cd,
        MAX(created_at) AS max_created_at,
        BOOL_OR(EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30) AS is_long_running
    FROM adu.markers
    WHERE original_message_id IN ({message_id})
       OR original_message_id IN ({message_ids_placeholder})
    GROUP BY 1, 2, 3
),
error_data AS (
    SELECT
        original_message_id,
        service_nm,
        BOOL_OR(TRUE) AS has_error  -- replaced 1 with TRUE
    FROM adu.error_logs
    WHERE original_message_id IN ({message_id})
       OR original_message_id IN ({message_ids_placeholder})
    GROUP BY 1, 2
),
final_marker_data AS (
    SELECT
        original_message_id,
        parent_original_message_id,
        marker_type,
        (marker->'payload'->>'subject_area_cd') AS subject_area_cd,
        MAX(created_at) AS max_created_at,
        MIN(created_at) AS min_created_at,
        BOOL_OR(EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30) AS is_long_running
    FROM adu.final_markers
    WHERE (original_message_id IN ({message_ids_placeholder}) AND marker_type = 'asOfRegionsStatementsPublished')
       OR (parent_original_message_id = '{parent_message_id}' AND marker_type = 'eodAllRegionStatementsPublished')
       OR original_message_id IN ({message_id})
    GROUP BY 1, 2, 3, 4
)
-- Pricing Raw Status
SELECT
    'pricing_raw_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete') THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'raw_statement_loader') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaRawLoadComplete' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- Pricing Enrich Status
SELECT
    'pricing_enrich_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaEnriched') THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'enriched_statement_loader') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxAccountSubjectAreaEnriched' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaEnriched' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaEnriched' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- Pricing Roll Status
SELECT
    'pricing_roll_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaRollupComplete') THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'rollup') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxAccountSubjectAreaRollupComplete' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaRollupComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionSubjectAreaRollupComplete' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- Pricing Mart Status
SELECT
    'pricing_mart_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionValuationPricesMartLoadComplete') THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'mln') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxAccountSubjectAreaMartLoadComplete' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionValuationPricesMartLoadComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodpxRegionValuationPricesMartLoadComplete' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- EOD Raw Status
SELECT
    'eod_raw_status' AS workflow_type,
    CASE
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'eod_raw_loader') THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaRawLoadComplete' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- EOD Enrich Status
SELECT
    'eod_enrich_status' AS workflow_type,
    CASE
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'eodRegionSubjectAreaEnriched'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'eod_enrichment_service') THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'eodRegionSubjectAreaEnriched'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaEnrichStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaEnriched' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaEnrichStarted' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- EOD Roll Status
SELECT
    'eod_roll_status' AS workflow_type,
    CASE
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'eodRegionSubjectAreaRollupComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'eod_rollup_service') THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'eodRegionSubjectAreaRollupComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaRollupStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaRollupComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionSubjectAreaRollupStarted' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- EOD Mart Status
SELECT
    'eod_mart_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionMartLoadComplete') THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'eod_mart_loader') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionMartloadStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionMartLoadComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'eodRegionMartloadStarted' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- EOD Final Status
SELECT
    'eod_final_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type = 'eodRegionStatementsPublished')
        AND (
            SELECT COUNT(DISTINCT subject_area_cd) FROM final_marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type = 'eodRegionSubjectAreaPublished'
            AND subject_area_cd IN ('positions', 'disposal_lots', 'cash_settlements', 'transactions', 'taxlots')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'eod_final_publisher') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished')) THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished') AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(max_created_at) FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished')) AS last_updated,
    (SELECT MIN(min_created_at) FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('eodRegionStatementsPublished', 'eodRegionSubjectAreaPublished')) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- SOD Raw Status
SELECT
    'sod_raw_status' AS workflow_type,
    CASE
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'sodRegionSubjectAreaRawLoadComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'sod_raw_loader') THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'sodRegionSubjectAreaRawLoadComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaRawloadStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaRawLoadComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaRawloadStarted' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- SOD Enrich Status
SELECT
    'sod_enrich_status' AS workflow_type,
    CASE
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'sodRegionSubjectAreaEnriched'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'sod_enrichment_service') THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'sodRegionSubjectAreaEnriched'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaEnrichStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaEnriched' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaEnrichStarted' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- SOD Roll Status
SELECT
    'sod_roll_status' AS workflow_type,
    CASE
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'sodRegionSubjectAreaRollupComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'sod_rollup_service') THEN 'failed'
        WHEN (
            SELECT COUNT(DISTINCT subject_area_cd) FROM marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type_cd = 'sodRegionSubjectAreaRollupComplete'
            AND subject_area_cd IN ('taxlots', 'positions', 'disposal_lots', 'transactions', 'cash_settlements')
        ) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaRollupStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaRollupComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionSubjectAreaRollupStarted' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- SOD Mart Status
SELECT
    'sod_mart_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionMartLoadComplete') THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'sod_mart_loader') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionMartloadStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionMartLoadComplete' LIMIT 1) AS last_updated,
    (SELECT max_created_at FROM marker_data WHERE original_message_id IN ({message_id}) AND marker_type_cd = 'sodRegionMartloadStarted' LIMIT 1) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- SOD Final Status
SELECT
    'sod_final_status' AS workflow_type,
    CASE
        WHEN EXISTS (SELECT 1 FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type = 'sodRegionStatementsPublished')
        AND (
            SELECT COUNT(DISTINCT subject_area_cd) FROM final_marker_data
            WHERE original_message_id IN ({message_id})
            AND marker_type = 'sodRegionSubjectAreaPublished'
            AND subject_area_cd IN ('positions', 'disposal_lots', 'cash_settlements', 'transactions', 'taxlots')
        ) = 5 THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE original_message_id IN ({message_id}) AND service_nm = 'sod_final_publisher') THEN 'failed'
        WHEN EXISTS (SELECT 1 FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished')) THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished') AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(max_created_at) FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished')) AS last_updated,
    (SELECT MIN(min_created_at) FROM final_marker_data WHERE original_message_id IN ({message_id}) AND marker_type IN ('sodRegionStatementsPublished', 'sodRegionSubjectAreaPublished')) AS started_at,
    NULL::integer AS total_count,
    NULL::integer AS positions_count,
    NULL::integer AS taxlots_count

UNION ALL

-- AOD Raw Status
SELECT
    'aod_raw_status' AS workflow_type,
    CASE
        WHEN (
            (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete' AND subject_area_cd = 'positions') = {total_count}
            AND (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete' AND subject_area_cd = 'taxlots') = {total_count}
        ) THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE service_nm = 'aod_raw_loader') THEN 'failed'
        WHEN (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete' AND subject_area_cd IN ('positions', 'taxlots')) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadStarted' AND is_long_running) THEN 'Long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete') AS last_updated,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadStarted') AS started_at,
    {total_count}::integer AS total_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete' AND subject_area_cd = 'positions')::integer AS positions_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRawLoadComplete' AND subject_area_cd = 'taxlots')::integer AS taxlots_count

UNION ALL

-- AOD Enrich Status
SELECT
    'aod_enrich_status' AS workflow_type,
    CASE
        WHEN (
            (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnriched' AND subject_area_cd = 'positions') = {total_count}
            AND (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnriched' AND subject_area_cd = 'taxlots') = {total_count}
        ) THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE service_nm = 'aod_enrichment_service') THEN 'failed'
        WHEN (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnriched' AND subject_area_cd IN ('positions', 'taxlots')) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnrichStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnriched') AS last_updated,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnrichStarted') AS started_at,
    {total_count}::integer AS total_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnriched' AND subject_area_cd = 'positions')::integer AS positions_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaEnriched' AND subject_area_cd = 'taxlots')::integer AS taxlots_count

UNION ALL

-- AOD Roll Status
SELECT
    'aod_roll_status' AS workflow_type,
    CASE
        WHEN (
            (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupComplete' AND subject_area_cd = 'positions') = {total_count}
            AND (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupComplete' AND subject_area_cd = 'taxlots') = {total_count}
        ) THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE service_nm = 'aod_rollup_service') THEN 'failed'
        WHEN (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupComplete' AND subject_area_cd IN ('positions', 'taxlots')) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupComplete') AS last_updated,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupStarted') AS started_at,
    {total_count}::integer AS total_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupComplete' AND subject_area_cd = 'positions')::integer AS positions_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaRollupComplete' AND subject_area_cd = 'taxlots')::integer AS taxlots_count

UNION ALL

-- AOD Mart Status
SELECT
    'aod_mart_status' AS workflow_type,
    CASE
        WHEN (
            (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete' AND subject_area_cd = 'positions') = {total_count}
            AND (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete' AND subject_area_cd = 'taxlots') = {total_count}
        ) THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE service_nm = 'aod_mart_loader') THEN 'failed'
        WHEN (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete' AND subject_area_cd IN ('positions', 'taxlots')) > 0 THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadStarted' AND is_long_running) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete') AS last_updated,
    (SELECT MAX(max_created_at) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadStarted') AS started_at,
    {total_count}::integer AS total_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete' AND subject_area_cd = 'positions')::integer AS positions_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM marker_data WHERE marker_type_cd = 'asOfRegionSubjectAreaMartLoadComplete' AND subject_area_cd = 'taxlots')::integer AS taxlots_count

UNION ALL

-- AOD Final Status
SELECT 
    'aod_final_status' AS workflow_type,
    CASE 
        WHEN (
            (SELECT COUNT(DISTINCT original_message_id) FROM final_marker_data WHERE marker_type = 'asOfRegionsStatementsPublished') = {total_count}
            AND EXISTS (SELECT 1 FROM final_marker_data WHERE parent_original_message_id = '{parent_message_id}' AND marker_type = 'eodAllRegionStatementsPublished')
        ) THEN 'completed'
        WHEN EXISTS (SELECT 1 FROM error_data WHERE service_nm = 'aod_final_publisher') THEN 'failed'
        WHEN (
            EXISTS (SELECT 1 FROM final_marker_data WHERE marker_type = 'asOfRegionsStatementsPublished')
            OR EXISTS (SELECT 1 FROM final_marker_data WHERE parent_original_message_id = '{parent_message_id}' AND marker_type = 'eodAllRegionStatementsPublished')
        ) THEN 'inprogress'
        WHEN EXISTS (SELECT 1 FROM final_marker_data WHERE 
            (marker_type = 'asOfRegionsStatementsPublished' OR (parent_original_message_id = '{parent_message_id}' AND marker_type = 'eodAllRegionStatementsPublished'))
            AND is_long_running
        ) THEN 'long_running'
        ELSE 'pending'
    END AS status,
    (SELECT MAX(max_created_at) FROM final_marker_data WHERE 
        marker_type = 'asOfRegionsStatementsPublished' 
        OR (parent_original_message_id = '{parent_message_id}' AND marker_type = 'eodAllRegionStatementsPublished')
    ) AS last_updated,
    (SELECT MIN(min_created_at) FROM final_marker_data WHERE 
        marker_type = 'asOfRegionsStatementsPublished' 
        OR (parent_original_message_id = '{parent_message_id}' AND marker_type = 'eodAllRegionStatementsPublished')
    ) AS started_at,
    {total_count}::integer AS total_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM final_marker_data WHERE marker_type = 'asOfRegionsStatementsPublished')::integer AS positions_count,
    (SELECT COUNT(DISTINCT original_message_id) FROM final_marker_data WHERE marker_type = 'asOfRegionsStatementsPublished')::integer AS taxlots_count