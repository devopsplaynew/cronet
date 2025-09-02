-- batch_workflow_status
SELECT 
    client_cd,
    processing_region_cd,
    workflow_type,
    status,
    status_with_long_running,
    last_updated,
    business_dt
FROM (
    -- Trading ARS
    SELECT 
        client_cd,
        processing_region_cd,
        'trading_ars' AS workflow_type,
        CASE WHEN COUNT(CASE WHEN received_eagle_responses IS NOT NULL THEN 1 END) > 0 
              AND COUNT(CASE WHEN published_markers IS NOT NULL THEN 1 END) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 1 END) > 0 THEN 'inprogress'
             ELSE 'pending' END AS status,
        CASE WHEN COUNT(CASE WHEN received_eagle_responses IS NOT NULL THEN 1 END) > 0 
              AND COUNT(CASE WHEN published_markers IS NOT NULL THEN 1 END) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN (EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 15) 
                           AND (received_eagle_responses IS NULL OR published_markers IS NULL) THEN 1 END) > 0 THEN 'long_running'
             WHEN COUNT(CASE WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 1 END) > 0 THEN 'inprogress'
             ELSE 'pending' END AS status_with_long_running,
        MAX(created_at) AS last_updated,
        business_dt
    FROM ars_events
    WHERE trigger_marker_type_cd = 'opsRegionEODTradingSignoff'
      AND business_dt = '{business_date}'
      AND (client_cd, processing_region_cd) IN ({client_region_list})
    GROUP BY client_cd, processing_region_cd, business_dt
    
    UNION ALL
    
    -- Pricing ARS
    SELECT 
        client_cd,
        processing_region_cd,
        'pricing_ars' AS workflow_type,
        CASE WHEN COUNT(CASE WHEN received_eagle_responses IS NOT NULL THEN 1 END) > 0 
              AND COUNT(CASE WHEN published_markers IS NOT NULL THEN 1 END) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 1 END) > 0 THEN 'inprogress'
             ELSE 'pending' END AS status,
        CASE WHEN COUNT(CASE WHEN received_eagle_responses IS NOT NULL THEN 1 END) > 0 
              AND COUNT(CASE WHEN published_markers IS NOT NULL THEN 1 END) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN (EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 15) 
                           AND (received_eagle_responses IS NULL OR published_markers IS NULL) THEN 1 END) > 0 THEN 'long_running'
             WHEN COUNT(CASE WHEN received_eagle_responses IS NULL OR published_markers IS NULL THEN 1 END) > 0 THEN 'inprogress'
             ELSE 'pending' END AS status_with_long_running,
        MAX(created_at) AS last_updated,
        business_dt
    FROM ars_events
    WHERE trigger_marker_type_cd = 'opsRegionEODPricingSignoff'
      AND business_dt = '{business_date}'
      AND (client_cd, processing_region_cd) IN ({client_region_list})
    GROUP BY client_cd, processing_region_cd, business_dt
    
    UNION ALL
    
    -- Pricing Marker
    SELECT 
        pe.client_cd,
        pe.processing_region_cd,
        'pricing_marker' AS workflow_type,
        CASE WHEN COUNT(pm.pricing_event_id) = 0 THEN 'pending'
             WHEN COUNT(CASE WHEN pm.marker_type_cd = 'eodPXRegionSubjectAreaTransformed' THEN 1 END) > 0 THEN 'completed'
             ELSE 'inprogress' END AS status,
        CASE WHEN COUNT(CASE WHEN pm.marker_type_cd = 'eodPXRegionSubjectAreaTransformed' THEN 1 END) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN pm.pricing_event_id IS NULL 
                           AND (EXTRACT(EPOCH FROM (NOW() - pe.created_at)/60) > 15) THEN 1 END) > 0 THEN 'long_running'
             WHEN COUNT(CASE WHEN pm.pricing_event_id IS NOT NULL THEN 1 END) > 0 THEN 'inprogress'
             ELSE 'pending' END AS status_with_long_running,
        MAX(pe.created_at) AS last_updated,
        pe.business_dt
    FROM pricing_events pe
    LEFT JOIN pricing_markers pm ON pe.id = pm.pricing_event_id
        AND pm.marker_type_cd = 'eodPXRegionSubjectAreaTransformed'
    WHERE pe.business_dt = '{business_date}'
      AND (pe.client_cd, pe.processing_region_cd) IN ({client_region_list})
    GROUP BY pe.client_cd, pe.processing_region_cd, pe.business_dt
    
    UNION ALL
    
-- EOD ARS
SELECT 
    client_cd,
    processing_region_cd,
    'eod_ars' AS workflow_type,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND business_dt = ae.business_dt
        ) THEN 'pending'
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'accountingRegionEodclose'
              AND business_dt = ae.business_dt
        ) THEN 'pending'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND received_eagle_responses IS NOT NULL
              AND published_markers IS NOT NULL
              AND business_dt = ae.business_dt
        ) AND EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'accountingRegionEodclose'
              AND received_eagle_responses IS NOT NULL
              AND published_markers IS NOT NULL
              AND business_dt = ae.business_dt
        ) THEN 'completed'
        ELSE 'inprogress'
    END AS status,
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND business_dt = ae.business_dt
        ) THEN 'pending'
        WHEN NOT EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'accountingRegionEodclose'
              AND business_dt = ae.business_dt
        ) THEN 'pending'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND received_eagle_responses IS NOT NULL
              AND published_markers IS NOT NULL
              AND business_dt = ae.business_dt
        ) AND EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'accountingRegionEodclose'
              AND received_eagle_responses IS NOT NULL
              AND published_markers IS NOT NULL
              AND business_dt = ae.business_dt
        ) THEN 'completed'
        WHEN EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'opsRegionEodSignoff'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
              AND business_dt = ae.business_dt
        ) OR EXISTS (
            SELECT 1 FROM ars_events 
            WHERE client_cd = ae.client_cd 
              AND processing_region_cd = ae.processing_region_cd
              AND trigger_marker_type_cd = 'accountingRegionEodclose'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
              AND business_dt = ae.business_dt
        ) THEN 'long_running'
        ELSE 'inprogress'
    END AS status_with_long_running,
    MAX(created_at) AS last_updated,
    business_dt
FROM ars_events ae
WHERE business_dt = '{business_date}'
  AND (client_cd, processing_region_cd) IN ({client_region_list})
GROUP BY client_cd, processing_region_cd, business_dt
    
    UNION ALL
    
    -- EOD
    SELECT 
        client_cd,
        processing_region_cd,
        'eod' AS workflow_type,
        CASE WHEN COUNT(*) > 0 THEN 'completed' ELSE 'pending' END AS status,
        CASE WHEN COUNT(*) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30 THEN 1 END) > 0 THEN 'long_running'
             ELSE 'pending' END AS status_with_long_running,
        MAX(created_at) AS last_updated,
        business_dt
    FROM accounting_events
    WHERE snapshot_type_cd = 'EOD'
      AND business_dt = '{business_date}'
      AND (client_cd, processing_region_cd) IN ({client_region_list})
    GROUP BY client_cd, processing_region_cd, business_dt
    
    UNION ALL
    
    -- EOD Marker
    SELECT 
        ae.client_cd,
        ae.processing_region_cd,
        'eod_marker' AS workflow_type,
        CASE WHEN COUNT(m.accounting_event_id) = 0 THEN 'pending'
             WHEN COUNT(DISTINCT CASE WHEN m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements') 
                                   THEN m.subject_area_cd END) = 6 THEN 'completed'
             ELSE 'inprogress' END AS status,
        CASE WHEN COUNT(DISTINCT CASE WHEN m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements') 
                                   THEN m.subject_area_cd END) = 6 THEN 'completed'
             WHEN COUNT(CASE WHEN EXTRACT(EPOCH FROM (NOW() - ae.created_at)/60) > 30 THEN 1 END) > 0 THEN 'long_running'
             ELSE 'inprogress' END AS status_with_long_running,
        MAX(ae.created_at) AS last_updated,
        ae.business_dt
    FROM accounting_events ae
    LEFT JOIN markers m ON ae.id = m.accounting_event_id
        AND m.marker_type = 'eodRegionSubjectAreaTransformed'
    WHERE ae.snapshot_type_cd = 'EOD'
      AND ae.business_dt = '{business_date}'
      AND (ae.client_cd, ae.processing_region_cd) IN ({client_region_list})
    GROUP BY ae.client_cd, ae.processing_region_cd, ae.business_dt
    
    UNION ALL
    
    -- Asof Events
    SELECT 
        client_cd,
        processing_region_cd,
        'asof_events' AS workflow_type,
        CASE WHEN COUNT(*) > 0 THEN 'completed' ELSE 'pending' END AS status,
        CASE WHEN COUNT(*) > 0 THEN 'completed' ELSE 'pending' END AS status_with_long_running,
        MAX(created_at) AS last_updated,
        business_dt
    FROM as_of_events
    WHERE business_dt = '{business_date}'
      AND (client_cd, processing_region_cd) IN ({client_region_list})
    GROUP BY client_cd, processing_region_cd, business_dt
    
    UNION ALL
    
    -- Asof Marker
    SELECT 
        ae.client_cd,
        ae.processing_region_cd,
        'asof_marker' AS workflow_type,
        CASE WHEN COUNT(*) = 0 THEN 'pending'
             WHEN COUNT(arm.as_of_event_id) > 0 THEN 'completed'
             ELSE 'inprogress' END AS status,
        CASE WHEN COUNT(*) = 0 THEN 'pending'
             WHEN COUNT(arm.as_of_event_id) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN EXTRACT(EPOCH FROM (NOW() - ae.created_at)/60) > 30 THEN 1 END) > 0 THEN 'long_running'
             ELSE 'inprogress' END AS status_with_long_running,
        MAX(ae.created_at) AS last_updated,
        ae.business_dt
    FROM as_of_events ae
    LEFT JOIN as_of_request_markers arm ON ae.id = arm.as_of_event_id
    WHERE ae.business_dt = '{business_date}'
      AND (ae.client_cd, ae.processing_region_cd) IN ({client_region_list})
    GROUP BY ae.client_cd, ae.processing_region_cd, ae.business_dt
    
    UNION ALL
    
    -- AOD
    SELECT 
        client_cd,
        processing_region_cd,
        'aod' AS workflow_type,
        CASE WHEN COUNT(*) > 0 THEN 'completed' ELSE 'pending' END AS status,
        CASE WHEN COUNT(*) > 0 THEN 'completed' ELSE 'pending' END AS status_with_long_running,
        MAX(created_at) AS last_updated,
        business_dt
    FROM accounting_events
    WHERE snapshot_type_cd = 'AOD'
      AND business_dt = '{business_date}'
      AND (client_cd, processing_region_cd) IN ({client_region_list})
    GROUP BY client_cd, processing_region_cd, business_dt
    
    UNION ALL
    
    -- AOD Marker (REMOVED EXTRA COLUMNS)
    SELECT 
        ae.client_cd,
        ae.processing_region_cd,
        'aod_marker' AS workflow_type,
        CASE WHEN COUNT(*) = 0 THEN 'pending'
             WHEN COUNT(DISTINCT ae.id) = COUNT(DISTINCT CASE WHEN m.subject_area_cd IN ('positions', 'taxlots') THEN ae.id END) THEN 'completed'
             ELSE 'inprogress' END AS status,
        CASE WHEN COUNT(*) = 0 THEN 'pending'
             WHEN COUNT(DISTINCT ae.id) = COUNT(DISTINCT CASE WHEN m.subject_area_cd IN ('positions', 'taxlots') THEN ae.id END) THEN 'completed'
             WHEN COUNT(CASE WHEN EXTRACT(EPOCH FROM (NOW() - ae.created_at)/60) > 30 THEN 1 END) > 0 THEN 'long_running'
             ELSE 'inprogress' END AS status_with_long_running,
        MAX(ae.created_at) AS last_updated,
        ae.business_dt
    FROM accounting_events ae
    LEFT JOIN markers m ON ae.id = m.accounting_event_id
        AND m.marker_type = 'asOfRegionSubjectAreaTransformed'
        AND m.subject_area_cd IN ('positions', 'taxlots')
    WHERE ae.snapshot_type_cd = 'AOD'
      AND ae.business_dt = '{business_date}'
      AND (ae.client_cd, ae.processing_region_cd) IN ({client_region_list})
    GROUP BY ae.client_cd, ae.processing_region_cd, ae.business_dt
    
    UNION ALL
    
    -- SOD ARS
    SELECT 
        client_cd,
        processing_region_cd,
        'sod_ars' AS workflow_type,
        CASE WHEN NOT EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = ae.client_cd 
                  AND processing_region_cd = ae.processing_region_cd
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
                  AND business_dt = '{sod_date}'
            ) THEN 'pending'
            WHEN EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = ae.client_cd 
                  AND processing_region_cd = ae.processing_region_cd
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
                  AND received_eagle_responses IS NOT NULL
                  AND business_dt = '{sod_date}'
            ) AND EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = ae.client_cd 
                  AND processing_region_cd = ae.processing_region_cd
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessDone'
                  AND received_eagle_responses IS NOT NULL
                  AND business_dt = '{sod_date}'
            ) THEN 'completed'
            ELSE 'inprogress' END AS status,
        CASE WHEN NOT EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = ae.client_cd 
                  AND processing_region_cd = ae.processing_region_cd
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
                  AND business_dt = '{sod_date}'
            ) THEN 'pending'
            WHEN EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = ae.client_cd 
                  AND processing_region_cd = ae.processing_region_cd
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
                  AND received_eagle_responses IS NOT NULL
                  AND business_dt = '{sod_date}'
            ) AND EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = ae.client_cd 
                  AND processing_region_cd = ae.processing_region_cd
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessDone'
                  AND received_eagle_responses IS NOT NULL
                  AND business_dt = '{sod_date}'
            ) THEN 'completed'
            WHEN EXISTS (
                SELECT 1 FROM ars_events 
                WHERE client_cd = ae.client_cd 
                  AND processing_region_cd = ae.processing_region_cd
                  AND trigger_marker_type_cd = 'sodRegionGlobalProcessTrigger'
                  AND EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30
                  AND business_dt = '{sod_date}'
            ) AND (
                NOT EXISTS (
                    SELECT 1 FROM ars_events 
                    WHERE client_cd = ae.client_cd 
                      AND processing_region_cd = ae.processing_region_cd
                      AND trigger_marker_type_cd = 'sodRegionGlobalprocessDone'
                      AND received_eagle_responses IS NOT NULL
                      AND business_dt = '{sod_date}'
                ) OR EXISTS (
                    SELECT 1 FROM ars_events 
                    WHERE client_cd = ae.client_cd 
                      AND processing_region_cd = ae.processing_region_cd
                      AND trigger_marker_type_cd = 'sodRegionGlobalProcessDone'
                      AND received_eagle_responses IS NULL
                      AND business_dt = '{sod_date}'
                )
            ) THEN 'long_running'
            ELSE 'inprogress' END AS status_with_long_running,
        MAX(created_at) AS last_updated,
        '{sod_date}' AS business_dt
    FROM ars_events ae
    WHERE business_dt = '{sod_date}'
      AND (client_cd, processing_region_cd) IN ({client_region_list})
    GROUP BY client_cd, processing_region_cd
    
    UNION ALL
    
    -- SOD
    SELECT 
        client_cd,
        processing_region_cd,
        'sod' AS workflow_type,
        CASE WHEN COUNT(*) > 0 THEN 'completed' ELSE 'pending' END AS status,
        CASE WHEN COUNT(*) > 0 THEN 'completed'
             WHEN COUNT(CASE WHEN EXTRACT(EPOCH FROM (NOW() - created_at)/60) > 30 THEN 1 END) > 0 THEN 'long_running'
             ELSE 'pending' END AS status_with_long_running,
        MAX(created_at) AS last_updated,
        '{sod_date}' AS business_dt
    FROM accounting_events
    WHERE snapshot_type_cd = 'SOD'
      AND business_dt = '{sod_date}'
      AND (client_cd, processing_region_cd) IN ({client_region_list})
    GROUP BY client_cd, processing_region_cd
    
    UNION ALL
    
    -- SOD Marker
    SELECT 
        ae.client_cd,
        ae.processing_region_cd,
        'sod_marker' AS workflow_type,
        CASE WHEN COUNT(m.accounting_event_id) = 0 THEN 'pending'
             WHEN COUNT(DISTINCT CASE WHEN m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements') 
                                   THEN m.subject_area_cd END) = 6 THEN 'completed'
             ELSE 'inprogress' END AS status,
        CASE WHEN COUNT(DISTINCT CASE WHEN m.subject_area_cd IN ('positions', 'taxlots', 'traded_cash', 'disposal_lots', 'transactions', 'cash_settlements') 
                                   THEN m.subject_area_cd END) = 6 THEN 'completed'
             WHEN COUNT(CASE WHEN EXTRACT(EPOCH FROM (NOW() - ae.created_at)/60) > 30 THEN 1 END) > 0 THEN 'long_running'
             ELSE 'inprogress' END AS status_with_long_running,
        MAX(ae.created_at) AS last_updated,
        '{sod_date}' AS business_dt
    FROM accounting_events ae
    LEFT JOIN markers m ON ae.id = m.accounting_event_id
        AND m.marker_type = 'sodRegionSubjectAreaTransformed'
    WHERE ae.snapshot_type_cd = 'SOD'
      AND ae.business_dt = '{sod_date}'
      AND (ae.client_cd, ae.processing_region_cd) IN ({client_region_list})
    GROUP BY ae.client_cd, ae.processing_region_cd
) AS combined_results
ORDER BY client_cd, processing_region_cd, workflow_type;

-- ... (keep the rest of your file the same)