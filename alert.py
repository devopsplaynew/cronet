def process_data(df):
    """Process data with proper handling for missing transformed rows"""
    alerts = {}
    
    if df.empty:
        print("Warning: No data returned from database")
        return []
    
    try:
        current_est = get_current_est()
        
        # Group by message groups
        grouped = df.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd',
                            'business_dt', 'original_message_id'])
        
        for group_key, group_df in grouped:
            try:
                msg_id = group_key[4]
                
                # Get accounting event (should always exist)
                accounting_row = group_df[group_df['marker_type'] == 'accounting_events']
                if accounting_row.empty:
                    continue
                
                # Get processing time (required)
                if 'processing_time' not in accounting_row.columns or accounting_row['processing_time'].isnull().all():
                    print(f"Warning: Missing processing time for accounting event {msg_id}")
                    continue
                    
                accounting_time = pd.to_datetime(accounting_row['processing_time'].iloc[0])
                duration = (current_est - accounting_time).total_seconds() / 60
                
                # Only check if accounting event is older than 30 mins
                if duration > 30:
                    # Check for transformed rows
                    transformed_rows = group_df[
                        group_df['marker_type'].str.contains('Transformed', case=False, na=False)
                    ]
                    
                    # CASE 1: Missing transformed marker entirely
                    if transformed_rows.empty:
                        alerts[msg_id] = create_alert(
                            group_key,
                            duration,
                            'Missing transformed marker'
                        )
                    
                    # CASE 2: Transformed exists but check counts
                    else:
                        snapshot_type = group_key[2]
                        required_count = 6 if snapshot_type in ['EOD', 'SOD'] else 2 if snapshot_type == 'AOD' else None
                        
                        for _, row in transformed_rows.iterrows():
                            # Skip if missing processing time
                            if pd.isnull(row.get('processing_time')):
                                continue
                                
                            transformed_time = pd.to_datetime(row['processing_time'])
                            transform_duration = (transformed_time - accounting_time).total_seconds() / 60
                            
                            # Check count mismatch after 30 mins
                            if (required_count is not None and 
                                'count' in row and 
                                row['count'] != required_count and 
                                transform_duration > 30):
                                alerts[msg_id] = create_alert(
                                    group_key,
                                    transform_duration,
                                    f'Count mismatch (expected {required_count}, got {row["count"]})'
                                )
                            
                            # Check slow processing (>30 mins)
                            if transform_duration > 30:
                                alerts[msg_id] = create_alert(
                                    group_key,
                                    transform_duration,
                                    'Slow processing (>30 mins)'
                                )
                                
            except Exception as e:
                print(f"Error processing message {msg_id}: {str(e)}")
                continue
                
    except Exception as e:
        print(f"Processing error: {str(e)}")
        
    return list(alerts.values())
