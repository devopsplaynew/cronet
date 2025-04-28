require 'pg'
require 'mail'
require 'time'

# Database configuration
DB_CONFIG = {
  host: 'your_db_host',
  dbname: 'your_db_name',
  user: 'your_db_user',
  password: 'your_db_password'
}

# Email configuration
Mail.defaults do
  delivery_method :smtp, {
    address: 'your_smtp_server',
    port: 587,
    domain: 'yourdomain.com',
    user_name: 'your_email@yourdomain.com',
    password: 'your_email_password',
    authentication: 'plain',
    enable_starttls_auto: true
  }
end

# Marker configurations
MARKER_PAIRS = [
  {
    first_marker: 'opsregioneodmarker',
    second_marker: 'daterolled',
    threshold_minutes: 30,
    pair_name: 'EOD Marker vs Date Rolled'
  },
  {
    first_marker: 'opsregiontradingsignoff',
    second_marker: 'globalprocessdone',
    threshold_minutes: 45,
    pair_name: 'Trading Signoff vs Global Process Done'
  },
  {
    first_marker: 'opsregionEODpricingsignoff',
    second_marker: 'eodvaluation',
    threshold_minutes: 30,
    pair_name: 'EOD Pricing Signoff vs Valuation'
  }
]

# Recipient configuration
ALERT_RECIPIENTS = [
  'operations-team@yourdomain.com',
  'it-support@yourdomain.com',
  'trading-ops@yourdomain.com'
]

def check_all_marker_pairs
  conn = PG.connect(DB_CONFIG)
  
  MARKER_PAIRS.each do |pair|
    check_marker_pair(conn, pair)
  end
  
  conn.close
rescue PG::Error => e
  puts "Database error: #{e.message}"
ensure
  conn.close if conn
end

def check_marker_pair(conn, pair)
  query = <<~SQL
    WITH first_markers AS (
        SELECT 
            client_id, 
            region, 
            created_at AS first_marker_time
        FROM marker_status
        WHERE marker_name = $1
        AND status = 'published'
        AND created_at >= NOW() - INTERVAL '24 hours'
    ),
    second_markers AS (
        SELECT 
            client_id, 
            region, 
            created_at AS second_marker_time
        FROM marker_status
        WHERE marker_name = $2
        AND status = 'published'
        AND created_at >= NOW() - INTERVAL '24 hours'
    ),
    paired_markers AS (
        SELECT 
            f.client_id,
            f.region,
            f.first_marker_time,
            s.second_marker_time,
            EXTRACT(EPOCH FROM (s.second_marker_time - f.first_marker_time))/60 AS duration_minutes
        FROM first_markers f
        LEFT JOIN second_markers s ON f.client_id = s.client_id 
                                  AND f.region = s.region
                                  AND s.second_marker_time > f.first_marker_time
    )
    SELECT 
        client_id,
        region,
        first_marker_time,
        second_marker_time,
        duration_minutes,
        CASE 
            WHEN second_marker_time IS NULL AND first_marker_time < NOW() - INTERVAL '#{pair[:threshold_minutes]} minutes' THEN 'missing'
            WHEN duration_minutes > #{pair[:threshold_minutes]} THEN 'delayed'
            WHEN second_marker_time IS NOT NULL THEN 'completed'
            ELSE 'first_only'
        END AS status
    FROM paired_markers
    ORDER BY client_id, region;
  SQL

  results = conn.exec_params(query, [pair[:first_marker], pair[:second_marker]])

  # Send notifications for new publications
  send_new_marker_notifications(pair[:first_marker], pair[:second_marker])

  # Process results and send alerts
  process_marker_results(results, pair)
end

def send_new_marker_notifications(first_marker, second_marker)
  # Get new first markers (published in last 15 minutes)
  new_first_markers = get_new_markers(first_marker, '15 minutes')
  
  new_first_markers.each do |marker|
    subject = "ℹ️ Marker Published: #{first_marker} for #{marker['client_id']}/#{marker['region']}"
    body = <<~BODY
      The #{first_marker} marker has been published.

      Details:
      Client: #{marker['client_id']}
      Region: #{marker['region']}
      Published At: #{marker['created_at']}

      Next expected marker: #{second_marker}
    BODY

    send_notification_email(subject, body)
  end

  # Get new second markers
  new_second_markers = get_new_markers(second_marker, '15 minutes')
  
  new_second_markers.each do |marker|
    subject = "✅ Marker Published: #{second_marker} for #{marker['client_id']}/#{marker['region']}"
    body = <<~BODY
      The #{second_marker} marker has been published successfully.

      Details:
      Client: #{marker['client_id']}
      Region: #{marker['region']}
      Published At: #{marker['created_at']}
    BODY

    send_notification_email(subject, body)
  end
end

def get_new_markers(marker_name, time_interval)
  conn = PG.connect(DB_CONFIG)
  
  query = <<~SQL
    SELECT client_id, region, created_at
    FROM marker_status
    WHERE marker_name = $1
    AND status = 'published'
    AND created_at >= NOW() - INTERVAL $2
    ORDER BY created_at DESC
  SQL
  
  results = conn.exec_params(query, [marker_name, time_interval])
  conn.close
  results
rescue PG::Error => e
  puts "Database error when fetching new markers: #{e.message}"
  []
ensure
  conn.close if conn
end

def process_marker_results(results, pair)
  results.each do |row|
    next unless ['missing', 'delayed'].include?(row['status'])

    client_id = row['client_id']
    region = row['region']
    first_time = row['first_marker_time']
    second_time = row['second_marker_time']
    duration = row['duration_minutes']

    if row['status'] == 'missing'
      subject = "🚨 ALERT: #{pair[:second_marker].upcase} MISSING for #{client_id}/#{region}"
      current_delay = ((Time.now - Time.parse(first_time))/60).round(1)
      body = <<~BODY
        #{pair[:pair_name]} Process Delay Alert

        The #{pair[:first_marker]} marker was published at #{first_time},
        but #{pair[:second_marker]} has NOT been published yet.
        (Threshold: #{pair[:threshold_minutes]} minutes, Current Delay: #{current_delay} minutes)

        Details:
        Client: #{client_id}
        Region: #{region}
        #{pair[:first_marker]} Time: #{first_time}

        Please investigate immediately.
      BODY
    else # delayed
      subject = "⚠️ WARNING: #{pair[:second_marker].upcase} DELAYED for #{client_id}/#{region}"
      body = <<~BODY
        #{pair[:pair_name]} Process Delay Alert

        The #{pair[:second_marker]} marker was published #{duration} minutes after #{pair[:first_marker]}
        (Threshold: #{pair[:threshold_minutes]} minutes).

        Details:
        Client: #{client_id}
        Region: #{region}
        #{pair[:first_marker]} Time: #{first_time}
        #{pair[:second_marker]} Time: #{second_time}
        Duration: #{duration} minutes

        Please review the process for potential issues.
      BODY
    end

    send_alert_email(subject, body)
  end
end

def send_alert_email(subject, body)
  Mail.deliver do
    from    'process-alerts@yourdomain.com'
    to      ALERT_RECIPIENTS
    subject subject
    body    body
  end
  puts "Sent alert: #{subject}"
rescue => e
  puts "Failed to send alert email: #{e.message}"
end

def send_notification_email(subject, body)
  Mail.deliver do
    from    'process-notifications@yourdomain.com'
    to      ALERT_RECIPIENTS
    subject subject
    body    body
  end
  puts "Sent notification: #{subject}"
rescue => e
  puts "Failed to send notification email: #{e.message}"
end

# Run the check
check_all_marker_pairs

=========

-- opsregioneodmarker vs daterolled
SELECT 
    'opsregioneodmarker vs daterolled' AS comparison,
    client_id,
    region,
    first_marker_time,
    second_marker_time,
    duration_minutes,
    status
FROM (
    WITH first_markers AS (SELECT client_id, region, created_at AS first_marker_time FROM marker_status WHERE marker_name = 'opsregioneodmarker' AND status = 'published' AND created_at >= NOW() - INTERVAL '24 hours'),
    second_markers AS (SELECT client_id, region, created_at AS second_marker_time FROM marker_status WHERE marker_name = 'daterolled' AND status = 'published' AND created_at >= NOW() - INTERVAL '24 hours'),
    paired_markers AS (
        SELECT f.client_id, f.region, f.first_marker_time, s.second_marker_time,
        EXTRACT(EPOCH FROM (s.second_marker_time - f.first_marker_time))/60 AS duration_minutes
        FROM first_markers f LEFT JOIN second_markers s ON f.client_id = s.client_id AND f.region = s.region AND s.second_marker_time > f.first_marker_time
    )
    SELECT client_id, region, first_marker_time, second_marker_time, duration_minutes,
    CASE 
        WHEN second_marker_time IS NULL AND first_marker_time < NOW() - INTERVAL '30 minutes' THEN 'missing'
        WHEN duration_minutes > 30 THEN 'delayed'
        WHEN second_marker_time IS NOT NULL THEN 'completed'
        ELSE 'first_only'
    END AS status
    FROM paired_markers
) AS eod_vs_daterolled
WHERE status != 'first_only'

UNION ALL

-- opsregiontradingsignoff vs globalprocessdone
SELECT 
    'opsregiontradingsignoff vs globalprocessdone' AS comparison,
    client_id,
    region,
    first_marker_time,
    second_marker_time,
    duration_minutes,
    status
FROM (
    WITH first_markers AS (SELECT client_id, region, created_at AS first_marker_time FROM marker_status WHERE marker_name = 'opsregiontradingsignoff' AND status = 'published' AND created_at >= NOW() - INTERVAL '24 hours'),
    second_markers AS (SELECT client_id, region, created_at AS second_marker_time FROM marker_status WHERE marker_name = 'globalprocessdone' AND status = 'published' AND created_at >= NOW() - INTERVAL '24 hours'),
    paired_markers AS (
        SELECT f.client_id, f.region, f.first_marker_time, s.second_marker_time,
        EXTRACT(EPOCH FROM (s.second_marker_time - f.first_marker_time))/60 AS duration_minutes
        FROM first_markers f LEFT JOIN second_markers s ON f.client_id = s.client_id AND f.region = s.region AND s.second_marker_time > f.first_marker_time
    )
    SELECT client_id, region, first_marker_time, second_marker_time, duration_minutes,
    CASE 
        WHEN second_marker_time IS NULL AND first_marker_time < NOW() - INTERVAL '45 minutes' THEN 'missing'
        WHEN duration_minutes > 45 THEN 'delayed'
        WHEN second_marker_time IS NOT NULL THEN 'completed'
        ELSE 'first_only'
    END AS status
    FROM paired_markers
) AS trading_vs_global
WHERE status != 'first_only'

UNION ALL

-- opsregionEODpricingsignoff vs eodvaluation
SELECT 
    'opsregionEODpricingsignoff vs eodvaluation' AS comparison,
    client_id,
    region,
    first_marker_time,
    second_marker_time,
    duration_minutes,
    status
FROM (
    WITH first_markers AS (SELECT client_id, region, created_at AS first_marker_time FROM marker_status WHERE marker_name = 'opsregionEODpricingsignoff' AND status = 'published' AND created_at >= NOW() - INTERVAL '24 hours'),
    second_markers AS (SELECT client_id, region, created_at AS second_marker_time FROM marker_status WHERE marker_name = 'eodvaluation' AND status = 'published' AND created_at >= NOW() - INTERVAL '24 hours'),
    paired_markers AS (
        SELECT f.client_id, f.region, f.first_marker_time, s.second_marker_time,
        EXTRACT(EPOCH FROM (s.second_marker_time - f.first_marker_time))/60 AS duration_minutes
        FROM first_markers f LEFT JOIN second_markers s ON f.client_id = s.client_id AND f.region = s.region AND s.second_marker_time > f.first_marker_time
    )
    SELECT client_id, region, first_marker_time, second_marker_time, duration_minutes,
    CASE 
        WHEN second_marker_time IS NULL AND first_marker_time < NOW() - INTERVAL '30 minutes' THEN 'missing'
        WHEN duration_minutes > 30 THEN 'delayed'
        WHEN second_marker_time IS NOT NULL THEN 'completed'
        ELSE 'first_only'
    END AS status
    FROM paired_markers
) AS pricing_vs_valuation
WHERE status != 'first_only'
ORDER BY comparison, client_id, region;
