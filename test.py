require 'pg'
require 'mail'

# Configuration
DB_CONFIG = {
  host: 'your_db_host',
  dbname: 'your_db_name',
  user: 'your_db_user',
  password: 'your_db_password'
}.freeze

MAIL_CONFIG = {
  from: 'marker-alerts@yourdomain.com',
  to: ['operations-team@yourdomain.com'],
  smtp: {
    address: 'smtp.yourdomain.com',
    port: 587,
    domain: 'yourdomain.com',
    user_name: 'alerts@yourdomain.com',
    password: 'your_email_password',
    authentication: 'plain',
    enable_starttls_auto: true
  }
}.freeze

def check_missing_markers
  conn = PG.connect(DB_CONFIG)
  
  results = conn.exec(<<~SQL)
    WITH marker_pairs AS (
      SELECT 'opsregioneodmarker' AS first_marker, 'daterolled' AS second_marker
      UNION SELECT 'opsregiontradingsignoff', 'globalprocessdone'
      UNION SELECT 'opsregionEODpricingsignoff', 'eodvaluation'
    ),
    first_markers AS (
      SELECT mp.*, ms.client_id, ms.region, ms.created_at AS first_time
      FROM marker_status ms
      JOIN marker_pairs mp ON ms.marker_name = mp.first_marker
      WHERE ms.status = 'published'
      AND ms.created_at >= NOW() - INTERVAL '24 hours'
    )
    SELECT 
      fm.first_marker,
      fm.second_marker,
      fm.client_id,
      fm.region,
      fm.first_time,
      EXTRACT(EPOCH FROM (NOW() - fm.first_time))/60 AS minutes_missing
    FROM first_markers fm
    WHERE NOT EXISTS (
      SELECT 1 FROM marker_status ms
      WHERE ms.marker_name = fm.second_marker
      AND ms.client_id = fm.client_id
      AND ms.region = fm.region
      AND ms.status = 'published'
      AND ms.created_at > fm.first_time
    )
    AND fm.first_time < NOW() - INTERVAL '30 minutes'
    ORDER BY minutes_missing DESC
  SQL

  send_alerts(results) if results.any?
rescue PG::Error => e
  puts "Database error: #{e.message}"
ensure
  conn&.close
end

def send_alerts(results)
  Mail.defaults { delivery_method :smtp, MAIL_CONFIG[:smtp] }

  results.each do |row|
    mail = Mail.new do
      from    MAIL_CONFIG[:from]
      to      MAIL_CONFIG[:to]
      subject "🚨 MISSING: #{row['second_marker']} for #{row['client_id']}/#{row['region']}"
      body    <<~BODY
        #{row['first_marker']} was published at #{row['first_time']}
        but #{row['second_marker']} has not been published yet.
        
        Time elapsed: #{row['minutes_missing'].to_f.round(1)} minutes
        
        Client: #{row['client_id']}
        Region: #{row['region']}
        
        Immediate action required!
      BODY
    end
    
    mail.deliver!
    puts "Alert sent for #{row['client_id']}/#{row['region']} - #{row['second_marker']} missing"
  end
rescue => e
  puts "Failed to send alert: #{e.message}"
end

# Run the check
check_missing_markers
