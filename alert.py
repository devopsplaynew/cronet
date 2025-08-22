import psycopg2
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import time
import pytz

# ===== DB Configs =====
DB1_CONFIG = {"host":"localhost","port":"5432","dbname":"atls","user":"admin","password":"admin"}
DB2_CONFIG = {"host":"localhost","port":"5432","dbname":"adm","user":"admin","password":"admin"}

# ===== Email Config =====
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "praveenangel53@gmail.com"
EMAIL_PASS = "rxbn mpuu auht ldxi"
EMAIL_TO = ["praveenangel53@gmail.com"]

# ===== Timezone =====
EST = pytz.timezone("US/Eastern")
LOG_FILE = "aodgl_alerts.log"

def write_log(msg):
    ts = datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{ts}] {msg}\n")
    print(f"[LOG] {msg}")

def send_email(subject, body):
    write_log(f"EMAIL SENT: {subject} - {body}")
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_USER
    msg['To'] = ", ".join(EMAIL_TO)
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
            s.starttls()
            s.login(EMAIL_USER, EMAIL_PASS)
            s.sendmail(EMAIL_USER, EMAIL_TO, msg.as_string())
    except Exception as e:
        write_log(f"EMAIL FAILED: {e}")

def get_business_date(now):
    # Fixed business_date between 21:45 → 03:00
    if now.weekday() in (5,6):
        return None
    return (now - timedelta(days=1) if now.hour < 3 else now).strftime("%Y-%m-%d")

def within_schedule(now):
    if now.weekday() in (5,6):
        return False
    start = now.replace(hour=11, minute=45, second=0, microsecond=0)
    end = (now + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
    if now.hour < 3:
        start -= timedelta(days=1)
    return start <= now <= end

def check_db2_files(business_date):
    conn2 = psycopg2.connect(**DB2_CONFIG)
    cur2 = conn2.cursor()
    cur2.execute("""
        SELECT file_name
        FROM file_marker
        WHERE business_date=%s AND party_cd='VYA' AND processing_region_cd='AMER' AND entity_id IS NOT NULL
          AND file_name NOT IN (
              SELECT file_name
              FROM files
              WHERE business_date=%s AND party_cd='VYA' AND processing_region_cd='AMER' AND file_type='GLAOD'
          )
    """, (business_date, business_date))
    missing = cur2.fetchall()
    alert_triggered = False
    if missing:
        files_list = ', '.join([f[0] for f in missing])
        send_email("AODGL FILE ALERT", f"Missing files for business_date={business_date}: {files_list}")
        write_log(f"DB2 alert triggered: {files_list}")
        alert_triggered = True
    else:
        write_log(f"DB2 check OK, no missing files for bd={business_date}")
    cur2.close()
    conn2.close()
    return alert_triggered

def check_alerts():
    now = datetime.now(EST)
    bd = get_business_date(now)
    if not bd:
        write_log("Weekend - skipping alerts")
        return
    
    write_log(f"Checking alerts for business_date: {bd}")
    
    conn = psycopg2.connect(**DB1_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT marker_template_id, created_at
        FROM input_markers
        WHERE marker_template_id IN ('3','4') AND business_date=%s
        ORDER BY created_at DESC
    """, (bd,))
    markers = cur.fetchall()
    marker3 = marker4 = None
    for m_id, t in markers:
        if m_id == '3' and not marker3:
            marker3 = t.astimezone(EST)
        if m_id == '4' and not marker4:
            marker4 = t.astimezone(EST)

    # Status flags for summary
    marker4_status = marker3_status = db2_status = "Skipped"

    # --- Marker4 alert ---
    if not marker4:
        send_email("AODGL DELAY ALERT", f"Marker4 missing for business_date={bd}")
        marker4_status = "Alert sent"
        write_log(f"Marker4 missing for bd={bd}")
        marker3_status = db2_status = "Waiting"
    else:
        marker4_status = "OK"
        write_log(f"Marker4 found at {marker4}")
        
        # --- Marker3 alert ---
        if not marker3:
            time_since_marker4 = now - marker4
            if time_since_marker4 >= timedelta(minutes=30):
                send_email("AODGL DELAY ALERT", f"Marker3 missing >30 min after Marker4 {marker4}, bd={bd}")
                marker3_status = "Alert sent"
                write_log(f"Marker3 missing >30min, bd={bd}")
            else:
                marker3_status = f"Waiting 30min ({int(time_since_marker4.total_seconds()/60)}min passed)"
                write_log(f"Marker3 waiting 30min window, bd={bd}")
            db2_status = "Waiting"
        else:
            marker3_status = "OK"
            write_log(f"Marker3 found at {marker3}")
            
            # --- DB2 alert ---
            time_since_marker3 = now - marker3
            if time_since_marker3 >= timedelta(minutes=15):
                db2_triggered = check_db2_files(bd)
                db2_status = "Alert sent" if db2_triggered else "OK"
            else:
                db2_status = f"Waiting 15min ({int(time_since_marker3.total_seconds()/60)}min passed)"
                write_log(f"DB2 waiting 15min window from marker3 at {marker3}")

    # --- Log summary ---
    write_log(f"Iteration summary - Marker4: {marker4_status}, Marker3: {marker3_status}, DB2: {db2_status}")

    cur.close()
    conn.close()

def wait_until_next_15min():
    now = datetime.now(EST)
    # Compute next 15-min slot
    next_slot_minute = ((now.minute // 15) + 1) * 15
    next_hour = now.hour
    if next_slot_minute >= 60:
        next_slot_minute -= 60
        next_hour = (next_hour + 1) % 24
    next_run = now.replace(hour=next_hour, minute=next_slot_minute, second=0, microsecond=0)
    sleep_sec = max((next_run - now).total_seconds(), 0)
    write_log(f"Sleeping {int(sleep_sec)} seconds until next 15-min iteration at {next_run.strftime('%H:%M')}")
    time.sleep(sleep_sec)

def main():
    write_log("=== Script started ===")
    while True:
        now = datetime.now(EST)
        if within_schedule(now):
            check_alerts()
            wait_until_next_15min()
        else:
            write_log("Outside schedule, sleeping until next iteration")
            # Sleep for 15 minutes but check if we're entering the schedule
            time.sleep(15 * 60)

if __name__=="__main__":
    main()
