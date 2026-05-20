import json
import os
import re
import time
from urllib.parse import unquote

from logger import get_logger
from event_bus import bus as event_bus

import pandas as pd
import requests


INPUT_LOG = "input.log"
THREAT_EXCEL = "Blockchain/leader/excel2.xlsx"
STATE_FILE = ".rle_state.json"
POLL_SECONDS = int(os.getenv("RLE_POLL_SECONDS", "5"))
CONTEXT_SECONDS = int(os.getenv("RLE_CONTEXT_SECONDS", "15"))
BLOCKCHAIN_API_URL = os.getenv("BLOCKCHAIN_API_URL", "http://localhost:8000/blockchain")

logger = get_logger("rle")

SOURCE_SUFFIX_PATTERNS = [
    re.compile(r"\s*\|\|SRC_IP=([^\s|]+)\s*$"),
    re.compile(r"\s*\|\|SRC=([^\s|]+)\s*$"),
]

ATTACK_TYPE_MAP = {
    "DoS": "network",
    "SQL Injection": "webattack",
    "Brute Force": "authentication",
    "Credential Harvesting": "data exfiltration",
    "Directory Traversal": "directory traversal",
    "Session Hijacking": "social enginnering",
    "Keylogging": "social enginnering",
    "Cookie Stealing": "data exfiltration",
}


def build_blockchain_row(alert: dict) -> dict:
    attack_name = alert.get("Attack")
    attack_count = alert.get("Attack Count")
    start_time = alert.get("Start Time")
    if hasattr(start_time, "isoformat"):
        start_value = start_time.isoformat()
    else:
        start_value = str(start_time) if start_time is not None else "-"

    return {
        "Attack": attack_name,
        "Type": ATTACK_TYPE_MAP.get(str(attack_name), "unknown"),
        "Severity": get_severity(attack_count or 0),
        "Source IP": alert.get("IP"),
        "Target IP": alert.get("Target IP"),
        "Start Time": start_value,
        "Attack Count": attack_count,
    }


def send_alerts_to_blockchain(alerts: list[dict]) -> int:
    if not alerts:
        return 0

    sent = 0
    for alert in alerts:
        row_payload = build_blockchain_row(alert)
        try:
            resp = requests.post(
                f"{BLOCKCHAIN_API_URL}/add_alert",
                json={"row": row_payload, "alert": row_payload.get("Attack") or "alert"},
                timeout=5,
            )
            if resp.status_code == 200:
                sent += 1
            else:
                logger.warning("Blockchain add_alert failed (%d): %s", resp.status_code, resp.text)
        except Exception as exc:
            logger.warning("Blockchain add_alert error: %s", exc)

    return sent


def fully_decode(url):
    prev = ""
    url = str(url)
    while prev != url:
        prev = url
        url = unquote(url)
    return url


def split_source_and_log(line):
    text = line.rstrip("\r\n")

    for pattern in SOURCE_SUFFIX_PATTERNS:
        source_match = pattern.search(text)
        if source_match:
            source_ip = source_match.group(1)
            raw_log = pattern.sub("", text)
            return source_ip, raw_log

    return "unknown", text


def parse_log_line(line):
    source_ip, raw_line = split_source_and_log(line)

    pattern = r"^\s*(\d+\.\d+\.\d+\.\d+).*?\[(.*?)\]\s+\"?(GET|POST)\s+(.*?)\s+HTTP.*?\"?\s+(\d{3})"
    match = re.search(pattern, raw_line)

    if not match:
        return None

    raw_time = match.group(2)
    raw_time = re.sub(r"\s[+-]\d{4}", "", raw_time)

    if " " in raw_time:
        parts = raw_time.split(" ")
        date_part = parts[0]
        time_part = parts[1] if len(parts) > 1 else ""
    else:
        date_part, time_part = raw_time.split(":", 1)

    return {
        "IP": match.group(1),
        "Date": date_part,
        "Time": time_part,
        "Method": match.group(3),
        "URL": match.group(4),
        "Status Code": match.group(5),
        "Target IP": source_ip,
    }


def detect_sqli(url):
    url = fully_decode(url).lower()

    patterns = [
        r"(\bor\b|\band\b)\s*['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?",
        r"['\"]\s*or\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+",
        r"union\s+select",
        r"sleep\s*\(",
        r"benchmark\s*\(",
        r"information_schema",
        r"--",
        r"or\s*1\s*=\s*1",
    ]

    return any(re.search(p, url) for p in patterns)


def detect_xss_advanced(url):
    raw = fully_decode(url).lower()
    attacks = []
    flag = True

    if flag:
        if re.search(r"(localstorage|sessionstorage|json\s*\.\s*stringify)", raw):
            attacks.append("Session Hijacking")
            flag = False
        elif re.search(r"document\s*\.\s*cookie", raw):
            attacks.append("Cookie Stealing")
            flag = False
        elif re.search(r"(onkey(down|press|up)|addEventListener\s*\(\s*['\"]key)", raw):
            attacks.append("Keylogging")
            flag = False
        elif re.search(r"(fetch\s*\()", raw):
            attacks.append("Data Exfiltration")
            flag = False
        elif re.search(r"type\s*=\s*['\"]?\s*password", raw):
            attacks.append("Credential Harvesting")
            flag = False

    if flag:
        if re.search(r"(window\s*\.\s*location|location\s*\.\s*href)", raw):
            attacks.append("XSS")
        elif re.search(r"<script[^>]*>\s*alert\s*\(", raw):
            attacks.append("XSS")
        elif "<script" in raw:
            attacks.append("XSS")

    return attacks if attacks else None


def detect_directory_traversal(url):
    url = fully_decode(url).lower()
    return bool(
        re.search(r"(\.\./|\.\.\\|%2e%2e%2f)", url)
        or re.search(r"(/etc/passwd|/etc/shadow|php://filter|proc/self)", url)
        or re.search(r"(file|page|include|path|template)\s*=\s*https?://", url)
    )


def detect_attack(url):
    attacks = []

    if detect_sqli(url):
        attacks.append("SQL Injection")

    xss = detect_xss_advanced(url)
    if xss:
        attacks.extend(xss)

    if detect_directory_traversal(url):
        attacks.append("Directory Traversal")

    return attacks if attacks else None


def read_new_log_lines(input_file, last_offset):
    if not os.path.exists(input_file):
        return [], last_offset

    file_size = os.path.getsize(input_file)
    if file_size < last_offset:
        # Log file was truncated / rotated.  The old data is already in
        # Excel, so skip to the current end instead of re-reading from 0.
        logger.info(
            "Log file shrank (%d -> %d bytes). Snapping offset to current size.",
            last_offset, file_size,
        )
        return [], file_size

    with open(input_file, "rb") as f:
        f.seek(last_offset)
        new_bytes = f.read()
        new_offset = f.tell()

    if not new_bytes:
        return [], new_offset

    new_text = new_bytes.decode("utf-8", errors="replace")
    lines = [line for line in new_text.splitlines() if line.strip()]
    return lines, new_offset


def parse_lines_to_df(lines):
    parsed_logs = []

    for line in lines:
        parsed = parse_log_line(line)
        if parsed:
            parsed_logs.append(parsed)

    return pd.DataFrame(parsed_logs)


def apply_attack_detection(df):
    if df.empty:
        return df

    result = df.copy()

    result["Timestamp"] = pd.to_datetime(
        result["Date"].astype(str) + " " + result["Time"].astype(str),
        format="%d/%b/%Y %H:%M:%S",
        errors="coerce",
    )

    result = result.sort_values(by=["IP", "Timestamp"], kind="stable")

    result["Attack"] = result["URL"].apply(detect_attack)
    result = result.explode("Attack", ignore_index=True)

    result["DoS_Flag"] = False

    for _, group in result.groupby(["IP", "URL"], dropna=False):
        ordered = group.dropna(subset=["Timestamp"]).sort_values("Timestamp")
        if ordered.empty:
            continue

        timestamps = ordered["Timestamp"].tolist()
        indices = ordered.index.tolist()

        for i, start in enumerate(timestamps):
            window_end = start + pd.Timedelta(seconds=2)
            j = i
            while j < len(timestamps) and timestamps[j] <= window_end:
                j += 1

            if (j - i) >= 30:
                result.loc[indices[i:j], "DoS_Flag"] = True

    result["BF_Flag"] = False

    login_df = result[
        (result["URL"].str.contains("login", case=False, na=False))
        & (result["Status Code"].astype(str) == "401")
    ]

    for _, group in login_df.groupby("IP", dropna=False):
        ordered = group.dropna(subset=["Timestamp"]).sort_values("Timestamp")
        if ordered.empty:
            continue

        timestamps = ordered["Timestamp"].tolist()
        indices = ordered.index.tolist()

        for i, start in enumerate(timestamps):
            window_end = start + pd.Timedelta(seconds=10)
            j = i
            while j < len(timestamps) and timestamps[j] <= window_end:
                j += 1

            if (j - i) >= 5:
                result.loc[indices[i:j], "BF_Flag"] = True

    result.loc[result["DoS_Flag"], "Attack"] = "DoS"
    result.loc[result["BF_Flag"], "Attack"] = "Brute Force"

    return result


def add_alert_if_new(alert, signature, seen_signatures, alerts):
    if signature in seen_signatures:
        return

    seen_signatures.add(signature)
    alerts.append(alert)


def format_sources(values):
    unique_sources = sorted({str(v) for v in values if pd.notna(v) and str(v).strip()})
    return ", ".join(unique_sources) if unique_sources else "unknown"


def add_type_and_threat_id(df, start_id=1):
    if df.empty:
        return df

    result = df.copy()
    result.insert(0, "Threat ID", range(start_id, start_id + len(result)))
    result["Type"] = result["Attack"].map(ATTACK_TYPE_MAP).fillna("unknown")

    ordered_columns = ["Threat ID", "IP", "Attack", "Type"]
    ordered_columns.extend([column for column in result.columns if column not in ordered_columns])
    return result[ordered_columns]


def normalize_existing_alerts(df):
    if df.empty:
        return df

    result = df.copy()

    if "IP" not in result.columns and "Source IP" in result.columns:
        result = result.rename(columns={"Source IP": "IP"})

    if "Threat ID" not in result.columns:
        result.insert(0, "Threat ID", range(1, len(result) + 1))
    else:
        result["Threat ID"] = pd.to_numeric(result["Threat ID"], errors="coerce")
        missing_ids = result["Threat ID"].isna()
        if missing_ids.any():
            current_max = result["Threat ID"].dropna().max()
            current_max = int(current_max) if pd.notna(current_max) else 0
            fill_ids = range(current_max + 1, current_max + 1 + missing_ids.sum())
            result.loc[missing_ids, "Threat ID"] = list(fill_ids)
        result["Threat ID"] = result["Threat ID"].astype(int)

    if "Type" not in result.columns:
        result["Type"] = result["Attack"].map(ATTACK_TYPE_MAP).fillna("unknown")
    else:
        missing_type = result["Type"].isna() | (result["Type"].astype(str).str.strip() == "")
        if missing_type.any():
            result.loc[missing_type, "Type"] = result.loc[missing_type, "Attack"].map(ATTACK_TYPE_MAP).fillna("unknown")

    ordered_columns = ["Threat ID", "IP", "Attack", "Type"]
    ordered_columns.extend([column for column in result.columns if column not in ordered_columns])
    return result[ordered_columns]


def build_alerts(df, seen_signatures):
    alerts = []

    if df.empty:
        return alerts

    dos_df = df[df["DoS_Flag"]]

    for (ip, url), group in dos_df.groupby(["IP", "URL"], dropna=False):
        ordered = group.dropna(subset=["Timestamp"]).sort_values("Timestamp")
        if ordered.empty or not ordered["Is_New"].any():
            continue

        rows = ordered[["Timestamp", "Is_New", "Target IP"]].to_dict("records")
        i = 0

        while i < len(rows):
            start = rows[i]["Timestamp"]
            window_end = start + pd.Timedelta(seconds=2)
            j = i

            while j < len(rows) and rows[j]["Timestamp"] <= window_end:
                j += 1

            if (j - i) >= 30 and any(r["Is_New"] for r in rows[i:j]):
                sources = format_sources(r["Target IP"] for r in rows[i:j])
                alert = {
                    "IP": ip,
                    "Attack": "DoS",
                    "Target IP": sources,
                        "Start Time": rows[i]["Timestamp"],
                    "Attack Count": j - i,
                }
                signature = (
                    "DoS",
                    str(ip),
                    str(url),
                        str(alert["Start Time"]),
                    sources,
                    alert["Attack Count"],
                )
                add_alert_if_new(alert, signature, seen_signatures, alerts)

            i = j

    bf_df = df[df["BF_Flag"]]

    for ip, group in bf_df.groupby("IP", dropna=False):
        ordered = group.dropna(subset=["Timestamp"]).sort_values("Timestamp")
        if ordered.empty or not ordered["Is_New"].any():
            continue

        rows = ordered[["Timestamp", "Is_New", "Target IP"]].to_dict("records")
        i = 0

        while i < len(rows):
            start = rows[i]["Timestamp"]
            window_end = start + pd.Timedelta(seconds=10)
            j = i

            while j < len(rows) and rows[j]["Timestamp"] <= window_end:
                j += 1

            if (j - i) >= 5 and any(r["Is_New"] for r in rows[i:j]):
                sources = format_sources(r["Target IP"] for r in rows[i:j])
                alert = {
                    "IP": ip,
                    "Attack": "Brute Force",
                    "Target IP": sources,
                        "Start Time": rows[i]["Timestamp"],
                    "Attack Count": j - i,
                }
                signature = (
                    "Brute Force",
                    str(ip),
                        str(alert["Start Time"]),
                    sources,
                    alert["Attack Count"],
                )
                add_alert_if_new(alert, signature, seen_signatures, alerts)

            i = j

    other_df = df[(~df["DoS_Flag"]) & (~df["BF_Flag"]) & df["Attack"].notna() & df["Is_New"]]

    for (ip, attack), group in other_df.groupby(["IP", "Attack"], dropna=False):
        ordered = group.dropna(subset=["Timestamp"]).sort_values("Timestamp")
        if ordered.empty:
            continue

        sources = format_sources(ordered["Target IP"].tolist())
        alert = {
            "IP": ip,
            "Attack": attack,
            "Target IP": sources,
            "Start Time": ordered["Timestamp"].iloc[0],
            "Attack Count": len(ordered),
        }
        signature = (
            str(attack),
            str(ip),
            str(alert["Start Time"]),
            sources,
            alert["Attack Count"],
        )
        add_alert_if_new(alert, signature, seen_signatures, alerts)

    return alerts


def get_severity(count):
    """Derive severity from Attack Count.
    
    count <= 1 → low
    count < 10 → medium
    count < 100 → high
    otherwise → critical
    """
    if count <= 1:
        return "low"
    elif count < 10:
        return "medium"
    elif count < 100:
        return "high"
    else:
        return "critical"


def append_alerts_to_excel(alerts, output_excel):
    if not alerts:
        return 0

    new_alerts_df = pd.DataFrame(alerts)

    if os.path.exists(output_excel):
        existing_df = normalize_existing_alerts(pd.read_excel(output_excel))
        next_threat_id = 1

        if "Threat ID" in existing_df.columns:
            threat_ids = pd.to_numeric(existing_df["Threat ID"], errors="coerce").dropna()
            if not threat_ids.empty:
                next_threat_id = int(threat_ids.max()) + 1

        result_df = pd.concat([existing_df, add_type_and_threat_id(new_alerts_df, next_threat_id)], ignore_index=True)
    else:
        result_df = add_type_and_threat_id(new_alerts_df)

    # Add Severity column derived from Attack Count
    result_df["Severity"] = result_df["Attack Count"].apply(get_severity)

    # Rename IP column to Source IP
    result_df = result_df.rename(columns={"IP": "Source IP"})

    result_df.to_excel(output_excel, index=False)
    return len(new_alerts_df)



def load_state(state_file):
    if not os.path.exists(state_file):
        return {"offset": 0}

    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"offset": int(data.get("offset", 0))}
    except Exception:
        return {"offset": 0}


def save_state(state_file, offset):
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump({"offset": int(offset)}, f)
    try:
        import supabase_storage
        import threading
        threading.Thread(target=supabase_storage.upload_file, args=(state_file, state_file)).start()
    except Exception:
        pass


def keep_recent_context(df):
    if df.empty:
        return df

    valid_times = df["Timestamp"].dropna()
    if valid_times.empty:
        return df.iloc[0:0].copy()

    cutoff = valid_times.max() - pd.Timedelta(seconds=CONTEXT_SECONDS)
    return df[df["Timestamp"].notna() & (df["Timestamp"] >= cutoff)].copy()




def run_stream_monitor():
    logger.info("RLE streaming monitor started...")

    state = load_state(STATE_FILE)
    last_offset = state["offset"]

    # Fix stale offset: if the saved offset exceeds the current log size,
    # snap it to the file size so we don't re-process old data.
    if os.path.exists(INPUT_LOG):
        current_size = os.path.getsize(INPUT_LOG)
        if last_offset > current_size:
            logger.info(
                "Saved offset (%d) exceeds log size (%d). "
                "Snapping to current size.",
                last_offset, current_size,
            )
            last_offset = current_size
            save_state(STATE_FILE, last_offset)

    recent_df = pd.DataFrame()
    seen_signatures = set()

    while True:
        try:
            lines, new_offset = read_new_log_lines(INPUT_LOG, last_offset)

            if not lines:
                # Always persist the offset — even when no lines are returned
                # (e.g. after a file-shrink correction).
                if new_offset != last_offset:
                    last_offset = new_offset
                    save_state(STATE_FILE, last_offset)
                time.sleep(POLL_SECONDS)
                continue

            new_df = parse_lines_to_df(lines)
            last_offset = new_offset
            save_state(STATE_FILE, last_offset)

            if new_df.empty:
                logger.debug("New lines arrived but none matched the parser.")
                time.sleep(POLL_SECONDS)
                continue

            if not recent_df.empty:
                recent_df = recent_df.copy()
                recent_df["Is_New"] = False

            new_df = new_df.copy()
            new_df["Is_New"] = True

            working_df = pd.concat([recent_df, new_df], ignore_index=True)
            working_df = apply_attack_detection(working_df)

            alerts = build_alerts(working_df, seen_signatures)
            sent = send_alerts_to_blockchain(alerts)
            count = append_alerts_to_excel(alerts, THREAT_EXCEL)

            if count > 0:
                logger.info("Added %d alerts to %s", count, THREAT_EXCEL)
                if sent > 0:
                    logger.info("Sent %d alerts to blockchain", sent)
                event_bus.emit_threadsafe("alerts.changed", {"trigger": "rle", "count": count})
                
                try:
                    import supabase_storage
                    import threading
                    threading.Thread(target=supabase_storage.upload_file, args=(THREAT_EXCEL, THREAT_EXCEL)).start()
                except Exception as e:
                    logger.error(f"S3 Upload failed: {e}")
            else:
                logger.debug("Processed new logs, no new threats detected.")

            recent_df = keep_recent_context(working_df)
            save_state(STATE_FILE, last_offset)

        except KeyboardInterrupt:
            logger.info("RLE stopped by user.")
            break
        except Exception as e:
            logger.error("Monitoring error: %s", e)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run_stream_monitor()
