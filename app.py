import uuid
import zipfile
import json
from io import BytesIO
from datetime import datetime, time, timedelta
from pathlib import Path

import pandas as pd
from flask import Flask, render_template, request, send_file, flash, redirect, url_for, jsonify


_MINIMAL_STYLES_XML = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
<borders count="1"><border/></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>"""


def _sanitize_xlsx(file_bytes: BytesIO) -> BytesIO:
    out = BytesIO()
    with zipfile.ZipFile(file_bytes, "r") as zin:
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == "xl/styles.xml":
                    data = _MINIMAL_STYLES_XML
                zout.writestr(item, data)
    out.seek(0)
    return out


app = Flask(__name__)
app.secret_key = "change-this-secret-key"

EVENT_TO_KEEP = "Access Granted by Face"
CLUSTER_MINUTES = 30

SITES_FILE = Path(__file__).parent / "sites.json"


def load_sites():
    if SITES_FILE.exists():
        with open(SITES_FILE, "r") as f:
            return json.load(f)
    return []


def save_sites(sites):
    with open(SITES_FILE, "w") as f:
        json.dump(sites, f, indent=2)


def get_sites():
    return sorted(load_sites())


def safe_str(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def find_best_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def parse_user_date(value: str):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def format_dt_series(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")


def format_ts(ts) -> str:
    if pd.isna(ts):
        return ""
    return pd.to_datetime(ts).strftime("%H:%M")


def build_clusters(group: pd.DataFrame, gap_minutes: int = 30) -> pd.DataFrame:
    """
    Merge multiple authentications within 30 minutes into one event cluster.
    """
    group = group.sort_values("Time").copy()
    group["prev_time"] = group["Time"].shift(1)
    group["gap_min"] = (group["Time"] - group["prev_time"]).dt.total_seconds() / 60
    group["new_cluster"] = group["gap_min"].isna() | (group["gap_min"] > gap_minutes)
    group["cluster_id"] = group["new_cluster"].cumsum()

    clusters = (
        group.groupby("cluster_id", as_index=False)
        .agg(
            Employee_ID=("Employee ID", "first"),
            Date=("Date", "first"),
            Name=("Name", lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0]),
            Cluster_Start=("Time", "min"),
            Cluster_End=("Time", "max"),
            Auth_Count=("Time", "size"),
        )
    )

    return clusters


def round_checkin_time(dt_value: pd.Timestamp) -> pd.Timestamp:
    """
    Rounding rule:
    - 06:30 to 07:34 => 07:00
    - 07:35 to 08:35 => 08:00
    - 08:36 to 09:35 => 09:00
    and so on
    """
    if pd.isna(dt_value):
        return pd.NaT

    rounded = dt_value.replace(second=0, microsecond=0)
    hour = rounded.hour
    minute = rounded.minute

    if hour == 6 and minute >= 30:
        return rounded.replace(hour=7, minute=0)

    if minute <= 35:
        return rounded.replace(minute=0)

    return rounded.replace(minute=0) + timedelta(hours=1)


def overlap_hours(start_dt: pd.Timestamp, end_dt: pd.Timestamp, break_start: time, break_end: time) -> float:
    if pd.isna(start_dt) or pd.isna(end_dt) or end_dt <= start_dt:
        return 0.0

    day = start_dt.date()
    break_start_dt = pd.Timestamp(datetime.combine(day, break_start))
    break_end_dt = pd.Timestamp(datetime.combine(day, break_end))

    overlap_start = max(start_dt, break_start_dt)
    overlap_end = min(end_dt, break_end_dt)

    if overlap_end <= overlap_start:
        return 0.0

    return (overlap_end - overlap_start).total_seconds() / 3600.0


def calculate_total_hours_and_used_events(group: pd.DataFrame):
    """
    Treat clustered events as alternating:
    1st = IN
    2nd = OUT
    3rd = IN
    4th = OUT
    ...

    Returns:
    - total hours
    - text showing which pairs were used
    """
    ordered_events = list(group["Cluster_Start"].sort_values())

    if len(ordered_events) < 2:
        if len(ordered_events) == 1:
            return None, f"Only one entry: {format_ts(ordered_events[0])}"
        return None, ""

    pair_count = len(ordered_events) // 2
    if pair_count == 0:
        return None, ""

    total_hours = 0.0
    valid_pair_found = False
    used_pairs = []

    for i in range(0, pair_count * 2, 2):
        raw_in = ordered_events[i]
        raw_out = ordered_events[i + 1]

        rounded_in = round_checkin_time(raw_in)

        if pd.isna(rounded_in) or pd.isna(raw_out):
            continue

        if raw_out <= rounded_in:
            continue

        pair_hours = (raw_out - rounded_in).total_seconds() / 3600.0

        lunch_deduction = overlap_hours(rounded_in, raw_out, time(12, 0), time(13, 0))
        evening_deduction = overlap_hours(rounded_in, raw_out, time(17, 0), time(18, 0))

        pair_hours -= lunch_deduction
        pair_hours -= evening_deduction

        if pair_hours < 0:
            pair_hours = 0.0

        total_hours += pair_hours
        valid_pair_found = True

        deductions = []
        if lunch_deduction > 0:
            deductions.append("12-1")
        if evening_deduction > 0:
            deductions.append("5-6")

        pair_text = f"{format_ts(rounded_in)} -> {format_ts(raw_out)}"
        if deductions:
            pair_text += f" (-{' & '.join(deductions)})"

        used_pairs.append(pair_text)

    if not valid_pair_found:
        return None, ""

    return round(total_hours, 2), " | ".join(used_pairs)


def summarize_day(group: pd.DataFrame) -> pd.Series:
    group = group.sort_values("Cluster_Start").copy()

    first_event = group["Cluster_Start"].min()
    last_event = group["Cluster_Start"].max()

    rounded_first_register = round_checkin_time(first_event)
    total_hours, used_events = calculate_total_hours_and_used_events(group)

    return pd.Series(
        {
            "Name": group["Name"].mode().iloc[0] if not group["Name"].mode().empty else group["Name"].iloc[0],
            "Date": group["Date"].iloc[0],
            "First Register": rounded_first_register,
            "Last Register": last_event,
            "Used Events": used_events,
            "Total Hours": "" if total_hours is None else total_hours,
        }
    )


def process_attendance_excel(file_stream, start_date=None, end_date=None):
    file_bytes = BytesIO(file_stream.read())
    clean_bytes = _sanitize_xlsx(file_bytes)
    raw_df = pd.read_excel(clean_bytes, engine="openpyxl")
    raw_df = normalize_column_names(raw_df)

    employee_id_col = find_best_column(raw_df, ["Employee ID", "EmployeeID", "Emp ID", "ID"])
    name_col = find_best_column(raw_df, ["Name", "Employee Name", "Person Name"])
    event_col = find_best_column(raw_df, ["Event Sub Type", "Event Type", "Event"])
    time_col = find_best_column(raw_df, ["Time", "Event Time", "Date Time", "DateTime"])

    required_missing = []
    if not employee_id_col:
        required_missing.append("Employee ID")
    if not name_col:
        required_missing.append("Name")
    if not event_col:
        required_missing.append("Event Sub Type")
    if not time_col:
        required_missing.append("Time")

    if required_missing:
        raise ValueError(f"Missing required columns: {', '.join(required_missing)}")

    df = raw_df.rename(
        columns={
            employee_id_col: "Employee ID",
            name_col: "Name",
            event_col: "Event Sub Type",
            time_col: "Time",
        }
    ).copy()

    df["Event Sub Type"] = df["Event Sub Type"].astype(str).str.strip()
    df = df[df["Event Sub Type"].eq(EVENT_TO_KEEP)].copy()

    df["Employee ID"] = df["Employee ID"].apply(safe_str)
    df["Name"] = df["Name"].apply(safe_str)

    raw_time = df["Time"].copy()
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")

    nat_mask = df["Time"].isna()
    if nat_mask.any():
        numeric = pd.to_numeric(raw_time[nat_mask], errors="coerce")
        excel_dates = pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")
        df.loc[nat_mask, "Time"] = excel_dates

    df = df[(df["Employee ID"] != "") & df["Time"].notna()].copy()

    if df.empty:
        raise ValueError("No valid rows found after filtering.")

    df["Date"] = df["Time"].dt.date

    if start_date:
        df = df[df["Date"] >= start_date].copy()
    if end_date:
        df = df[df["Date"] <= end_date].copy()

    if df.empty:
        raise ValueError("No rows found in the selected date range.")

    df = df.sort_values(["Employee ID", "Date", "Time"]).reset_index(drop=True)

    clustered = (
        df.groupby(["Employee ID", "Date"], group_keys=False)
        .apply(build_clusters, gap_minutes=CLUSTER_MINUTES)
        .reset_index(drop=True)
    )

    daily_summary = (
        clustered.groupby(["Employee_ID", "Date"], group_keys=False)
        .apply(summarize_day)
        .reset_index(drop=True)
    )

    daily_export = daily_summary.copy()
    daily_export["Date"] = pd.to_datetime(daily_export["Date"]).dt.strftime("%Y-%m-%d")

    for col in ["First Register", "Last Register"]:
        daily_export[col] = format_dt_series(daily_export[col])

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        daily_export.to_excel(writer, sheet_name="Daily Summary", index=False)

    output.seek(0)
    return output


@app.route("/api/sites", methods=["GET"])
def api_get_sites():
    return jsonify(get_sites())


@app.route("/api/sites", methods=["POST"])
def api_add_site():
    data = request.get_json()
    site_name = data.get("name", "").strip()

    if not site_name:
        return jsonify({"error": "Site name cannot be empty"}), 400

    sites = load_sites()
    if site_name in sites:
        return jsonify({"error": "Site already exists"}), 400

    sites.append(site_name)
    save_sites(sites)

    return jsonify({"success": True, "sites": sorted(sites)})


@app.route("/api/sites/<site_name>", methods=["DELETE"])
def api_delete_site(site_name):
    sites = load_sites()
    if site_name not in sites:
        return jsonify({"error": "Site not found"}), 404

    sites.remove(site_name)
    save_sites(sites)

    return jsonify({"success": True, "sites": sorted(sites)})


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        site = request.form.get("site", "").strip()
        if not site:
            flash("Please select a site.")
            return redirect(url_for("index"))

        if "file" not in request.files:
            flash("Please choose an Excel file.")
            return redirect(url_for("index"))

        file = request.files["file"]
        if not file or file.filename == "":
            flash("Please choose an Excel file.")
            return redirect(url_for("index"))

        if not file.filename.lower().endswith((".xlsx", ".xls")):
            flash("Please upload an Excel file (.xlsx or .xls).")
            return redirect(url_for("index"))

        try:
            start_date = parse_user_date(request.form.get("start_date", "").strip())
            end_date = parse_user_date(request.form.get("end_date", "").strip())

            if start_date and end_date and start_date > end_date:
                flash("Start date cannot be after end date.")
                return redirect(url_for("index"))

            result_file = process_attendance_excel(
                file_stream=file,
                start_date=start_date,
                end_date=end_date,
            )

            download_name = f"employee_daily_summary_{uuid.uuid4().hex[:8]}.xlsx"

            return send_file(
                result_file,
                as_attachment=True,
                download_name=download_name,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            flash(f"Error: {str(e)}")
            return redirect(url_for("index"))

    return render_template("index.html", sites=get_sites())


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)