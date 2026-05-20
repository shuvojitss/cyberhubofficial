"""
excel_to_pdf.py
================
Converts a Security Alerts Excel file into a stunning branded PDF report.

Usage:
    python excel_to_pdf.py                        # uses built-in sample data
    python excel_to_pdf.py my_alerts.xlsx         # uses your own Excel file

Dependencies:
    pip install reportlab openpyxl pandas
"""

import os
import sys
from datetime import datetime

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether
)
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.graphics import renderPDF

# ─────────────────────────────────────────────
# 0.  CONFIG  – tweak these to match your brand
# ─────────────────────────────────────────────
COMPANY_NAME   = "CyberShield Security Operations"
REPORT_TITLE   = "Security Alert Report"
REPORT_SUBTITLE = "Incident Monitoring & Threat Analysis"
DEPARTMENT     = "Security Operations Center (SOC)"
CLASSIFICATION = "CONFIDENTIAL – INTERNAL USE ONLY"
LOGO_TEXT      = "CS"          # shown as a logo placeholder; replace with an image path if needed
GENERATED_BY   = "SOC Analyst Automation System"

# Brand colours
COL_PRIMARY   = colors.HexColor("#0D1B2A")   # dark navy
COL_SECONDARY = colors.HexColor("#1B4F72")   # deep blue
COL_ACCENT    = colors.HexColor("#E74C3C")   # alert red
COL_HEADER_BG = colors.HexColor("#1B4F72")
COL_ROW_EVEN  = colors.HexColor("#EAF2FB")
COL_ROW_ODD   = colors.white
COL_GOLD      = colors.HexColor("#F0B429")

SEVERITY_COLORS = {
    "CRITICAL": colors.HexColor("#C0392B"),
    "HIGH":     colors.HexColor("#E67E22"),
    "MEDIUM":   colors.HexColor("#F1C40F"),
    "LOW":      colors.HexColor("#27AE60"),
    "INFO":     colors.HexColor("#2980B9"),
}

# ─────────────────────────────────────────────
# 1.  SAMPLE DATA  (used when no xlsx supplied)
# ─────────────────────────────────────────────
SAMPLE_DATA = [
    ["Alert ID", "Timestamp",           "Severity", "Category",           "Source IP",      "Destination IP", "Description",                                   "Status",    "Assigned To"],
    ["ALT-001",  "2025-04-20 08:12:34", "CRITICAL", "Brute Force",        "192.168.10.45",  "10.0.0.5",       "Multiple failed SSH login attempts detected",    "Open",      "Alice Chen"],
    ["ALT-002",  "2025-04-20 09:03:11", "HIGH",     "Malware",            "10.0.1.22",      "8.8.8.8",        "Suspicious outbound DNS tunneling activity",     "In Review", "Bob Smith"],
    ["ALT-003",  "2025-04-20 09:47:55", "HIGH",     "Privilege Escalation","172.16.0.33",   "172.16.0.1",     "Unauthorized sudo command executed on server",   "Open",      "Alice Chen"],
    ["ALT-004",  "2025-04-20 10:21:08", "MEDIUM",   "Phishing",           "203.0.113.50",   "10.0.2.15",      "Phishing URL clicked by employee workstation",   "Resolved",  "Carol Wang"],
    ["ALT-005",  "2025-04-20 11:05:44", "CRITICAL", "Data Exfiltration",  "10.0.3.77",      "198.51.100.9",   "Large data transfer to unknown external server", "Open",      "Bob Smith"],
    ["ALT-006",  "2025-04-20 11:33:22", "LOW",      "Port Scan",          "192.168.50.12",  "10.0.0.0/24",    "Internal network port scan detected",            "Closed",    "Dave Patel"],
    ["ALT-007",  "2025-04-20 12:00:01", "HIGH",     "Ransomware",         "10.0.4.88",      "10.0.4.88",      "File encryption activity detected on endpoint",  "In Review", "Alice Chen"],
    ["ALT-008",  "2025-04-20 13:14:39", "MEDIUM",   "Insider Threat",     "10.0.1.101",     "10.0.5.200",     "Access to sensitive HR files outside hours",     "Open",      "Carol Wang"],
    ["ALT-009",  "2025-04-20 14:02:17", "INFO",     "Policy Violation",   "10.0.2.55",      "0.0.0.0",        "USB device connected to secured workstation",    "Closed",    "Dave Patel"],
    ["ALT-010",  "2025-04-20 15:48:30", "CRITICAL", "Zero-Day Exploit",   "203.0.113.200",  "10.0.0.1",       "Exploit attempt against CVE-2025-12345 detected","Open",      "Bob Smith"],
    ["ALT-011",  "2025-04-21 07:22:10", "HIGH",     "DDoS",               "Multiple",       "10.0.0.1",       "Distributed denial-of-service attack detected",  "In Review", "Alice Chen"],
    ["ALT-012",  "2025-04-21 08:55:44", "MEDIUM",   "Misconfiguration",   "10.0.6.30",      "Internet",       "AWS S3 bucket set to public accidentally",       "Resolved",  "Dave Patel"],
    ["ALT-013",  "2025-04-21 09:30:00", "LOW",      "Reconnaissance",     "198.51.100.77",  "10.0.0.0/16",    "External host probing internal DNS records",     "Closed",    "Carol Wang"],
    ["ALT-014",  "2025-04-21 10:10:59", "CRITICAL", "APT Activity",       "45.33.32.156",   "10.0.1.50",      "Nation-state threat actor TTPs observed",        "Open",      "Bob Smith"],
    ["ALT-015",  "2025-04-21 11:00:00", "HIGH",     "Credential Theft",   "10.0.7.19",      "10.0.0.5",       "Pass-the-hash attack detected in domain",        "In Review", "Alice Chen"],
]


# ─────────────────────────────────────────────
# 2.  LOAD DATA
# ─────────────────────────────────────────────
def load_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = df.fillna("")
    return df


def load_sample() -> pd.DataFrame:
    rows = SAMPLE_DATA
    df = pd.DataFrame(rows[1:], columns=rows[0])
    return df


def generate_report(excel_path: str, output_dir: str, output_name: str | None = None) -> dict:
    if not os.path.exists(excel_path):
        raise FileNotFoundError(excel_path)

    os.makedirs(output_dir, exist_ok=True)

    if not output_name:
        output_name = f"security_alert_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"

    output_path = os.path.join(output_dir, output_name)
    df = load_excel(excel_path)
    build_pdf(df, output_path)

    try:
        import sys
        # ensure supabase_storage can be imported if report.py is run directly
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        import supabase_storage
        import threading
        # We upload it using a relative path that includes reports/
        threading.Thread(target=supabase_storage.upload_file, args=(output_path, f"reports/{output_name}")).start()
    except Exception as e:
        print(f"Failed to upload report to Supabase: {e}")

    return {
        "output_path": output_path,
        "row_count": len(df),
    }


# ─────────────────────────────────────────────
# 3.  STYLES
# ─────────────────────────────────────────────
def build_styles():
    base = getSampleStyleSheet()
    styles = {}

    styles["company"] = ParagraphStyle(
        "company", fontSize=22, leading=26,
        textColor=COL_PRIMARY, fontName="Helvetica-Bold",
        alignment=TA_LEFT, spaceAfter=2,
    )
    styles["report_title"] = ParagraphStyle(
        "report_title", fontSize=15, leading=20,
        textColor=COL_SECONDARY, fontName="Helvetica-Bold",
        alignment=TA_LEFT, spaceAfter=2,
    )
    styles["subtitle"] = ParagraphStyle(
        "subtitle", fontSize=10, leading=14,
        textColor=colors.HexColor("#555555"), fontName="Helvetica",
        alignment=TA_LEFT, spaceAfter=2,
    )
    styles["classification"] = ParagraphStyle(
        "classification", fontSize=8, leading=10,
        textColor=COL_ACCENT, fontName="Helvetica-Bold",
        alignment=TA_RIGHT,
    )
    styles["section_heading"] = ParagraphStyle(
        "section_heading", fontSize=12, leading=16,
        textColor=COL_SECONDARY, fontName="Helvetica-Bold",
        spaceBefore=14, spaceAfter=6,
    )
    styles["meta"] = ParagraphStyle(
        "meta", fontSize=9, leading=13,
        textColor=colors.HexColor("#444444"), fontName="Helvetica",
        spaceAfter=2,
    )
    styles["footer"] = ParagraphStyle(
        "footer", fontSize=7, leading=10,
        textColor=colors.HexColor("#888888"), fontName="Helvetica",
        alignment=TA_CENTER,
    )
    styles["summary_value"] = ParagraphStyle(
        "summary_value", fontSize=18, leading=22,
        textColor=COL_PRIMARY, fontName="Helvetica-Bold",
        alignment=TA_CENTER,
    )
    styles["summary_label"] = ParagraphStyle(
        "summary_label", fontSize=8, leading=11,
        textColor=colors.HexColor("#666666"), fontName="Helvetica",
        alignment=TA_CENTER,
    )
    return styles


# ─────────────────────────────────────────────
# 4.  HEADER / FOOTER CANVAS
# ─────────────────────────────────────────────
class ReportCanvas:
    def __init__(self, doc, styles):
        self.doc    = doc
        self.styles = styles

    def on_page(self, canvas, doc):
        canvas.saveState()
        w, h = A4

        # ── Top banner ──
        canvas.setFillColor(COL_PRIMARY)
        canvas.rect(0, h - 55, w, 55, fill=True, stroke=False)

        # Logo circle
        canvas.setFillColor(COL_ACCENT)
        canvas.circle(35, h - 27, 18, fill=True, stroke=False)
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 11)
        canvas.drawCentredString(35, h - 31, LOGO_TEXT)

        # Company name in banner
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 13)
        canvas.drawString(62, h - 24, COMPANY_NAME)
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(COL_GOLD)
        canvas.drawString(62, h - 37, REPORT_SUBTITLE)

        # Classification tag (top right)
        canvas.setFillColor(COL_ACCENT)
        canvas.roundRect(w - 165, h - 43, 155, 22, 4, fill=True, stroke=False)
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 7)
        canvas.drawCentredString(w - 87, h - 35, CLASSIFICATION)

        # ── Bottom footer ──
        canvas.setFillColor(COL_PRIMARY)
        canvas.rect(0, 0, w, 30, fill=True, stroke=False)
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica", 7)
        canvas.drawString(20, 11,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  By: {GENERATED_BY}")
        canvas.drawRightString(w - 20, 11,
            f"Page {doc.page}")

        canvas.setFillColor(COL_GOLD)
        canvas.setFont("Helvetica-Bold", 7)
        canvas.drawCentredString(w / 2, 11, f"{DEPARTMENT}")

        canvas.restoreState()


# ─────────────────────────────────────────────
# 5.  SUMMARY CARDS
# ─────────────────────────────────────────────
def build_summary(df: pd.DataFrame, styles) -> list:
    sev_col = next((c for c in df.columns if "sev" in c.lower()), None)
    status_col = next((c for c in df.columns if "status" in c.lower()), None)

    total     = len(df)
    critical  = len(df[df[sev_col].str.upper() == "CRITICAL"]) if sev_col else "—"
    high      = len(df[df[sev_col].str.upper() == "HIGH"])     if sev_col else "—"
    open_cnt  = len(df[df[status_col].str.upper() == "OPEN"])  if status_col else "—"

    def card(value, label, bg):
        return Table(
            [[Paragraph(str(value), styles["summary_value"])],
             [Paragraph(label,      styles["summary_label"])]],
            colWidths=[3.8 * cm], rowHeights=[1.1 * cm, 0.7 * cm],
            style=TableStyle([
                ("BACKGROUND",  (0, 0), (-1, -1), bg),
                ("ROUNDEDCORNERS", [6]),
                ("TOPPADDING",  (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING",  (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ])
        )

    cards = Table(
        [[
            card(total,    "Total Alerts",     colors.HexColor("#EAF2FB")),
            card(critical, "Critical",         colors.HexColor("#FDEDEC")),
            card(high,     "High",             colors.HexColor("#FEF9E7")),
            card(open_cnt, "Open",             colors.HexColor("#EAFAF1")),
        ]],
        colWidths=[3.8 * cm] * 4,
        hAlign="LEFT",
        style=TableStyle([
            ("LEFTPADDING",  (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ])
    )
    return [
        Paragraph("Executive Summary", styles["section_heading"]),
        cards,
        Spacer(1, 10),
    ]


# ─────────────────────────────────────────────
# 6.  MAIN DATA TABLE
# ─────────────────────────────────────────────
def build_data_table(df: pd.DataFrame, styles) -> list:
    sev_col = next((c for c in df.columns if "sev" in c.lower()), None)

    # Build column widths proportionally (landscape A4 minus margins)
    usable = 25.7 * cm   # A4 width minus 2 x 1.5 cm margin
    col_map = {
        "Alert ID":       1.8, "Timestamp": 3.2, "Severity": 1.8,
        "Category":       2.5, "Source IP": 2.8, "Destination IP": 2.8,
        "Description":    6.5, "Status":    1.9, "Assigned To": 2.4,
    }
    widths = [col_map.get(c, 3.0) * cm for c in df.columns]
    total_w = sum(widths)
    if total_w > usable:
        scale = usable / total_w
        widths = [w * scale for w in widths]

    # Header row
    header = [Paragraph(f"<b>{c}</b>", ParagraphStyle(
        "th", fontSize=8, textColor=colors.white,
        fontName="Helvetica-Bold", alignment=TA_CENTER, leading=10,
    )) for c in df.columns]

    data = [header]

    # Data rows
    for _, row in df.iterrows():
        sev = str(row.get(sev_col, "")).upper() if sev_col else ""
        cells = []
        for col in df.columns:
            val  = str(row[col])
            align = TA_CENTER if col in ("Alert ID", "Severity", "Status", "Assigned To", "Timestamp") else TA_LEFT
            if col == sev_col:
                sc = SEVERITY_COLORS.get(sev, colors.black)
                p = Paragraph(f"<b>{val}</b>", ParagraphStyle(
                    "sev", fontSize=7.5, textColor=sc,
                    fontName="Helvetica-Bold", alignment=TA_CENTER, leading=10,
                ))
            else:
                p = Paragraph(val, ParagraphStyle(
                    "cell", fontSize=7.5, textColor=COL_PRIMARY,
                    fontName="Helvetica", alignment=align, leading=10,
                ))
            cells.append(p)
        data.append(cells)

    tbl = Table(data, colWidths=widths, repeatRows=1)

    # Alternating row styles
    style_cmds = [
        ("BACKGROUND",   (0, 0), (-1, 0),  COL_HEADER_BG),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [COL_ROW_ODD, COL_ROW_EVEN]),
        ("GRID",         (0, 0), (-1, -1),  0.3, colors.HexColor("#CCCCCC")),
        ("LINEBELOW",    (0, 0), (-1, 0),   1.2, COL_ACCENT),
        ("TOPPADDING",   (0, 0), (-1, -1),  4),
        ("BOTTOMPADDING",(0, 0), (-1, -1),  4),
        ("LEFTPADDING",  (0, 0), (-1, -1),  5),
        ("RIGHTPADDING", (0, 0), (-1, -1),  5),
        ("VALIGN",       (0, 0), (-1, -1),  "MIDDLE"),
    ]
    tbl.setStyle(TableStyle(style_cmds))

    return [
        Paragraph("Security Alert Details", styles["section_heading"]),
        tbl,
        Spacer(1, 6),
    ]


# ─────────────────────────────────────────────
# 7.  SEVERITY BREAKDOWN TABLE
# ─────────────────────────────────────────────
def build_breakdown(df: pd.DataFrame, styles) -> list:
    sev_col    = next((c for c in df.columns if "sev" in c.lower()), None)
    status_col = next((c for c in df.columns if "status" in c.lower()), None)
    if not sev_col:
        return []

    sev_counts = df[sev_col].str.upper().value_counts()
    order = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]

    bk_data = [[
        Paragraph("<b>Severity</b>", ParagraphStyle("bkh", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold", alignment=TA_CENTER)),
        Paragraph("<b>Count</b>",    ParagraphStyle("bkh", fontSize=9, textColor=colors.white, fontName="Helvetica-Bold", alignment=TA_CENTER)),
        Paragraph("<b>% of Total</b>",ParagraphStyle("bkh",fontSize=9, textColor=colors.white, fontName="Helvetica-Bold", alignment=TA_CENTER)),
    ]]
    for sev in order:
        cnt = sev_counts.get(sev, 0)
        pct = f"{cnt/len(df)*100:.1f}%" if len(df) else "0.0%"
        sc  = SEVERITY_COLORS.get(sev, colors.black)
        bk_data.append([
            Paragraph(f"<b>{sev}</b>", ParagraphStyle("bkcell", fontSize=9, textColor=sc, fontName="Helvetica-Bold", alignment=TA_CENTER)),
            Paragraph(str(cnt),         ParagraphStyle("bkcell2", fontSize=9, textColor=COL_PRIMARY, fontName="Helvetica", alignment=TA_CENTER)),
            Paragraph(pct,              ParagraphStyle("bkcell2", fontSize=9, textColor=COL_PRIMARY, fontName="Helvetica", alignment=TA_CENTER)),
        ])

    bk_tbl = Table(bk_data, colWidths=[4*cm, 3*cm, 3.5*cm])
    bk_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0),  COL_HEADER_BG),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),   [COL_ROW_ODD, COL_ROW_EVEN]),
        ("GRID",         (0, 0), (-1, -1), 0.3, colors.HexColor("#CCCCCC")),
        ("LINEBELOW",    (0, 0), (-1, 0),  1.2, COL_ACCENT),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
    ]))

    return [
        Paragraph("Severity Breakdown", styles["section_heading"]),
        bk_tbl,
        Spacer(1, 10),
    ]


# ─────────────────────────────────────────────
# 8.  BUILD PDF
# ─────────────────────────────────────────────
def build_pdf(df: pd.DataFrame, output_path: str):
    doc = SimpleDocTemplate(
        output_path,
        pagesize=landscape(A4),
        leftMargin=1.5*cm, rightMargin=1.5*cm,
        topMargin=2.0*cm,  bottomMargin=1.5*cm,
    )

    styles  = build_styles()
    rc      = ReportCanvas(doc, styles)
    story   = []

    # ── Cover block (inside page, below banner) ──
    now = datetime.now().strftime("%d %B %Y, %H:%M")
    story += [
        Spacer(1, 6),
        Table(
            [[
                Paragraph(COMPANY_NAME,    styles["company"]),
                Paragraph(CLASSIFICATION,  styles["classification"]),
            ]],
            colWidths=["75%", "25%"],
            style=TableStyle([("VALIGN", (0,0), (-1,-1), "BOTTOM")])
        ),
        Paragraph(REPORT_TITLE,   styles["report_title"]),
        Paragraph(REPORT_SUBTITLE, styles["subtitle"]),
        HRFlowable(width="100%", thickness=1.5, color=COL_ACCENT, spaceAfter=6),
        Table(
            [[
                Paragraph(f"<b>Report Date:</b> {now}", styles["meta"]),
                Paragraph(f"<b>Total Records:</b> {len(df)}", styles["meta"]),
                Paragraph(f"<b>Department:</b> {DEPARTMENT}", styles["meta"]),
            ]],
            colWidths=["33%", "33%", "34%"],
        ),
        Spacer(1, 10),
    ]

    # ── Summary cards ──
    story += build_summary(df, styles)
    story += [HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC"), spaceAfter=4)]

    # ── Severity breakdown ──
    story += build_breakdown(df, styles)

    # ── Main data table ──
    story += build_data_table(df, styles)

    # ── Disclaimer footer note ──
    story += [
        Spacer(1, 6),
        HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#CCCCCC"), spaceAfter=4),
        Paragraph(
            "This report is automatically generated and classified as confidential. "
            "Unauthorized distribution or reproduction is strictly prohibited. "
            f"© {datetime.now().year} {COMPANY_NAME}. All rights reserved.",
            styles["footer"],
        ),
    ]

    doc.build(story, onFirstPage=rc.on_page, onLaterPages=rc.on_page)
    print(f"✅  PDF saved → {output_path}")


# ─────────────────────────────────────────────
# 9.  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    excel_path = sys.argv[1] if len(sys.argv) > 1 else "input.xlsx"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "."
    output_name = sys.argv[3] if len(sys.argv) > 3 else "security_alert_report.pdf"

    if os.path.exists(excel_path):
        try:
            df = load_excel(excel_path)
            print(f"✅  Loaded {len(df)} rows from '{excel_path}'")
        except Exception as e:
            print(f"❌  Error reading Excel file: {e}")
            print("📋  Using built-in sample security alert data instead.\n")
            df = load_sample()
    else:
        print(f"⚠️  File not found: {excel_path}")
        print("📋  Using built-in sample security alert data instead.\n")
        df = load_sample()

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_name)
    build_pdf(df, output_path)
