"""Draw the two guide diagrams with PyMuPDF and save them as PNG."""

import os
import sys

# Optional: a folder where the tool's Python packages were installed with pip --target.
if os.environ.get("DOC_TOOLS_DEPS"):
    sys.path.insert(0, os.environ["DOC_TOOLS_DEPS"])

import pymupdf as fitz

OUT = sys.argv[1]

INK = (0.13, 0.15, 0.20)
MUTED = (0.40, 0.43, 0.50)
BLUE = (0.16, 0.38, 0.72)
BLUE_BG = (0.90, 0.94, 0.99)
GREEN = (0.13, 0.50, 0.33)
GREEN_BG = (0.90, 0.97, 0.92)
ORANGE = (0.78, 0.42, 0.10)
ORANGE_BG = (1.00, 0.95, 0.88)
GREY_BG = (0.95, 0.95, 0.96)


def box(page, rect, title, sub, fill, stroke):
    r = fitz.Rect(*rect)
    page.draw_rect(r, color=stroke, fill=fill, width=1.2, radius=0.08)
    page.insert_textbox(
        fitz.Rect(r.x0 + 4, r.y0 + 5, r.x1 - 4, r.y0 + 26),
        title,
        fontsize=10.5,
        fontname="hebo",
        color=INK,
        align=1,
    )
    if sub:
        page.insert_textbox(
            fitz.Rect(r.x0 + 4, r.y0 + 25, r.x1 - 4, r.y1 - 2),
            sub,
            fontsize=8,
            fontname="helv",
            color=MUTED,
            align=1,
        )
    return r


def arrow(page, p1, p2, color=INK, dashed=False):
    p1, p2 = fitz.Point(*p1), fitz.Point(*p2)
    page.draw_line(p1, p2, color=color, width=1.2, dashes="[3 2] 0" if dashed else None)
    d = p2 - p1
    length = (d.x**2 + d.y**2) ** 0.5 or 1
    ux, uy = d.x / length, d.y / length
    s = 6
    a = fitz.Point(p2.x - s * ux + s * 0.5 * uy, p2.y - s * uy - s * 0.5 * ux)
    b = fitz.Point(p2.x - s * ux - s * 0.5 * uy, p2.y - s * uy + s * 0.5 * ux)
    page.draw_polyline([a, p2, b], color=color, fill=color, width=1.2, closePath=True)


def label(page, x, y, text, color):
    page.insert_text((x, y), text, fontsize=7.5, fontname="helv", color=color)


def save(doc, name, zoom=3):
    doc[0].get_pixmap(matrix=fitz.Matrix(zoom, zoom)).save(f"{OUT}/{name}")


# ------------------------------------------------------------ architecture
doc = fitz.open()
page = doc.new_page(width=760, height=520)
page.draw_rect(page.rect, color=None, fill=(1, 1, 1))
page.insert_text(
    (20, 28),
    "Payment Hub local stack: payments and telemetry",
    fontsize=14,
    fontname="hebo",
    color=INK,
)
for y, text in ((52, "PAYMENTS"), (262, "METRICS"), (400, "LOGS")):
    page.insert_text((20, y), text, fontsize=8, fontname="hebo", color=MUTED)

box(
    page,
    (20, 62, 170, 132),
    "ESS (simulator)",
    "plays FIN / SnF / compliance\nsends payments (M1)\nreceives outbound (M2)",
    ORANGE_BG,
    ORANGE,
)
box(
    page,
    (250, 62, 450, 132),
    "Payment Hub",
    "gRPC edge + 11 stages\n(python -m hub all)",
    BLUE_BG,
    BLUE,
)
box(
    page,
    (530, 62, 720, 132),
    "Kafka broker",
    "topics hub.*  (KRaft)\nJMX exporter :9404",
    BLUE_BG,
    BLUE,
)
box(
    page,
    (300, 162, 470, 222),
    "PostgreSQL",
    "system of record\n(written by DB sink)",
    BLUE_BG,
    BLUE,
)

arrow(page, (170, 85), (250, 85), BLUE)
label(page, 176, 80, "Deliver* (gRPC)", BLUE)
arrow(page, (250, 110), (170, 110), BLUE)
label(page, 178, 124, "Send* / Screen", BLUE)
arrow(page, (450, 85), (530, 85), BLUE)
label(page, 468, 80, "produce", BLUE)
arrow(page, (530, 110), (450, 110), BLUE)
label(page, 466, 124, "consume", BLUE)
arrow(page, (420, 132), (420, 162), BLUE)
label(page, 426, 151, "DB sink writes", BLUE)

box(page, (20, 272, 170, 342), "Grafana :3000", "3 dashboards\nrun annotations", GREEN_BG, GREEN)
box(
    page,
    (250, 272, 450, 342),
    "Prometheus :9090",
    "scrapes /metrics every 5 s\nkeeps 24 h",
    GREEN_BG,
    GREEN,
)
box(
    page,
    (530, 272, 720, 342),
    "kafka-exporter :9308",
    "consumer-group lag\nas the broker sees it",
    GREEN_BG,
    GREEN,
)

arrow(page, (130, 132), (268, 272), GREEN, dashed=True)
label(page, 168, 205, ":9464 (ESS)", GREEN)
arrow(page, (280, 132), (280, 272), GREEN, dashed=True)
label(page, 285, 250, ":9464 (Hub)", GREEN)
arrow(page, (560, 132), (432, 272), GREEN, dashed=True)
label(page, 505, 245, ":9404 (JMX)", GREEN)
arrow(page, (690, 132), (690, 272), GREEN, dashed=True)
label(page, 696, 205, "lag", GREEN)
arrow(page, (530, 307), (450, 307), GREEN)
label(page, 476, 302, "scrape", GREEN)
arrow(page, (250, 307), (170, 307), GREEN)
label(page, 192, 302, "PromQL", GREEN)

box(
    page,
    (20, 410, 170, 470),
    "Filebeat",
    "reads container stdout\n(ECS JSON lines)",
    GREY_BG,
    MUTED,
)
box(
    page, (250, 410, 450, 470), "Elasticsearch :9200", "logs-payments-*\nkept 1 day", GREY_BG, MUTED
)
box(page, (530, 410, 720, 470), "Kibana :5601", "search one payment\nby UETR", GREY_BG, MUTED)
arrow(page, (170, 440), (250, 440), MUTED)
label(page, 200, 435, "ship", MUTED)
arrow(page, (450, 440), (530, 440), MUTED)
label(page, 478, 435, "query", MUTED)
page.draw_line(fitz.Point(20, 120), fitz.Point(8, 120), color=MUTED, width=1.2, dashes="[3 2] 0")
page.draw_line(fitz.Point(8, 120), fitz.Point(8, 440), color=MUTED, width=1.2, dashes="[3 2] 0")
arrow(page, (8, 440), (20, 440), MUTED, dashed=True)
label(page, 14, 382, "container stdout", MUTED)
page.insert_text(
    (20, 500),
    "Solid arrows: data in use. Dashed: where telemetry is collected from. Ports are on localhost.",
    fontsize=8,
    fontname="helv",
    color=MUTED,
)
save(doc, "diagram_stack.png")

# ------------------------------------------------------------ triage funnel
doc = fitz.open()
page = doc.new_page(width=760, height=300)
page.draw_rect(page.rect, color=None, fill=(1, 1, 1))
page.insert_text(
    (20, 28), "The triage method used in every scenario", fontsize=14, fontname="hebo", color=INK
)
steps = [
    ("1. User affected?", "E2E Latency (Simulator)\nM1 sent / M2 received"),
    ("2. Work piling up?", "Consumer Lag (Broker)\nper consumer group"),
    ("3. Which stage?", "IO Wait Ratio\nProcess latency avg / max"),
    ("4. Why?", "gRPC calls, commit vs\nprocess, JDBC, memory"),
    ("5. Confirm", "one payment's journey\nby UETR (Kibana, DB)"),
]
x = 20
for i, (title, sub) in enumerate(steps):
    fill, stroke = (ORANGE_BG, ORANGE) if i == 4 else (BLUE_BG, BLUE)
    box(page, (x, 60, x + 130, 140), title, sub, fill, stroke)
    if i < 4:
        arrow(page, (x + 130, 100), (x + 148, 100), INK)
    x += 148
page.insert_textbox(
    fitz.Rect(20, 165, 740, 295),
    "Metrics (steps 1 to 4) tell you THAT something is wrong and WHERE. "
    "The per-payment trace (step 5) tells you WHAT happened, hop by hop.\n\n"
    "Read the dashboard top-down, in this order, every time. Jumping straight to step 4 "
    "is how a slow dependency gets blamed on the stage that calls it (scenario 1), and how "
    "a failure with no lag gets missed altogether (scenario 4).",
    fontsize=10,
    fontname="helv",
    color=INK,
)
save(doc, "diagram_triage.png")
print("diagrams written")
