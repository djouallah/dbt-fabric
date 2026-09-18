"""Generate docs/how-it-works-dark.svg — the hand-drawn README diagram.

    python docs/how-it-works.py docs

One geometry, drawn twice per stroke with jitter, in a handwriting font: the picture is
meant to look like someone sketched the medallion on a whiteboard, not like a tool emitted
it. Dark only, on purpose — it carries its own background, so it reads on either GitHub
theme.
"""
import math
import random
import re
import sys

W, H = 1000, 600

BG = "#1f1f1f"
INK = "#e5e7eb"
SOFT = "#9ca3af"
GOLD = "#fbbf24"
BRONZE = "#cd8b48"
SILVER = "#b9c4d0"

T_BRONZE = "rgba(205,139,72,0.085)"
T_SILVER = "rgba(185,196,208,0.06)"
T_GOLD = "rgba(251,191,36,0.08)"

rnd = random.Random(20260918)


def n(v):
    return round(v, 1)


def jit(v, a):
    return n(v + rnd.uniform(-a, a))


def line(x1, y1, x2, y2, segs=3, a=1.3):
    pts = []
    for i in range(segs + 1):
        t = i / segs
        pts.append((jit(x1 + (x2 - x1) * t, a), jit(y1 + (y2 - y1) * t, a)))
    return "M" + " L".join("%s %s" % p for p in pts)


def stroke(cls, d):
    return "<path class='%s' d='%s'/>" % (cls, d)


def box(x, y, w, h, cls="s", segs=3, a=1.3):
    sides = [(x, y, x + w, y), (x + w, y, x + w, y + h),
             (x + w, y + h, x, y + h), (x, y + h, x, y)]
    out = []
    for (x1, y1, x2, y2) in sides:
        for _ in range(2):
            out.append(stroke(cls, line(x1, y1, x2, y2, segs, a)))
    return out


def tint(x, y, w, h, fill):
    return "<path d='M%d %d h%d v%d h%d z' fill='%s' stroke='none'/>" % (
        x, y, w, h, -w, fill)


def arrow(x1, y1, x2, y2, cls="s", bow=0.13, head=12, a=0.9):
    """Two wobbly strokes along a quadratic, plus a two-stroke arrowhead."""
    dx, dy = x2 - x1, y2 - y1
    mx, my = (x1 + x2) / 2, (y1 + y2) / 2
    cx, cy = mx - dy * bow, my + dx * bow
    out = []
    for _ in range(2):
        out.append(stroke(cls, "M%s %s Q%s %s %s %s" % (
            jit(x1, a), jit(y1, a), n(cx), n(cy), jit(x2, a), jit(y2, a))))
    ang = math.atan2(y2 - cy, x2 - cx)
    for spread in (2.6, -2.6):
        hx = x2 + head * math.cos(ang + spread)
        hy = y2 + head * math.sin(ang + spread)
        for _ in range(2):
            out.append(stroke(cls, "M%s %s L%s %s" % (
                jit(x2, a), jit(y2, a), jit(hx, a), jit(hy, a))))
    return out


NUM = re.compile(r"-?\d+(?:\.\d+)?")


def sketch(d, cls="s", a=0.8, times=2):
    """Draw a path string twice, jittering every coordinate: doodle ink."""
    out = []
    for _ in range(times):
        out.append(stroke(cls, NUM.sub(lambda m: str(jit(float(m.group()), a)), d)))
    return out


def text(x, y, s, size=14, cls="ink", anchor="start", italic=False, rot=None):
    bits = ["<text x='%s' y='%s' font-size='%s' text-anchor='%s' class='%s'" % (
        n(x), n(y), size, anchor, cls)]
    if italic:
        bits.append(" font-style='italic'")
    if rot:
        bits.append(" transform='rotate(%s %s %s)'" % (n(rot), n(x), n(y)))
    bits.append(">%s</text>" % s)
    return "".join(bits)


def dot(x, y, r=1.8, cls="ink-fill"):
    return "<circle cx='%s' cy='%s' r='%s' class='%s'/>" % (n(x), n(y), r, cls)


# ---------------------------------------------------------------- doodles
def circ(cx, cy, r):
    """A circle as four cubics. NOT an arc: sketch() jitters every number it finds, and a
    jittered large-arc/sweep flag is an invalid path that renders as nothing."""
    k = 0.5523 * r
    return ("M%s %s C%s %s %s %s %s %s C%s %s %s %s %s %s "
            "C%s %s %s %s %s %s C%s %s %s %s %s %s" % (
                n(cx - r), n(cy), n(cx - r), n(cy - k), n(cx - k), n(cy - r), n(cx), n(cy - r),
                n(cx + k), n(cy - r), n(cx + r), n(cy - k), n(cx + r), n(cy),
                n(cx + r), n(cy + k), n(cx + k), n(cy + r), n(cx), n(cy + r),
                n(cx - k), n(cy + r), n(cx - r), n(cy + k), n(cx - r), n(cy)))


def duck(cx, cy, cls="s", running=False):
    o = []
    o += sketch("M%d %d q2 -13 15 -13 q11 0 12 9 q1 6 -7 8 q-13 3 -20 -4 z"
                % (cx - 16, cy + 6), cls)
    o += sketch(circ(cx + 10, cy - 11, 6.5), cls, a=0.6)
    o += sketch("M%d %d l9 3 l-9 3" % (cx + 17, cy - 12), cls)
    o.append(dot(cx + 11, cy - 13, 1.6))
    if running:
        o += sketch("M%d %d l-11 0" % (cx - 20, cy - 4), cls)
        o += sketch("M%d %d l-15 0" % (cx - 20, cy + 2), cls)
        o += sketch("M%d %d l-9 0" % (cx - 20, cy + 8), cls)
        o += sketch("M%d %d l-4 8 M%d %d l4 8" % (cx - 6, cy + 8, cx + 4, cy + 8), cls)
    return o


def waves(cx, cy, cls="s", width=24):
    return sketch("M%d %d q6 -4 12 0 q6 4 12 0 q6 -4 12 0" % (cx - width, cy), cls)


def iceberg(cx, cy, cls="s"):
    o = []
    o += sketch("M%d %d l14 19 l-27 0 z" % (cx - 1, cy - 20), cls)
    o += sketch("M%d %d l-9 12 q14 10 31 2 l-6 -14" % (cx - 11, cy + 1), cls)
    o += waves(cx - 2, cy + 1, cls, 26)
    return o


def warehouse(cx, cy, cls="s"):
    o = []
    o += sketch("M%d %d l17 -13 l17 13" % (cx - 17, cy - 3), cls)
    o += sketch("M%d %d l0 17 l29 0 l0 -17" % (cx - 14, cy - 3), cls)
    o += sketch("M%d %d l0 -10 l9 0 l0 10" % (cx - 4, cy + 14), cls)
    return o


def bolt(cx, cy, cls="s"):
    return sketch("M%d %d l-13 19 l9 0 l-6 14 l15 -20 l-9 0 z" % (cx + 3, cy - 17), cls)


def papers(cx, cy, cls="s"):
    """A stack of CSVs with one ragged edge — AEMO's rows are not all the same width."""
    o = []
    o += sketch("M%d %d l19 0 l0 25 l-19 0 z" % (cx - 12, cy - 17), cls)
    o += sketch("M%d %d l19 0 l0 8 l-4 0 l4 5 l-5 4 l5 5 l-19 0 z" % (cx - 6, cy - 11), cls)
    for i in range(3):
        o += sketch("M%d %d l10 0" % (cx - 2, cy - 4 + i * 6), cls, a=0.6, times=1)
    return o


def brush(cx, cy, cls="s"):
    """A scrubbing brush and three bubbles: the cleansing layer."""
    o = []
    o += sketch("M%d %d l24 0 l0 9 l-24 0 z" % (cx - 14, cy - 3), cls)
    o += sketch("M%d %d q11 -1 11 -11" % (cx + 10, cy - 3), cls)
    for i in range(5):
        o += sketch("M%d %d l0 8" % (cx - 11 + i * 6, cy + 6), cls, a=0.6, times=1)
    for (bx, by, r) in ((cx - 19, cy - 15, 4), (cx - 8, cy - 22, 3), (cx + 2, cy - 16, 2.5)):
        o += sketch(circ(bx, by, r), cls, a=0.4)
    return o


def medal(cx, cy, cls="s"):
    o = []
    o += sketch("M%d %d l7 13 M%d %d l-7 13" % (cx - 9, cy - 20, cx + 9, cy - 20), cls)
    o += sketch(circ(cx, cy + 4, 10), cls, a=0.6)
    o += sketch("M%d %d l0 9 M%d %d l8 5 M%d %d l-8 5"
                % (cx, cy - 1, cx - 4, cy + 2, cx + 4, cy + 2), cls, a=0.5)
    return o


def bars(cx, cy, cls="s"):
    o = []
    for i, h in enumerate((7, 13, 19)):
        o += sketch("M%d %d l0 -%d l6 0 l0 %d" % (cx - 12 + i * 9, cy + 9, h, h), cls)
    return o


# ---------------------------------------------------------------- the scene
def scene():
    o = []

    o.append(text(44, 40, "a medallion you can re-engine", 19, "ink", rot=-0.5))
    o.append(text(330, 40, "- raw is python, everything after it is SQL", 13, "soft"))

    # ---------------------------------------------------------- bronze
    o.append(tint(42, 72, 256, 248, T_BRONZE))
    o += box(40, 70, 260, 252, "sbr")
    o.append(text(58, 106, "bronze", 18, "bro"))
    o.append(text(130, 106, "raw - files, not tables", 12, "soft", italic=True))
    o.append(text(58, 142, "download_aemo.py", 14))
    o.append(text(58, 162, "python - not a dbt model,", 11, "soft"))
    o.append(text(58, 180, "no engine involved yet", 11, "soft"))
    o.append(text(58, 214, "csv_raw/*.csv", 13))
    o.append(text(58, 232, "nemweb zips, unzipped. plain", 11, "soft"))
    o.append(text(58, 250, "CSV, ragged: 53- and 130-", 11, "soft"))
    o.append(text(58, 268, "column records in one file", 11, "soft"))
    o.append(text(58, 298, "csv_raw_archive_log.parquet", 12))
    o.append(text(58, 314, "the watermark: what landed", 11, "soft"))
    o += papers(264, 156, "ss")
    o.append(text(44, 344, "landed ONCE - every engine reads the same bytes",
                  11, "soft", italic=True))

    # ---------------------------------------------------------- silver
    o.append(tint(347, 72, 291, 248, T_SILVER))
    o += box(345, 70, 295, 252, "ssv")
    o.append(text(363, 106, "silver", 18, "sil"))
    o.append(text(428, 106, "cleansed - &lt;engine&gt;_landing", 12, "soft", italic=True))
    o.append(text(363, 144, "stg_csv_archive_log", 13))
    o.append(text(363, 162, "a view on the log - which files are new", 11, "soft"))
    o.append(text(363, 198, "fct_price  ·  fct_scada", 13))
    o.append(text(363, 216, "the daily archive, typed and deduped:", 11, "soft"))
    o.append(text(363, 234, "all 130 and all 53 columns, nothing dropped", 11, "soft"))
    o.append(text(363, 270, "fct_price_today  ·  fct_scada_today", 13))
    o.append(text(363, 288, "the intraday feed, same columns,", 11, "soft"))
    o.append(text(363, 306, "still AEMO's own shape", 11, "soft"))
    o += brush(598, 292, "ss")

    # ---------------------------------------------------------- gold
    o.append(tint(688, 72, 270, 248, T_GOLD))
    o += box(686, 70, 274, 252, "sa")
    o.append(text(704, 106, "gold", 18, "acc"))
    o.append(text(758, 106, "curated - &lt;engine&gt;_mart", 12, "soft", italic=True))
    o.append(text(704, 144, "fct_summary", 15))
    o.append(text(704, 166, "one row per (date, time, DUID):", 11, "soft"))
    o.append(text(704, 184, "generation joined to the price", 11, "soft"))
    o.append(text(704, 202, "of its region, merged key by key", 11, "soft"))
    o.append(text(704, 236, "dim_duid  ·  dim_calendar", 13))
    o.append(text(704, 254, "the star it is read through:", 11, "soft"))
    o.append(text(704, 272, "unit, fuel, region  ·  every date", 11, "soft"))
    o.append(text(704, 300, "Direct Lake reads all three", 11, "soft"))
    o.append(text(704, 316, "in place - no copy, no import", 11, "soft"))
    o += medal(930, 150, "ss")
    o += bars(930, 300, "ss")

    # ---------------------------------------------------------- the two SQL steps
    o += arrow(303, 196, 340, 196, "s", bow=0.06, head=11)
    o.append(text(321, 178, "dbt SQL", 10, "soft", anchor="middle"))
    o += arrow(643, 196, 681, 196, "s", bow=0.06, head=11)
    o.append(text(662, 178, "dbt SQL", 10, "soft", anchor="middle"))

    # ---------------------------------------------------------- the engine
    o.append(text(946, 556, "...the only thing that changes", 12, "soft",
                  anchor="end", italic=True))
    for (x1, y1, x2, y2) in ((40, 376, 960, 376), (960, 376, 960, 566),
                             (960, 566, 40, 566), (40, 566, 40, 376)):
        o.append(stroke("sb", line(x1, y1, x2, y2, 5, 1.1)))

    o += arrow(492, 372, 492, 330, "ss", bow=0.04, head=10, a=0.7)
    o += arrow(823, 372, 823, 330, "ss", bow=0.04, head=10, a=0.7)
    o.append(text(658, 354, "these two run on whichever engine you pick", 11, "soft",
                  anchor="middle", italic=True))

    o.append(text(500, 408, "dbt build --target &lt;engine&gt;", 17, "ink", anchor="middle"))
    o.append(text(500, 432, "five model trees, the same SQL (at least in spirit) - "
                            "the flag enables one, the other four parse disabled",
                  12, "soft", anchor="middle", italic=True))

    chips = [
        ("duckrun", "DuckDB, single node", "&#8594; Delta, via delta-rs",
         lambda x, y: duck(x, y, "ss", True)),
        ("iceberg", "DuckDB + iceberg extension", "&#8594; Iceberg, REST catalog",
         lambda x, y: iceberg(x, y, "ss")),
        ("ducklake", "DuckDB + a SQL catalog", "&#8594; parquet + Delta metadata",
         lambda x, y: duck(x, y, "ss") + waves(x, y + 15, "ss", 20)),
        ("dwh", "Fabric Warehouse", "&#8594; Delta",
         lambda x, y: warehouse(x, y, "ss")),
        ("spark", "Fabric Spark", "&#8594; Delta, in a Lakehouse",
         lambda x, y: bolt(x, y, "ss")),
    ]
    for i, (name, engine, out, art) in enumerate(chips):
        x = 44 + i * 185
        o += box(x, 452, 172, 88, "s", segs=3, a=1.1)
        o.append(text(x + 14, 478, name, 15))
        o.append(text(x + 14, 502, engine, 10.5, "soft"))
        o.append(text(x + 14, 522, out, 10.5, "soft"))
        o += art(x + 146, 480)

    return o


def render(body):
    head = [
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 %d %d' width='%d' height='%d'>"
        % (W, H, W, H),
        "<style>",
        "text{font-family:'Segoe Print','Bradley Hand','Comic Sans MS','Chalkboard SE',cursive}",
        ".ink{fill:%s}.soft{fill:%s}.acc{fill:%s}.bro{fill:%s}.sil{fill:%s}.ink-fill{fill:%s}"
        % (INK, SOFT, GOLD, BRONZE, SILVER, INK),
        ".s{fill:none;stroke:%s;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}" % INK,
        ".sa{fill:none;stroke:%s;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}" % GOLD,
        ".sbr{fill:none;stroke:%s;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}" % BRONZE,
        ".ssv{fill:none;stroke:%s;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}" % SILVER,
        ".ss{fill:none;stroke:%s;stroke-width:1.3;stroke-linecap:round;stroke-linejoin:round}" % SOFT,
        ".sb{fill:none;stroke:%s;stroke-width:1.3;stroke-linecap:round;stroke-dasharray:11 9}" % SOFT,
        "</style>",
        "<rect x='0' y='0' width='%d' height='%d' rx='14' fill='%s'/>" % (W, H, BG),
    ]
    return "\n".join(head + body + ["</svg>", ""])


if __name__ == "__main__":
    out = (sys.argv[1] if len(sys.argv) > 1 else "docs") + "/how-it-works-dark.svg"
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(render(scene()))
    print(out)
