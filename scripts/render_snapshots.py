"""Render the README result images from the real files in results/.

Nothing is typed in by hand: every number comes from results/*.json or the saved command output.

    uv run --with pillow python scripts/render_snapshots.py

Fonts: DejaVu Sans and DejaVu Sans Mono by default. Point FONT_SANS, FONT_SANS_BOLD and FONT_MONO at other
TTF files if they are not installed in the usual Linux or macOS places.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
OUT = ROOT / "docs" / "images"

SANS = [os.environ.get("FONT_SANS"), "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/System/Library/Fonts/Supplemental/Arial.ttf"]
SANS_BOLD = [
    os.environ.get("FONT_SANS_BOLD"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
]
MONO = [os.environ.get("FONT_MONO"), "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", "/System/Library/Fonts/Menlo.ttc"]

CANVAS = (1100, 600)  # every findings chart uses the same size so they align in the README

INK = (31, 41, 55)
MUTED = (75, 85, 99)
RULE = (209, 213, 219)
GREEN = (21, 128, 61)
BLUE = (29, 78, 216)
AMBER = (180, 83, 9)
WHITE = (255, 255, 255)

TERM_BG = (22, 27, 34)
TERM_FG = (230, 237, 243)
TERM_DIM = (139, 148, 158)
TERM_GREEN = (86, 211, 100)
TERM_CYAN = (121, 192, 255)


def font(candidates: list[str | None], size: int) -> ImageFont.FreeTypeFont:
    for path in candidates:
        if path and Path(path).exists():
            return ImageFont.truetype(path, size)
    raise SystemExit(f"no usable font among {candidates}; set FONT_SANS, FONT_SANS_BOLD or FONT_MONO")


def terminal(lines: list[str], dest: Path, width_chars: int | None = None) -> None:
    f = font(MONO, 17)
    line_h, pad = 26, 32
    width_chars = width_chars or max(len(x) for x in lines)
    char_w = f.getlength("M")
    w = int(char_w * width_chars + pad * 2)
    h = line_h * len(lines) + pad * 2 + 24
    img = Image.new("RGB", (w, h), TERM_BG)
    d = ImageDraw.Draw(img)
    for i, color in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
        d.ellipse((pad + i * 26, 16, pad + i * 26 + 12, 28), fill=color)
    y = pad + 16
    for text in lines:
        if text.startswith("$ "):
            d.text((pad, y), text, font=f, fill=TERM_CYAN)
        elif "PASSED" in text or "passed" in text or "Success" in text or text.startswith("====") and "failed" not in text:
            d.text((pad, y), text, font=f, fill=TERM_GREEN)
        elif text.startswith(("shape:", "(1 row)")):
            d.text((pad, y), text, font=f, fill=TERM_DIM)
        else:
            d.text((pad, y), text, font=f, fill=TERM_FG)
        y += line_h
    img.save(dest, optimize=True)


def test_summary_lines() -> list[str]:
    raw = (RESULTS / "full-run-output.txt").read_text().splitlines()
    keep: list[str] = ["$ ./run.sh test"]
    for line in raw:
        if line.startswith(("platform ", "cachedir", "rootdir", "configfile", "collecting", "plugins")):
            continue  # pytest session header, omitted to keep the image short
        if line.startswith(("EXIT=", "ELAPSED=")):
            continue
        keep.append(line)
    keep.append(f"total elapsed: {(RESULTS / 'full-run-output.txt').read_text().split('ELAPSED=')[-1].strip()} s")
    return keep


def title(d: ImageDraw.ImageDraw, text: str, sub: str, w: int) -> None:
    d.text((40, 30), text, font=font(SANS_BOLD, 28), fill=INK)
    d.text((40, 72), sub, font=font(SANS, 17), fill=MUTED)


def bar_chart(
    dest: Path, heading: str, sub: str, bars: list[tuple[str, float, str, tuple[int, int, int]]], note: str
) -> None:
    label_f, val_f = font(SANS, 18), font(SANS_BOLD, 18)
    w, h = CANVAS
    label_w = 400
    val_w = 200
    plot_w = w - 40 - label_w - val_w - 40
    top = 130
    row_h = min(90, (h - top - 90) // len(bars))
    img = Image.new("RGB", (w, h), WHITE)
    d = ImageDraw.Draw(img)
    title(d, heading, sub, w)
    mx = max(v for _, v, _, _ in bars) or 1
    x0 = 40 + label_w
    d.line((x0, top - 12, x0, top + row_h * len(bars)), fill=RULE, width=2)
    for i, (label, value, text, color) in enumerate(bars):
        y = top + i * row_h
        pad_y = (row_h - 36) // 2
        d.text((40, y + pad_y + 6), label, font=label_f, fill=INK)
        bw = max(int(plot_w * value / mx), 0)
        if bw:
            d.rectangle((x0 + 2, y + pad_y, x0 + 2 + bw, y + pad_y + 36), fill=color)
        d.text((x0 + 2 + bw + 16, y + pad_y + 6), text, font=val_f, fill=INK)
    d.text((40, h - 52), note, font=font(SANS, 16), fill=MUTED)
    img.save(dest, optimize=True)


def load(name: str) -> dict:  # type: ignore[type-arg]
    return json.loads((RESULTS / name).read_text())


def kb_or_mb(n: int) -> str:
    return f"{n / 1_000_000:.1f} MB" if n >= 1_000_000 else f"{n / 1000:.1f} kB"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    terminal(test_summary_lines(), OUT / "test-summary.png", 88)
    terminal((RESULTS / "walkthrough" / "05-status.txt").read_text().splitlines(), OUT / "status-output.png", 80)

    lost, restarts, slot = load("03_lost_ack.json"), load("04_restarts.json"), load("07_slot_wal.json")
    dups = [
        ("Lost ack (3 batches re-sent)", lost["new_duplicate_rows"], BLUE),
        ("Receiver restarted mid-run", restarts["a_receiver_restart"]["new_duplicates"], BLUE),
        ("Debezium killed after write, before ack", restarts["b_kill_after_write_before_ack"]["new_duplicates"], BLUE),
    ]
    all_equal = lost["silver_equals_source"] and all(
        restarts[k]["silver_equals_source"]
        for k in ("a_receiver_restart", "b_kill_after_write_before_ack", "c_kill_while_sink_refuses")
    )
    bar_chart(
        OUT / "findings-duplicates.png",
        "Duplicate rows in Bronze per replay scenario",
        "Rows with an idempotency_key that was already stored. Source: results/03_lost_ack.json, 04_restarts.json",
        [(label, v, f"{v} rows", color) for label, v, color in dups],
        "After the Silver job ran, Silver equaled the source table in every scenario above."
        if all_equal
        else "WARNING: Silver did not equal the source in at least one scenario.",
    )

    s = slot
    idle = s["idle_captured_tables_noise_only"]["after_45s"]["retained_bytes"]
    wal = [
        ("Debezium running", s["while_running"]["retained_bytes"], BLUE),
        ("Debezium stopped, start", s["stopped_start"]["retained_bytes"], AMBER),
        ("Debezium stopped, after the write burst", s["stopped_end"]["retained_bytes"], AMBER),
        ("Restarted and caught up", s["after_restart_and_catch_up"]["retained_bytes"], GREEN),
        ("Running, only an uncaptured table busy", idle, AMBER),
    ]
    bar_chart(
        OUT / "findings-wal.png",
        "WAL retained by the replication slot",
        "pg_wal_lsn_diff(current WAL position, restart_lsn). Source: results/07_slot_wal.json. Last bar: 45 s observed.",
        [(label, v, kb_or_mb(v), color) for label, v, color in wal],
        f"max_slot_wal_keep_size = {s['max_slot_wal_keep_size']} (no cap). Silver equaled the source after the restart: "
        f"{'yes' if s['silver_equals_source_after_restart'] else 'no'}.",
    )

    c2, c3, c5, c6 = load("02_changes.json"), lost, load("05_out_of_order.json"), load("06_schema_change.json")
    rows = [
        (
            "Initial changes (300 transactions)",
            f"{c2['silver_rows']['customers']} / {c2['silver_rows']['orders']}",
            f"{c2['source_rows']['customers']} / {c2['source_rows']['orders']}",
            c2["silver_rows"] == c2["source_rows"],
        ),
        (
            "Lost ack, replayed batches",
            f"{c3['silver_rows']['customers']} / {c3['silver_rows']['orders']}",
            "equal",
            c3["silver_equals_source"],
        ),
        ("Receiver restart", "-", "equal", restarts["a_receiver_restart"]["silver_equals_source"]),
        ("Kill after write, before ack", "-", "equal", restarts["b_kill_after_write_before_ack"]["silver_equals_source"]),
        ("Kill while the sink refuses writes", "-", "equal", restarts["c_kill_while_sink_refuses"]["silver_equals_source"]),
        ("Late events with a lower LSN", "-", "equal", c5["silver_equals_source"]),
        ("Full rebuild from Bronze", "-", "equal", c5["full_refresh_rebuild_equals_source"]),
        (
            "ADD COLUMN region",
            f"{c6['silver_rows_with_region']} with region",
            f"{c6['source_rows_with_region']} with region",
            c6["silver_equals_source"],
        ),
    ]
    f, fb = font(SANS, 18), font(SANS_BOLD, 18)
    cols = [40, 500, 740, 960]
    w, h = CANVAS
    row_h = 52
    top = 140
    img = Image.new("RGB", (w, h), WHITE)
    d = ImageDraw.Draw(img)
    title(d, "Silver table versus the source table", "Customers / orders row counts where the test recorded them. Source: results/*.json", w)
    for x, head in zip(cols, ["Scenario", "Silver rows", "Source rows", "Match"], strict=True):
        d.text((x, top - 34), head, font=fb, fill=MUTED)
    d.line((40, top - 4, w - 40, top - 4), fill=RULE, width=2)
    for i, (name, silver, src, ok) in enumerate(rows):
        y = top + i * row_h
        d.text((cols[0], y + 12), name, font=f, fill=INK)
        d.text((cols[1], y + 12), silver, font=f, fill=INK)
        d.text((cols[2], y + 12), src, font=f, fill=INK)
        d.text((cols[3], y + 12), "yes" if ok else "NO", font=fb, fill=GREEN if ok else (185, 28, 28))
        d.line((40, y + row_h - 2, w - 40, y + row_h - 2), fill=RULE, width=1)
    img.save(OUT / "findings-silver-match.png", optimize=True)
    print("wrote", *sorted(p.name for p in OUT.glob("*.png")))


if __name__ == "__main__":
    main()
