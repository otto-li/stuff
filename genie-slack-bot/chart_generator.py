"""Auto-generate charts from Genie query results."""

from __future__ import annotations

import io
import logging
import re
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

logger = logging.getLogger(__name__)

# Thresholds
MIN_ROWS = 2
MAX_ROWS_BAR = 30
MAX_ROWS_LINE = 500


def _try_parse_number(val: str) -> float | None:
    try:
        return float(val.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _try_parse_date(val: str) -> datetime | None:
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m"):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None


def _is_numeric_col(rows: list[list[str]], col_idx: int) -> bool:
    """Check if >=80% of non-empty values are numeric."""
    nums = 0
    total = 0
    for row in rows:
        if col_idx < len(row) and row[col_idx].strip():
            total += 1
            if _try_parse_number(row[col_idx]) is not None:
                nums += 1
    return total > 0 and nums / total >= 0.8


def _is_date_col(rows: list[list[str]], col_idx: int) -> bool:
    """Check if >=80% of non-empty values are dates."""
    dates = 0
    total = 0
    for row in rows:
        if col_idx < len(row) and row[col_idx].strip():
            total += 1
            if _try_parse_date(row[col_idx]) is not None:
                dates += 1
    return total > 0 and dates / total >= 0.8


def generate_chart(
    columns: list[str], rows: list[list[str]]
) -> bytes | None:
    """Generate a chart PNG from query results. Returns bytes or None if not chartable."""
    if not columns or not rows or len(rows) < MIN_ROWS:
        return None

    num_cols = len(columns)
    if num_cols < 2:
        return None

    # Find numeric columns (candidates for y-axis)
    numeric_idxs = [i for i in range(num_cols) if _is_numeric_col(rows, i)]
    if not numeric_idxs:
        return None

    # Find best x-axis: prefer date column, then first non-numeric column
    date_idx = None
    label_idx = None
    for i in range(num_cols):
        if i in numeric_idxs:
            continue
        if _is_date_col(rows, i):
            date_idx = i
            break
        if label_idx is None:
            label_idx = i

    x_idx = date_idx if date_idx is not None else label_idx
    is_date = date_idx is not None

    # If all columns are numeric, use first as x and rest as y
    if x_idx is None:
        x_idx = 0
        y_idxs = [i for i in numeric_idxs if i != 0]
    else:
        y_idxs = numeric_idxs

    if not y_idxs:
        return None

    # Limit y columns to 5 for readability
    y_idxs = y_idxs[:5]

    # Choose chart type
    if is_date:
        chart_type = "line"
        max_rows = MAX_ROWS_LINE
    elif len(rows) <= MAX_ROWS_BAR:
        chart_type = "bar"
        max_rows = MAX_ROWS_BAR
    else:
        chart_type = "line"
        max_rows = MAX_ROWS_LINE

    plot_rows = rows[:max_rows]

    # Parse x values
    if is_date:
        x_vals = [_try_parse_date(r[x_idx]) for r in plot_rows]
        valid = [(i, v) for i, v in enumerate(x_vals) if v is not None]
        if len(valid) < MIN_ROWS:
            return None
        indices = [i for i, _ in valid]
        x_vals = [v for _, v in valid]
    else:
        indices = list(range(len(plot_rows)))
        x_vals = [r[x_idx] if x_idx < len(r) else "" for r in plot_rows]

    # Parse y values per series
    series: list[tuple[str, list[float | None]]] = []
    for yi in y_idxs:
        vals = []
        for i in indices:
            row = plot_rows[i]
            vals.append(_try_parse_number(row[yi]) if yi < len(row) else None)
        series.append((columns[yi], vals))

    # Plot
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")
    ax.tick_params(colors="#e0e0e0")
    ax.xaxis.label.set_color("#e0e0e0")
    ax.yaxis.label.set_color("#e0e0e0")
    ax.title.set_color("#e0e0e0")
    for spine in ax.spines.values():
        spine.set_color("#444")

    colors = ["#00d4ff", "#ff6b6b", "#ffd93d", "#6bcb77", "#c084fc"]

    if chart_type == "bar" and len(series) == 1:
        name, vals = series[0]
        safe_vals = [v if v is not None else 0 for v in vals]
        ax.bar(range(len(x_vals)), safe_vals, color=colors[0], alpha=0.85)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(
            [str(x)[:20] for x in x_vals], rotation=45, ha="right", fontsize=8, color="#e0e0e0"
        )
        ax.set_ylabel(name, color="#e0e0e0")
    elif chart_type == "bar":
        import numpy as np
        x_pos = np.arange(len(x_vals))
        width = 0.8 / len(series)
        for j, (name, vals) in enumerate(series):
            safe_vals = [v if v is not None else 0 for v in vals]
            ax.bar(x_pos + j * width, safe_vals, width, label=name, color=colors[j % len(colors)], alpha=0.85)
        ax.set_xticks(x_pos + width * (len(series) - 1) / 2)
        ax.set_xticklabels(
            [str(x)[:20] for x in x_vals], rotation=45, ha="right", fontsize=8, color="#e0e0e0"
        )
        ax.legend(facecolor="#2a2a4a", edgecolor="#444", labelcolor="#e0e0e0")
    else:
        # Line chart
        for j, (name, vals) in enumerate(series):
            if is_date:
                ax.plot(x_vals, vals, label=name, color=colors[j % len(colors)], linewidth=2, marker="o", markersize=3)
            else:
                ax.plot(range(len(vals)), vals, label=name, color=colors[j % len(colors)], linewidth=2)
                ax.set_xticks(range(len(x_vals)))
                ax.set_xticklabels(
                    [str(x)[:15] for x in x_vals], rotation=45, ha="right", fontsize=8, color="#e0e0e0"
                )
        if is_date:
            ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.AutoDateLocator()))
            fig.autofmt_xdate()
            ax.tick_params(axis="x", colors="#e0e0e0")
        if len(series) > 1:
            ax.legend(facecolor="#2a2a4a", edgecolor="#444", labelcolor="#e0e0e0")

    ax.set_xlabel(columns[x_idx], color="#e0e0e0")
    if len(series) == 1:
        ax.set_title(f"{series[0][0]} by {columns[x_idx]}", fontsize=13, color="#e0e0e0")
    else:
        ax.set_title(f"Results by {columns[x_idx]}", fontsize=13, color="#e0e0e0")

    ax.grid(True, alpha=0.2, color="#888")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()
