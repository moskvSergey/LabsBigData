"""
ЛР-6. График снижения затрат после оптимизации (требование к отчёту Части 3).

Рисует PNG без внешних зависимостей (только Pillow) — matplotlib в стенде нет.
Запуск:  python resource_management/make_cost_chart.py
Результат: work/cost_reduction.png
"""

from __future__ import annotations

import os
import sys

from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import cost_model  # noqa: E402

OUT = os.path.join(ROOT, "work", "cost_reduction.png")

W, H = 1000, 560
BG = (255, 255, 255)
FG = (32, 32, 32)
GRID = (222, 226, 230)
BAR_BASE = (176, 42, 55)
BAR_OPT = (21, 128, 61)
ACCENT = (13, 71, 161)


def _font(size: int):
    for name in ("segoeui.ttf", "arial.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def build() -> str:
    report = cost_model.simulate()
    cfg = cost_model.load_config()

    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    f_title = _font(24)
    f_lbl = _font(18)
    f_small = _font(15)
    f_big = _font(20)

    d.text((30, 20), "ЛР-6, вариант 8: снижение затрат через on-demand + spot", font=f_title, fill=FG)
    d.text((30, 54), f"SLA: доступность >= {cfg['sla']['availability']:.1%}, инференс < {cfg['sla']['inference_latency_ms']} мс",
           font=f_small, fill=(90, 90, 90))

    plot_x, plot_y = 90, 100
    plot_w, plot_h = 840, 320
    max_cost = report.baseline_cost * 1.25

    # сетка
    for i in range(6):
        y = plot_y + plot_h - int(plot_h * i / 5)
        d.line([(plot_x, y), (plot_x + plot_w, y)], fill=GRID)
        d.text((plot_x - 62, y - 9), f"${max_cost * i / 5:.2f}", font=f_small, fill=(110, 110, 110))

    bars = [
        ("Baseline\n(100% on-demand)", report.baseline_cost, BAR_BASE),
        ("Optimized\n(75% spot + DRA)", report.optimized_cost, BAR_OPT),
    ]
    bw = 150
    gap = 120
    x = plot_x + 150
    for label, cost, color in bars:
        h = int(plot_h * cost / max_cost)
        top = plot_y + plot_h - h
        d.rectangle([x, top, x + bw, plot_y + plot_h], fill=color)
        d.text((x + bw // 2 - 26, top - 28), f"${cost:.2f}", font=f_big, fill=color)
        for j, line in enumerate(label.split("\n")):
            d.text((x + bw // 2 - 72, plot_y + plot_h + 14 + j * 20), line, font=f_small, fill=FG)
        x += bw + gap

    # подпись экономии
    d.line([(plot_x, plot_y + plot_h), (plot_x + plot_w, plot_y + plot_h)], fill=FG, width=2)
    d.text((60, 470), f"Экономия: {report.cost_savings_pct:.1f}% (требуется >= 40%)", font=f_big, fill=ACCENT)
    d.text((60, 500),
           f"Cost per job: ${report.baseline_cost_per_job:.2f} -> ${report.optimized_cost_per_job:.2f}   |   "
           f"Cost optimization factor: {report.cost_optimization_factor:.2f}x   |   "
           f"Доступность: {report.availability:.4%}",
           font=f_small, fill=FG)

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    img.save(OUT)
    return OUT


if __name__ == "__main__":
    path = build()
    print(f"График записан: {path}")
