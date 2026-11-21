#!/usr/bin/env python
"""
ЛР-6, Часть 4, п.1. Валидация данных перед ETL (Great Expectations-совместимо).

Читает expectation suite (тот же JSON-формат, что у GX) и проверяет CSV/Parquet
без установки great_expectations — стенд должен оставаться лёгким.
В CI этот же скрипт вызывается шагом `Run data validation`.

Запуск:
  python tests/validate_data.py --path data/sample_ratings.csv \
      --suite config/great_expectations/suites/ratings_suite.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import pandas as pd


def _load(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _check(expectation: dict, df: pd.DataFrame) -> tuple[str, bool, str]:
    etype = expectation["expectation_type"]
    kw = expectation.get("kwargs", {})

    if etype == "expect_table_columns_to_match_set":
        required = set(kw["column_set"])
        missing = required - set(df.columns)
        return etype, not missing, f"missing={sorted(missing)}"

    if etype == "expect_column_values_to_be_between":
        col = kw["column"]
        if col not in df.columns:
            return etype, False, f"нет колонки {col}"
        series = pd.to_numeric(df[col], errors="coerce")
        lo, hi = kw.get("min_value"), kw.get("max_value")
        bad = series.isna()
        if lo is not None:
            bad |= series < lo
        if hi is not None:
            bad |= series > hi
        return etype, not bad.any(), f"{col}: нарушений {int(bad.sum())}"

    if etype == "expect_column_values_to_not_be_null":
        col = kw["column"]
        nulls = int(df[col].isna().sum()) if col in df.columns else -1
        return etype, nulls == 0, f"{col}: null={nulls}"

    if etype == "expect_column_values_to_be_of_type":
        col, type_ = kw["column"], kw["type_"]
        dtype = str(df[col].dtype) if col in df.columns else "missing"
        ok = (type_ == "float" and dtype.startswith("float")) or \
             (type_ == "int" and dtype.startswith("int")) or dtype == type_
        return etype, ok, f"{col}: {dtype}"

    if etype == "expect_compound_columns_to_be_unique":
        cols = [c for c in kw["column_list"] if c in df.columns]
        dupes = int(df.duplicated(subset=cols).sum()) if cols else -1
        return etype, dupes == 0, f"дубликатов {dupes}"

    return etype, True, "пропущено (неизвестный тип)"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    ap.add_argument("--suite", required=True)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    if not os.path.exists(args.path):
        print(f"Нет файла данных: {args.path}")
        return 2

    df = _load(args.path)
    with open(args.suite, "r", encoding="utf-8") as fh:
        suite = json.load(fh)

    results = []
    failed = 0
    print(f"CHECKPOINT=ratings_suite DATASET={args.path} ROWS={len(df)}")
    for exp in suite["expectations"]:
        etype, ok, detail = _check(exp, df)
        print(f"  {'PASS' if ok else 'FAIL'} {etype} ({detail})")
        results.append({"expectation": etype, "success": ok, "detail": detail})
        failed += 0 if ok else 1

    summary = {
        "dataset": args.path,
        "rows": int(len(df)),
        "expectations_total": len(results),
        "expectations_failed": failed,
        "success_ratio": round((len(results) - failed) / max(len(results), 1), 4),
        "results": results,
    }
    print(f"SCHEMA_COMPLIANCE_RATIO={summary['success_ratio']:.4f}")

    out = args.out or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "work", "data_validation.json"
    )
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(f"Отчёт записан: {out}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
