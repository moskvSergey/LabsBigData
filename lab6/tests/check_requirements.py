"""
ЛР-6, Часть 6. Сводная автоматическая проверка всех требований.

Проверяет по частям задания: контейнеризация, K8s-манифесты, управление
ресурсами и cost, CI/CD, мониторинг, тесты. В конце печатает фиксированные
метрики в блоке ## METRICS ## (требование к формату отчёта).

Запуск:  python tests/check_requirements.py
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import common  # noqa: E402

K8S = os.environ.get("K8S_DIR") or os.path.join(ROOT, "kubernetes")


def _exists(*parts: str) -> bool:
    return os.path.exists(os.path.join(ROOT, *parts))


def _glob_count(pattern: str) -> int:
    return len(glob.glob(os.path.join(ROOT, pattern), recursive=True))


def check_containers() -> list[tuple[str, bool, str]]:
    checks = [
        ("Dockerfile model server", _exists("model_server", "Dockerfile"), "Часть 1"),
        ("Dockerfile Spark", _exists("spark", "Dockerfile"), "Часть 1"),
        ("Dockerfile load generator", _exists("load_generator", "Dockerfile"), "Часть 1"),
        ("docker-compose.yml", _exists("docker-compose.yml"), "Часть 1"),
    ]
    compose = ""
    path = os.path.join(ROOT, "docker-compose.yml")
    if os.path.exists(path):
        compose = open(path, "r", encoding="utf-8").read()
    checks += [
        ("multi-stage build (многоступенчатая сборка)",
         "AS builder" in open(os.path.join(ROOT, "model_server", "Dockerfile"), encoding="utf-8").read()
         if _exists("model_server", "Dockerfile") else False, "Часть 1, п.2"),
        ("лимиты ресурсов в compose (limits/reservations)",
         "limits:" in compose and "reservations:" in compose, "Часть 1, п.3"),
        ("лимиты памяти и CPU для компонентов",
         compose.count("cpus:") >= 3 and compose.count("memory:") >= 3, "Часть 1, п.3"),
    ]
    return checks


def check_kubernetes() -> list[tuple[str, bool, str]]:
    spark = _read(os.path.join(K8S, "manifests", "spark-app.yaml"))
    flink = _read(os.path.join(K8S, "manifests", "flink-app.yaml"))
    keda = _read(os.path.join(K8S, "keda", "scaledobject.yaml"))
    operator = _read(os.path.join(K8S, "spark-operator", "spark-operator.yaml"))
    return [
        ("Spark Operator / SparkApplication", "SparkApplication" in spark, "Часть 2, п.1"),
        ("Spark driver на on-demand, executor на spot",
         "node-pool: on-demand" in spark and "node-pool: spot" in spark, "Часть 2"),
        ("Spot-toleration в SparkApplication", 'key: "spot"' in spark, "Часть 3, п.5"),
        ("Anti-affinity executor'ов",
         "podAntiAffinity" in spark, "Часть 2, п.2"),
        ("Flink Operator / FlinkDeployment", "FlinkDeployment" in flink, "Часть 2, п.1"),
        ("StatefulSet с PVC для TaskManager'ов",
         "kind: StatefulSet" in flink and "volumeClaimTemplates" in flink, "Часть 2, п.3"),
        ("KEDA ScaledObject по lag в Kafka", "ScaledObject" in keda and "lagThreshold" in keda, "Часть 3, п.3"),
        ("Cluster Autoscaler ConfigMap", "cluster-autoscaler-config" in keda, "Часть 3, п.4"),
        ("GPU-профиль запуска", "nvidia.com/gpu" in operator, "Часть 3, п.7"),
        ("spark.plugins=com.nvidia.spark.SQLPlugin",
         "com.nvidia.spark.SQLPlugin" in operator, "Часть 3, п.7"),
        ("spark.task.resource.gpu.amount задан",
         "spark.task.resource.gpu.amount" in operator, "Часть 3, п.7"),
    ]


def check_resource_management() -> list[tuple[str, bool, str]]:
    yarn = _read(os.path.join(ROOT, "resource_management", "yarn", "capacity-scheduler.xml"))
    dyn = _read(os.path.join(ROOT, "resource_management", "spark", "dynamic-allocation.conf"))
    report = common.cost_model.simulate()
    return [
        ("YARN capacity-scheduler: очереди default/etl/ml",
         all(q in yarn for q in (">default,etl,ml<", "root.etl.capacity", "root.ml.capacity")), "Часть 3, п.1"),
        ("dynamic allocation включён", "spark.dynamicAllocation.enabled=true" in dyn, "Часть 3, п.2"),
        ("memory fraction настроен", "spark.memory.fraction=0.6" in dyn, "Часть 3, п.6"),
        ("экономия затрат >= 40% (вариант 8)", report.cost_savings_pct >= 40,
         f"Часть 3, п.6 (получено {report.cost_savings_pct:.2f}%)"),
        ("cost optimization factor > 1.5", report.cost_optimization_factor > 1.5,
         f"Часть 3 (получено {report.cost_optimization_factor:.2f}x)"),
        ("график снижения затрат", _exists("work", "cost_reduction.png"), "Часть 3, отчёт"),
    ]


def check_cicd() -> list[tuple[str, bool, str]]:
    ci = _read(os.path.join(ROOT, "cicd", ".github", "workflows", "ci.yml"))
    deploy = _read(os.path.join(ROOT, "cicd", ".github", "workflows", "deploy.yml"))
    return [
        ("GitHub Actions CI (validate/test/deploy)", all(j in ci for j in ("validate:", "test:", "deploy:")),
         "Часть 4, п.1"),
        ("Data validation в CI (Great Expectations)",
         "great_expectations" in ci or "validate_data.py" in ci, "Часть 4, п.1"),
        ("DVC-конфигурация", _exists("dvc", "dvc.yaml") and _exists("dvc", "config"), "Часть 4, п.2"),
        ("Delta Lake версионирование", _exists("cicd", "delta", "ratings_delta.sql"), "Часть 4, п.3"),
        ("Canary release", "canary" in ci.lower() or _exists("kubernetes", "deploy", "canary.yaml"),
         "Часть 4, п.4"),
        ("Автоматический откат", "ollback" in deploy and _exists("cicd", "scripts", "rollback.sh"),
         "Часть 4, п.4"),
        ("Smoke tests", _exists("cicd", "scripts", "smoke_test.py"), "Часть 4, п.4"),
        ("Сравнение версий по метрикам", _exists("cicd", "scripts", "check_metrics.py"), "Часть 4, п.4"),
    ]


def check_monitoring() -> list[tuple[str, bool, str]]:
    prom = _read(os.path.join(ROOT, "monitoring", "prometheus", "prometheus.yml"))
    alerts = _read(os.path.join(ROOT, "monitoring", "prometheus", "alerts.yml"))
    return [
        ("Prometheus: инфраструктурный уровень",
         "kubernetes_sd_configs" in prom and "kubernetes-nodes" in prom, "Часть 5, п.1"),
        ("Prometheus: уровень приложения", "model-server" in prom, "Часть 5, п.1"),
        ("Алерты: spill и drift",
         "HighShuffleSpill" in alerts and "HighDataDrift" in alerts, "Часть 5, п.2"),
        ("Алерты SLA и cost", "HighInferenceLatencyP99" in alerts and "CostSavingsBelowTarget" in alerts,
         "Часть 5, п.2"),
        ("Дашборд 1: инфраструктура", _exists("monitoring", "grafana", "dashboards", "infrastructure.json"),
         "Часть 5, п.3"),
        ("Дашборд 2: приложение", _exists("monitoring", "grafana", "dashboards", "application.json"),
         "Часть 5, п.3"),
        ("Дашборд 3: качество данных", _exists("monitoring", "grafana", "dashboards", "data.json"),
         "Часть 5, п.3"),
        ("Интеграция мониторинга с CI/CD", _exists("cicd", "scripts", "check_metrics.py"), "Часть 5, п.4"),
    ]


def check_testing() -> list[tuple[str, bool, str]]:
    return [
        ("Генератор нагрузки", _exists("load_generator", "generator.py"), "Часть 6, п.1"),
        ("Режимы normal/peak/fail/late",
         all(m in _read(os.path.join(ROOT, "load_generator", "generator.py"))
             for m in ('"normal"', '"peak"', '"fail"', '"late"')), "Часть 6, п.2"),
        ("Проверка cost optimization", _exists("tests", "test_cost.py"), "Часть 6, п.3"),
        ("Проверка SLA", _exists("tests", "test_sla.py"), "Часть 6, п.3"),
        ("Валидация K8s-манифестов", _exists("tests", "validate_manifests.py"), "Часть 6, п.3"),
        ("Валидация данных (GX-совместимо)", _exists("tests", "validate_data.py"), "Часть 4, п.1"),
        ("README.md с вариантом и инструкцией", _exists("README.md"), "Оформление"),
    ]


def _read(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def run_k8s_validation() -> tuple[bool, str]:
    script = os.path.join(HERE, "validate_manifests.py")
    proc = subprocess.run(
        [sys.executable, script, "--dir", K8S, "--strict-labels", "--kubectl", "off"],
        capture_output=True, text=True, cwd=ROOT,
    )
    tail = [ln for ln in proc.stdout.splitlines() if ln.startswith(("MANIFEST_ERRORS", "SPOT_", "GPU_"))]
    return proc.returncode == 0, "; ".join(tail)


def print_metrics(m: dict) -> None:
    print("## METRICS ##")
    print(f"REQUIRED_COST_SAVINGS={m['required_cost_savings']:g}")
    print(f"ACHIEVED_COST_SAVINGS={m['achieved_cost_savings']:.2f}")
    print(f"SYSTEM_AVAILABILITY={m['availability']:.6f}")
    print(f"COST_PER_JOB={m['cost_per_job']:.4f}")
    print(f"DATA_DRIFT_SCORE={m['data_drift_score']:.4f}")
    print(f"DATA_DRIFT_SCORE_LIVE={m['data_drift_score_live']:.4f}")
    print(f"BASELINE_COST_PER_JOB={m['baseline_cost_per_job']:.4f}")
    print(f"COST_OPTIMIZATION_FACTOR={m['cost_optimization_factor']:.3f}")
    print(f"SPOT_FRACTION={m['spot_fraction']:.2f}")
    print(f"REQUIRED_LATENCY_MS={m['required_latency_ms']:g}")
    print(f"MEASURED_P99_LATENCY_MS={m['inference_p99_ms']:.3f}")
    print(f"THROUGHPUT_EPS={m['throughput_eps']:.2f}")
    print(f"SCHEMA_COMPLIANCE={m['schema_compliance']:.4f}")
    print(f"SLA_OK={m['availability'] >= m['required_availability']}")
    print(f"COST_OK={m['achieved_cost_savings'] >= m['required_cost_savings']}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", default="", help="каталог с метриками (по умолчанию work/)")
    args = ap.parse_args()
    if args.work_dir:
        os.environ["WORK_DIR"] = os.path.abspath(args.work_dir)

    groups = [
        ("1. Контейнеризация", check_containers()),
        ("2. Kubernetes", check_kubernetes()),
        ("3. Управление ресурсами и cost", check_resource_management()),
        ("4. CI/CD для данных", check_cicd()),
        ("5. Мониторинг и алертинг", check_monitoring()),
        ("6. Тестирование и валидация", check_testing()),
    ]

    total = failed = 0
    results = []
    for title, checks in groups:
        print(f"\n=== {title} ===")
        for name, ok, where in checks:
            total += 1
            failed += 0 if ok else 1
            print(f"  [{'OK  ' if ok else 'FAIL'}] {name}  ({where})")
            results.append({"group": title, "check": name, "ok": bool(ok), "where": where})

    ok, detail = run_k8s_validation()
    total += 1
    failed += 0 if ok else 1
    print(f"\n=== 7. Офлайн-валидация манифестов ===")
    print(f"  [{'OK  ' if ok else 'FAIL'}] YAML/apiVersion/spot/GPU  ({detail})")
    results.append({"group": "7. Манифесты", "check": "offline validation", "ok": bool(ok), "where": detail})

    metrics = common.load_metrics()
    print_metrics(metrics)

    summary = {
        "checks_total": total,
        "checks_failed": failed,
        "success_ratio": round((total - failed) / max(total, 1), 4),
        "metrics": metrics,
        "results": results,
    }
    out = os.path.join(common.WORK, "check_requirements.json")
    os.makedirs(common.WORK, exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    try:
        common.write_cost_metrics()
    except Exception as exc:
        print(f"не удалось обновить cost_model.json: {exc}")

    print(f"\nCHECKS_TOTAL={total} CHECKS_FAILED={failed} SUCCESS_RATIO={summary['success_ratio']:.4f}")
    print(f"Отчёт: {out}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
