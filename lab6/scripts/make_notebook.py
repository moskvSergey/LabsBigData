"""
Генератор ноутбука отчёта ЛР-6 (lab6.ipynb).
Запуск:  python scripts/make_notebook.py
"""

from __future__ import annotations

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)}


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": text.splitlines(keepends=True),
    }


CELLS = [
    md(
        "# Лабораторная работа 6. DevOps для Big Data\n"
        "## Развёртка, мониторинг и управление ресурсами\n\n"
        "**Вариант:** последняя цифра зачётки — **8**\n\n"
        "**Бизнес-сценарий:** рекомендательная система онлайн-кинотеатра "
        "(персонализированные рекомендации в реальном времени)\n\n"
        "**Датасет:** MovieLens ml-32m (из ЛР-1), срез `ratings_100mb.parquet` — 4 млн оценок\n\n"
        "**Уникальное требование по cost optimization:** снизить затраты **на 40%** "
        "через комбинацию on-demand и spot без ущерба для SLA\n\n"
        "**Требования сценария:** latency инференса < 100 мс · SLA доступности 99.5% · CTR +10%\n\n"
        "---\n\n"
        "Отчёт состоит из шести частей задания: контейнеризация, Kubernetes, "
        "управление ресурсами, CI/CD, мониторинг, тестирование. Все цифры "
        "воспроизводятся командами из ячеек ниже."
    ),
    md("## 0. Окружение и проверка зависимостей"),
    code(
        "import os, sys, json, subprocess\n"
        "from pathlib import Path\n"
        "\n"
        "ROOT = Path.cwd()\n"
        "if ROOT.name != 'lab6':\n"
        "    for cand in (ROOT / 'lab6', ROOT.parent / 'lab6'):\n"
        "        if cand.is_dir():\n"
        "            ROOT = cand\n"
        "            break\n"
        "os.chdir(ROOT)\n"
        "print('Каталог лабораторной:', ROOT)\n"
        "\n"
        "def run(cmd: str) -> str:\n"
        "    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=ROOT)\n"
        "    out = (proc.stdout or '') + (proc.stderr or '')\n"
        "    print(f'$ {cmd}\\n{out.strip()}\\n')\n"
        "    return out\n"
        "\n"
        "print('Python:', sys.version.split()[0])\n"
        "print('Файлов в lab6:', sum(1 for _ in ROOT.rglob('*') if _.is_file()))"
    ),
    md(
        "## 1. Часть 1. Контейнеризация Big Data-приложений (2.0 балла)\n\n"
        "Оптимизированные образы для всех компонентов стенда:\n\n"
        "| Компонент | Dockerfile | Приём |\n"
        "|---|---|---|\n"
        "| Spark | `spark/Dockerfile` | многоступенчатая сборка, `jre` вместо `jdk`, вырезаны `examples/`, `data/`, `docs/` |\n"
        "| Model Server | `model_server/Dockerfile` | venv из builder в `python:3.11-slim`, непривилегированный `appuser`, HEALTHCHECK |\n"
        "| Load Generator | `load_generator/Dockerfile` | slim-база, `--no-cache-dir`, общий `cost_model.py` |\n"
        "| Kafka / Redis / Grafana / Prometheus | официальные образы | `apache/kafka:3.9.0` (KRaft), `redis:7.2-alpine`, `grafana:11.1.0`, `prometheus:v2.53.0` |\n\n"
        "Ключевое для JVM в контейнере: `-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0` — "
        "иначе JVM видит память хоста, а не cgroup-лимит пода, и падает с `OOMKilled`."
    ),
    code(
        "run('python tests/validate_manifests.py --dir kubernetes --strict-labels --quiet')"
    ),
    code(
        "# Лимиты ресурсов в compose (Часть 1, п.3)\n"
        "import re\n"
        "compose = (ROOT / 'docker-compose.yml').read_text(encoding='utf-8')\n"
        "blocks = re.findall(r'^  (\\w[\\w-]*):$', compose, flags=re.M)\n"
        "print('Сервисы стенда:', ', '.join(blocks))\n"
        "res = re.findall(r'\\n\\s+(cpus|memory):\\s*(\\S+)', compose)\n"
        "print(f'Задано лимитов CPU/памяти: {len(res)}')\n"
        "for kind, value in res[:8]:\n"
        "    print(f'  {kind}: {value}')\n"
        "print('\\nmulti-stage в Dockerfile Spark:',\n"
        "      'AS builder' in (ROOT / 'spark' / 'Dockerfile').read_text(encoding='utf-8'))\n"
        "print('multi-stage в Dockerfile model server:',\n"
        "      (ROOT / 'model_server' / 'Dockerfile').read_text(encoding='utf-8').count('FROM') >= 2)"
    ),
    md(
        "## 2. Часть 2. Оркестрация с Kubernetes (2.5 балла)\n\n"
        "Манифесты лежат в `kubernetes/`: Spark Operator + `SparkApplication`, "
        "Flink Operator + `FlinkDeployment`, StatefulSet с PVC, KEDA, "
        "Cluster Autoscaler, GPU-профиль, staging/canary/production.\n\n"
        "Разделение пулов: **driver и JobManager — on-demand**, "
        "**executor'ы и основные реплики сервиса — spot** (tolerations + nodeSelector). "
        "Anti-affinity по `kubernetes.io/hostname` разносит executor'ы по узлам, "
        "чтобы вытеснение одного узла не убивало весь stage."
    ),
    code(
        "import yaml, glob\n"
        "kinds = {}\n"
        "for path in sorted(glob.glob('kubernetes/**/*.yaml', recursive=True)):\n"
        "    for doc in yaml.safe_load_all(open(path, encoding='utf-8')):\n"
        "        if isinstance(doc, dict) and 'kind' in doc:\n"
        "            kinds[doc['kind']] = kinds.get(doc['kind'], 0) + 1\n"
        "print('Объектов Kubernetes по типам:')\n"
        "for k, v in sorted(kinds.items()):\n"
        "    print(f'  {k}: {v}')\n"
        "print('Всего:', sum(kinds.values()))"
    ),
    code(
        "# Проверка стратегии spot/on-demand по манифестам\n"
        "spark = (ROOT / 'kubernetes' / 'manifests' / 'spark-app.yaml').read_text(encoding='utf-8')\n"
        "checks = {\n"
        "    'driver на on-demand': 'node-pool: on-demand' in spark,\n"
        "    'executor на spot': 'node-pool: spot' in spark,\n"
        "    'spot-toleration': 'key: \"spot\"' in spark,\n"
        "    'anti-affinity executor': 'podAntiAffinity' in spark,\n"
        "    'dynamic allocation': 'spark.dynamicAllocation.enabled' in spark,\n"
        "    'restartPolicy OnFailure': 'onFailureRetries' in spark,\n"
        "}\n"
        "for name, ok in checks.items():\n"
        "    print(f\"  [{'OK' if ok else 'FAIL'}] {name}\")"
    ),
    md(
        "Для локальной проверки в кластере есть скрипт `scripts/deploy.ps1`: он создаёт "
        "kind-кластер с двумя пулами узлов (`on-demand` и `spot` с taint "
        "`spot=true:NoSchedule`), собирает образ и применяет манифесты."
    ),
    code(
        "import shutil\n"
        "if shutil.which('pwsh'):\n"
        "    run('pwsh -File scripts/deploy.ps1 -DryRun')\n"
        "else:\n"
        "    print('pwsh недоступен в этом окружении — план развёртывания:')\n"
        "    print('  kind create cluster --config kubernetes/kind-cluster.yaml')\n"
        "    print('  kubectl label node lab6-worker node-pool=on-demand')\n"
        "    print('  kubectl label node lab6-worker2 node-pool=spot')\n"
        "    print('  kubectl taint node lab6-worker2 spot=true:NoSchedule')\n"
        "    print('  docker build -t lab6-model-server:1.0 model_server')\n"
        "    print('  kind load docker-image lab6-model-server:1.0 --name lab6')\n"
        "    print('  kubectl apply -f kubernetes/manifests/flink-app.yaml')\n"
        "    print('  kubectl apply -f kubernetes/deploy/production.yaml')\n"
        "    print('  kubectl apply -f kubernetes/manifests/spark-app.yaml')\n"
        "    print('  kubectl apply --dry-run=client -f kubernetes/spark-operator/spark-operator.yaml')"
    ),
    md(
        "## 3. Часть 3. Управление ресурсами и cost optimization (2.0 балла)\n\n"
        "### Стратегия варианта 8: on-demand + spot\n\n"
        "| Слой | Пул | Почему |\n"
        "|---|---|---|\n"
        "| Spark driver, Flink JobManager | on-demand | потеря драйвера = потеря всей задачи |\n"
        "| Spark executor | spot | stateless, вытеснение лечится пересчётом stage |\n"
        "| Model server (6 реплик) | spot + anti-affinity | вытеснение снимает 1/6 мощности |\n"
        "| Model server (1 якорь) | on-demand | страховка SLA |\n"
        "| Батч-задачи | spot через KEDA | нет данных — нет расходов |\n\n"
        "Cost-модель (`resource_management/cost_model.py`) считает: baseline 8 executor-часов "
        "на `c5.4xlarge` ($0.768/ч) против смеси 75% spot ($0.2688/ч) с overhead +5% "
        "на пересчёт упавших stage."
    ),
    code("run('python resource_management/cost_model.py')"),
    code(
        "import json\n"
        "report = json.loads(run('python resource_management/cost_model.py').strip())\n"
        "print(f\"Экономия:        {report['cost_savings_pct']:.2f}%  (требуется >= 40%)\")\n"
        "print(f\"Cost per job:    ${report['baseline_cost_per_job']:.2f} -> ${report['optimized_cost_per_job']:.2f}\")\n"
        "print(f\"Factor:          {report['cost_optimization_factor']:.2f}x\")\n"
        "print(f\"За месяц:        ${report['baseline_cost_total']:.2f} -> ${report['optimized_cost_total']:.2f} \"\n"
        "      f\"(экономия ${report['saved_total']:.2f})\")\n"
        "print(f\"Доступность:     {report['availability']:.6f}\")\n"
        "print(f\"Доля spot:       {report['spot_fraction']:.0%}\")"
    ),
    code("run('python resource_management/make_cost_chart.py')"),
    code(
        "from IPython.display import Image, display\n"
        "display(Image(filename='work/cost_reduction.png'))"
    ),
    md(
        "### Остальные механики Части 3\n\n"
        "| Механика | Реализация |\n"
        "|---|---|\n"
        "| YARN Capacity Scheduler | очереди `etl` 60%, `ml` 30%, `default` 10% + `maximum-capacity` |\n"
        "| Dynamic allocation | 2…20 executor'ов, `shuffleTracking`, AQE |\n"
        "| KEDA | ScaledObject по Kafka-lag, `lagThreshold=1000` |\n"
        "| Cluster Autoscaler | `scaleDownUnneededTime=30m`, `scaleDownUtilizationThreshold=0.5` |\n"
        "| Размеры инстансов | cpu-bound `c5.4xlarge`, memory-bound `r5.4xlarge` |\n"
        "| GPU-профиль | `nvidia.com/gpu: 1`, `spark.plugins`, `spark.task.resource.gpu.amount=0.25` |"
    ),
    code(
        "yarn = (ROOT / 'resource_management' / 'yarn' / 'capacity-scheduler.xml').read_text(encoding='utf-8')\n"
        "for q in ('default,etl,ml', 'root.etl.capacity', 'root.ml.capacity', 'maximum-capacity'):\n"
        "    print(f\"  [{'OK' if q in yarn else 'FAIL'}] {q}\")\n"
        "\n"
        "gpu = (ROOT / 'kubernetes' / 'spark-operator' / 'spark-operator.yaml').read_text(encoding='utf-8')\n"
        "print('\\nGPU-профиль:')\n"
        "for q in ('nvidia.com/gpu', 'com.nvidia.spark.SQLPlugin', 'spark.task.resource.gpu.amount'):\n"
        "    print(f\"  [{'OK' if q in gpu else 'FAIL'}] {q}\")\n"
        "print('\\nБез spark.plugins ускорения не будет: код останется на CPU-пути, карта простаивает.')\n"
        "QUERY_SECONDS, INIT_SECONDS, GPU_HOUR_COST = 45, 2.5, 0.0\n"
        "print(f'Окупаемость GPU: utilization={QUERY_SECONDS/(QUERY_SECONDS+INIT_SECONDS):.1%}, '\n"
        "      f'break_even={QUERY_SECONDS >= 30}')"
    ),
    md(
        "## 4. Часть 4. CI/CD для данных и моделей (1.5 балла)\n\n"
        "Три гейта в `cicd/.github/workflows/ci.yml`: `validate` (качество данных + "
        "манифесты) → `test` (unit-тесты + **cost-гейт 40%**) → `deploy` "
        "(canary → smoke → метрики → promote или автооткат).\n\n"
        "Версионирование данных — DVC (`dvc/dvc.yaml`), версионирование таблиц — "
        "Delta Lake (`cicd/delta/ratings_delta.sql`, включая `RESTORE TABLE` как откат)."
    ),
    code(
        "print('--- Data CI/CD (ci.yml) ---')\n"
        "ci = (ROOT / 'cicd' / '.github' / 'workflows' / 'ci.yml').read_text(encoding='utf-8')\n"
        "for job in ('validate:', 'test:', 'deploy:'):\n"
        "    print(f\"  [{'OK' if job in ci else 'FAIL'}] job {job}\")\n"
        "print('  [%s] cost-гейт 40%%' % ('OK' if 'cost_savings_pct' in ci and '40' in ci else 'FAIL'))\n"
        "\n"
        "print('\\n--- Delta Lake ---')\n"
        "ddl = (ROOT / 'cicd' / 'delta' / 'ratings_delta.sql').read_text(encoding='utf-8')\n"
        "for op in ('USING DELTA', 'MERGE INTO', 'DESCRIBE HISTORY', 'RESTORE TABLE', 'OPTIMIZE', 'VACUUM'):\n"
        "    print(f\"  [{'OK' if op in ddl else 'FAIL'}] {op}\")\n"
        "\n"
        "print('\\n--- Безопасный деплой ---')\n"
        "for name in ('cicd/scripts/smoke_test.py', 'cicd/scripts/check_metrics.py', 'cicd/scripts/rollback.sh'):\n"
        "    print(f\"  [{'OK' if (ROOT / name).exists() else 'FAIL'}] {name}\")"
    ),
    code(
        "# Валидация данных по expectation suite (Great Expectations-совместимо)\n"
        "run('python tests/validate_data.py --path data/sample_ratings.csv '\n"
        "    '--suite config/great_expectations/suites/ratings_suite.json')"
    ),
    md(
        "## 5. Часть 5. Мониторинг и алертинг (1.5 балла)\n\n"
        "Три уровня наблюдаемости:\n\n"
        "1. **Инфраструктура** — kubelet/node-exporter через `kubernetes_sd_configs` "
        "+ kube-state-metrics (поды, узлы, spot-вытеснения);\n"
        "2. **Приложение** — `/metrics` model server (p50/p95/p99, RPS, ошибки), "
        "Spark PrometheusServlet (shuffle spill), Flink reporter (checkpoint duration);\n"
        "3. **Данные** — PSI-дрейф, schema compliance, объём DLQ.\n\n"
        "13 алертов в 4 группах: SLA, cost, качество данных, инфраструктура. "
        "У каждого — severity, runbook-метка и описание действия."
    ),
    code(
        "import yaml\n"
        "\n"
        "alerts = yaml.safe_load((ROOT / 'monitoring' / 'prometheus' / 'alerts.yml').read_text(encoding='utf-8'))\n"
        "print('Алерты Prometheus:')\n"
        "total = 0\n"
        "for group in alerts['groups']:\n"
        "    names = [r['alert'] for r in group['rules']]\n"
        "    total += len(names)\n"
        "    print(f\"  {group['name']}: {len(names)} -> {', '.join(names)}\")\n"
        "print('Всего правил:', total)\n"
        "\n"
        "print('\\nДашборды Grafana:')\n"
        "for dash in ('infrastructure', 'application', 'data'):\n"
        "    spec = json.loads((ROOT / 'monitoring' / 'grafana' / 'dashboards' / f'{dash}.json').read_text(encoding='utf-8'))\n"
        "    titles = [p['title'] for p in spec['panels']]\n"
        "    print(f\"  {spec['title']}: {len(spec['panels'])} панелей\")\n"
        "    for t in titles:\n"
        "        print(f'     - {t}')"
    ),
    md(
        "## 6. Часть 6. Тестирование и валидация\n\n"
        "Генератор нагрузки (`load_generator/generator.py`) умеет четыре режима: "
        "`normal` (100 соб/с), `peak` (200 соб/с), `fail` (вытеснение узла) и "
        "`late` (5% событий с `event_time` на 6 минут в прошлом)."
    ),
    code(
        "gen_text = (ROOT / 'load_generator' / 'generator.py').read_text(encoding='utf-8')\n"
        "print('Режимы генератора нагрузки (из argparse choices):')\n"
        "for mode in ('normal', 'peak', 'fail', 'late'):\n"
        "    ok = f'\"{mode}\"' in gen_text\n"
        "    print(f\"  [{'OK' if ok else 'FAIL'}] {mode}\")\n"
        "print('\\nВалидация K8s-манифестов без кластера:')\n"
        "run('python tests/validate_manifests.py --dir kubernetes --strict-labels --quiet')"
    ),
    code(
        "# PSI data drift по реальному срезу датасета\n"
        "run('python tests/compute_drift.py')\n"
        "# И проверка, что алерт не «мёртвый»: инжектируем дрейф и возвращаем норму\n"
        "run('python tests/compute_drift.py --inject-drift 0.8')\n"
        "run('python tests/compute_drift.py')"
    ),
    code(
        "# Сводная проверка всех требований + фиксированные метрики\n"
        "out = run('python tests/check_requirements.py')\n"
        "print('\\n'.join(l for l in out.splitlines() if 'METRICS' in l or '=' in l and l.split('=')[0].isupper())[-1200:])"
    ),
    code(
        "# Тесты уникального требования и SLA\n"
        "run('python tests/test_cost.py')\n"
        "run('python tests/test_sla.py')"
    ),
    md(
        "## 7. Итоги\n\n"
        "| Требование | Цель | Факт | Статус |\n"
        "|---|---|---|---|\n"
        "| Экономия затрат (вариант 8) | ≥ 40% | 41.06% | ✅ |\n"
        "| Cost per job | < baseline | $4.07 против $6.91 | ✅ |\n"
        "| Cost optimization factor | > 1 | 1.70× | ✅ |\n"
        "| Доступность (SLA) | ≥ 99.5% | 99.999% | ✅ |\n"
        "| Latency инференса p99 | < 100 мс | 4.9 мс end-to-end / 0.06 мс в процессе | ✅ |\n"
        "| Schema compliance | ≥ 0.99 | 1.00 | ✅ |\n"
        "| Data drift (PSI) | < 0.2 | 0.0022 | ✅ |\n"
        "| Автопроверки | 48 | 48/48 | ✅ |\n\n"
        "**Что работает:** кластер описан кодом (29 объектов, операторы Spark/Flink, "
        "KEDA + HPA + Cluster Autoscaler), уникальное требование выполнено с запасом "
        "при сохранении SLA, мониторинг на трёх уровнях, CI/CD с canary и автооткатом.\n\n"
        "**Ограничения:** цифры cost-модели расчётные (прайс `c5.4xlarge`), фактический "
        "процент вытеснений на учебном стенде не воспроизводится; GPU-профиль "
        "проверяется манифестом (нет NVIDIA-карты — это разрешено заданием); "
        "Cluster Autoscaler и KEDA проверены конфигурационно."
    ),
]


def main() -> None:
    nb = {
        "cells": CELLS,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.12"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    out = os.path.join(ROOT, "lab6.ipynb")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(nb, fh, ensure_ascii=False, indent=1)
    print(f"Ноутбук записан: {out} ({len(CELLS)} ячеек)")


if __name__ == "__main__":
    main()
