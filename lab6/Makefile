# ЛР-6. Минимально работоспособный стенд (вариант 8).
# Все команды — из каталога lab6/.

.PHONY: help install demo up down logs load-test test metrics chart manifests \
        drift validate-data smoke k8s-plan cost clean

help:
	@echo "make install       — python-зависимости (fastapi, pandas, pyyaml)"
	@echo "make cost          — cost-модель варианта 8 (экономия >= 40%)"
	@echo "make chart         — график снижения затрат (work/cost_reduction.png)"
	@echo "make manifests     — офлайн-валидация K8s-манифестов"
	@echo "make validate-data — проверка качества данных (GX-совместимо)"
	@echo "make drift         — PSI data drift по срезу датасета"
	@echo "make demo          — весь офлайн-цикл: cost+chart+manifests+valid+drift+test"
	@echo "make up / down     — docker compose стенд (model server, Prometheus, Grafana)"
	@echo "make load-test     — нагрузочный тест внутри стенда"
	@echo "make test          — сводная проверка требований + ## METRICS ##"
	@echo "make k8s-plan      — что и как разворачивать в Kubernetes (kind)"

install:
	python -m pip install -r requirements.txt

cost:
	python resource_management/cost_model.py

chart:
	python resource_management/make_cost_chart.py

manifests:
	python tests/validate_manifests.py --dir kubernetes --strict-labels

validate-data:
	python tests/validate_data.py --path data/sample_ratings.csv \
		--suite config/great_expectations/suites/ratings_suite.json

drift:
	python tests/compute_drift.py

demo: cost chart manifests validate-data drift test

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f load-generator

# Режимы задаются переменными окружения (см. комментарий в docker-compose.yml):
#   make load-test            — normal
#   MODE=peak make load-test  — пиковая нагрузка (bash)
load-test:
	MODE=normal EVENTS=600 docker compose run --rm load-generator
	MODE=peak   EVENTS=600 docker compose run --rm load-generator
	MODE=fail   EVENTS=600 docker compose run --rm load-generator
	MODE=late   EVENTS=600 docker compose run --rm load-generator

test:
	python tests/check_requirements.py

metrics:
	python tests/metrics.py

smoke:
	python cicd/scripts/smoke_test.py --endpoint http://localhost:8000 --wait 30

k8s-plan:
	pwsh -File scripts/deploy.ps1 -DryRun

clean:
	rm -rf work/*.json work/*.png
