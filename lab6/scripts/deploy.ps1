# ЛР-6. Развёртывание в Kubernetes (kind) — скрипт для Windows-стенда.
#
# Что делает:
#   1. проверяет docker/kubectl;
#   2. создаёт kind-кластер с двумя пулами узлов: on-demand (control-plane +
#      worker) и spot (worker с taint spot=true:NoSchedule);
#   3. помечает узлы и ставит taint'ы, под которые написаны манифесты;
#   4. собирает образ model-server и загружает его в kind;
#   5. применяет манифесты (онлайн-валидация apiVersion/схемы);
#   6. печатает статус подов.
#
# Реальный GPU не требуется: манифест kubernetes/spark-operator/spark-operator.yaml
# проверяется через kubectl --dry-run=client (Часть 3, п.7 задания).
#
# Использование:
#   pwsh -File scripts/deploy.ps1                 # развернуть кластер
#   pwsh -File scripts/deploy.ps1 -DryRun         # только показать план
#   pwsh -File scripts/deploy.ps1 -Cleanup        # удалить кластер

param(
    [switch]$DryRun,
    [switch]$Cleanup,
    [string]$ClusterName = "lab6"
)

$ErrorActionPreference = "Stop"
# Каталог lab6: от расположения самого скрипта, с откатом на текущий каталог
# (нужен, когда скрипт выполняется как scriptblock или через stdin).
$LabRoot = $PSScriptRoot
if (-not $LabRoot) {
    if ($MyInvocation.MyCommand.Path) { $LabRoot = Split-Path -Parent $MyInvocation.MyCommand.Path }
    else { $LabRoot = (Get-Location).Path }
}
$KindExe = Join-Path $env:USERPROFILE ".dsh\bin\kind.exe"

function Require-Command($name) {
    $cmd = Get-Command $name -ErrorAction SilentlyContinue
    if (-not $cmd) { throw "Не найдена команда '$name'. Установите её и повторите." }
    return $cmd.Source
}

Write-Host "== ЛР-6: развёртывание в Kubernetes (kind) ==" -ForegroundColor Cyan
$docker = Require-Command docker
$kubectl = Require-Command kubectl
if (-not (Test-Path $KindExe)) {
    # PowerShell 5.1 не поддерживает присваивание if-выражения, поэтому так.
    $kindCmd = Get-Command kind -ErrorAction SilentlyContinue
    $KindExe = ""
    if ($kindCmd) { $KindExe = $kindCmd.Source }
}
if (-not $KindExe -or -not (Test-Path $KindExe)) {
    Write-Host "kind не найден. Установка: https://kind.sigs.k8s.io/docs/user/quick-start/#installation" -ForegroundColor Yellow
    Write-Host "Либо включите Kubernetes в Docker Desktop и примените манифесты напрямую:" -ForegroundColor Yellow
    Write-Host "  kubectl apply -f kubernetes/manifests/" -ForegroundColor Yellow
    if (-not $DryRun) { throw "kind недоступен" }
}

$configPath = Join-Path $LabRoot "kubernetes\kind-cluster.yaml"

if ($Cleanup) {
    Write-Host "Удаляю кластер $ClusterName..."
    & $KindExe delete cluster --name $ClusterName
    return
}

if ($DryRun) {
    Write-Host "[dry-run] kind create cluster --config $configPath" -ForegroundColor Gray
    Write-Host "[dry-run] kubectl label node lab6-worker node-pool=on-demand" -ForegroundColor Gray
    Write-Host "[dry-run] kubectl label node lab6-worker2 node-pool=spot" -ForegroundColor Gray
    Write-Host "[dry-run] kubectl taint node lab6-worker2 spot=true:NoSchedule" -ForegroundColor Gray
    Write-Host "[dry-run] docker build -t lab6-model-server:1.0 model_server" -ForegroundColor Gray
    Write-Host "[dry-run] kind load docker-image lab6-model-server:1.0 --name $ClusterName" -ForegroundColor Gray
    Write-Host "[dry-run] kubectl apply -f kubernetes/manifests/spark-app.yaml" -ForegroundColor Gray
    Write-Host "[dry-run] kubectl apply -f kubernetes/manifests/flink-app.yaml" -ForegroundColor Gray
    Write-Host "[dry-run] kubectl apply -f kubernetes/deploy/production.yaml" -ForegroundColor Gray
    Write-Host "[dry-run] kubectl apply --dry-run=client -f kubernetes/spark-operator/spark-operator.yaml" -ForegroundColor Gray
    return
}

Write-Host "-- 1/6 создаю kind-кластер (два пула: on-demand и spot)"
$existing = @(& $KindExe get clusters 2>$null | Where-Object { $_ -eq $ClusterName })
if ($existing.Count -gt 0) {
    Write-Host "   кластер $ClusterName уже существует, пропускаю создание"
} else {
    & $KindExe create cluster --config $configPath
}

Write-Host "-- 2/6 маркирую узлы под spot/on-demand"
kubectl label node "$ClusterName-worker"  node-pool=on-demand --overwrite
kubectl label node "$ClusterName-worker2" node-pool=spot --overwrite
kubectl taint node "$ClusterName-worker2" spot=true:NoSchedule --overwrite
kubectl label node "$ClusterName-control-plane" node-pool=on-demand --overwrite

Write-Host "-- 3/6 собираю образ model-server"
docker build -t lab6-model-server:1.0 (Join-Path $LabRoot "model_server")

Write-Host "-- 4/6 загружаю образ в kind"
& $KindExe load docker-image lab6-model-server:1.0 --name $ClusterName

Write-Host "-- 5/6 применяю манифесты"
kubectl apply -f (Join-Path $LabRoot "kubernetes\manifests\flink-app.yaml")
kubectl apply -f (Join-Path $LabRoot "kubernetes\keda\scaledobject.yaml")
kubectl apply -f (Join-Path $LabRoot "kubernetes\deploy\production.yaml")
kubectl apply -f (Join-Path $LabRoot "kubernetes\manifests\spark-app.yaml")
# GPU-профиль: реальный GPU не нужен, проверяем манифест клиентски
kubectl apply --dry-run=client -f (Join-Path $LabRoot "kubernetes\spark-operator\spark-operator.yaml")

Write-Host "-- 6/6 состояние кластера"
kubectl get nodes -L node-pool
kubectl get pods -A

Write-Host ""
Write-Host "Готово. Полезное:" -ForegroundColor Green
Write-Host "  kubectl -n lab6-prod get pods -o wide      # реплики на spot/on-demand"
Write-Host "  kubectl -n lab6-prod port-forward svc/model-server 8000:80"
Write-Host "  kubectl apply -f kubernetes/deploy/canary.yaml   # канареечный деплой"
Write-Host "  pwsh -File scripts/deploy.ps1 -Cleanup           # удалить кластер"
