#!/usr/bin/env bash
# 部署 EDP 到 minikube 的 data 命名空间。
# 用法：./deploy/k8s/apply.sh   （在仓库根目录执行）
#
# 前置：
#   1. minikube 已启动
#   2. 镜像已构建并加载：
#        docker build -t edp:dev .
#        minikube image load edp:dev
#      基础设施镜像同理（postgres:16 / apache/kafka:3.8.0 / minio/* / tabulario/iceberg-rest:1.6.0）
set -euo pipefail
cd "$(dirname "$0")/../.."

NS=data

# 00-base：namespace / edp-env / PVC / RBAC（namespace 要先建，后面的 configmap 才有落点）
kubectl apply -f deploy/k8s/00-base.yaml

# 从源码目录生成配置类 configmap（避免 SQL/YAML 双份维护）
kubectl -n "$NS" create configmap postgres-init \
  --from-file=schemas/postgres_init/01-init-platform-db.sh \
  --from-file=schemas/postgres_platform.sql \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" create configmap dagster-instance \
  --from-file=dagster.yaml=deploy/k8s/dagster.yaml \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" create configmap dagster-workspace \
  --from-file=workspace.yaml=deploy/k8s/workspace.yaml \
  --dry-run=client -o yaml | kubectl apply -f -

# 10-infra：postgres / kafka / minio / iceberg-rest / opensearch(+dashboards)；
# 20-apps：dagster 三件套 + gateway
kubectl apply -f deploy/k8s/10-infra.yaml -f deploy/k8s/20-apps.yaml

# Argo Workflows：官方 namespace 级安装 + EDP 叠加（30-argo.yaml 必须在其后，
# 覆盖空的 workflow-controller-configmap）。镜像需预载：
#   quay.io/argoproj/{workflow-controller,argocli,argoexec}:v4.0.7
kubectl -n "$NS" apply --server-side -f deploy/k8s/argo/namespace-install-v4.0.7.yaml
kubectl apply -f deploy/k8s/30-argo.yaml -f deploy/k8s/31-argo-worker-template.yaml
# dev 环境免登录访问 Argo UI（生产应保留默认 client 认证）
kubectl -n "$NS" patch deploy argo-server --type=json -p \
  '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--auth-mode=server"}]' \
  >/dev/null 2>&1 || true

echo
echo "等待就绪：kubectl -n $NS get pods -w"
echo "Dagster UI：kubectl -n $NS port-forward svc/dagster-webserver 3000:3000"
echo "Gateway   ：kubectl -n $NS port-forward svc/gateway 8000:8000"
echo "Tag 检索界面（OpenSearch Dashboards）：kubectl -n $NS port-forward svc/opensearch-dashboards 5601:5601"
echo "MLflow UI ：kubectl -n $NS port-forward svc/mlflow 5000:5000"
echo "Argo UI   ：kubectl -n $NS port-forward svc/argo-server 2746:2746   （https，浏览器接受自签证书）"
echo "首次部署记得建 Iceberg 表："
echo "  kubectl -n $NS run init-iceberg --rm -it --restart=Never --image=edp:dev \\"
echo "    --overrides='{\"spec\":{\"containers\":[{\"name\":\"init-iceberg\",\"image\":\"edp:dev\",\"command\":[\"python\",\"-m\",\"schemas.iceberg_tables\"],\"envFrom\":[{\"configMapRef\":{\"name\":\"edp-env\"}}]}]}}'"
