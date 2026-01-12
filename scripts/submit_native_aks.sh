#!/usr/bin/env bash
set -euo pipefail

AKS_API_SERVER="https://medallinks-rg-spark-medalli-0ccc71-aoqabo4n.hcp.eastus.azmk8s.io:443"
NAMESPACE="spark"

ACR_NAME="sparkdocker"
IMAGE_NAME="spark-medallion"
IMAGE_TAG="v2"

STORAGE_ACCOUNT_NAME="sparkdlt"

# Pull SP creds from Kubernetes Secret
K8S_SECRET_NAME="adls-sp-credentials"
CLIENT_ID="$(kubectl -n "${NAMESPACE}" get secret "${K8S_SECRET_NAME}" -o jsonpath='{.data.AZURE_CLIENT_ID}' | base64 --decode)"
CLIENT_SECRET="$(kubectl -n "${NAMESPACE}" get secret "${K8S_SECRET_NAME}" -o jsonpath='{.data.AZURE_CLIENT_SECRET}' | base64 --decode)"
TENANT_ID="$(kubectl -n "${NAMESPACE}" get secret "${K8S_SECRET_NAME}" -o jsonpath='{.data.AZURE_TENANT_ID}' | base64 --decode)"

spark-submit \
  --master k8s://${AKS_API_SERVER} \
  --deploy-mode cluster \
  --name medallion-etl-job \
  --conf spark.kubernetes.namespace=${NAMESPACE} \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=${ACR_NAME}.azurecr.io/${IMAGE_NAME}:${IMAGE_TAG} \
  --conf spark.kubernetes.container.image.pullPolicy=Always \
  \
  --conf spark.kubernetes.file.upload.path=/tmp \
  --conf spark.jars.ivy=/tmp/.ivy \
  \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4 \
  \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  \
  --conf spark.hadoop.fs.azure.account.auth.type.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=OAuth \
  --conf spark.hadoop.fs.azure.account.oauth.provider.type.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider \
  --conf spark.hadoop.fs.azure.account.oauth2.client.id.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=${CLIENT_ID} \
  --conf spark.hadoop.fs.azure.account.oauth2.client.secret.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=${CLIENT_SECRET} \
  --conf spark.hadoop.fs.azure.account.oauth2.client.endpoint.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=https://login.microsoftonline.com/${TENANT_ID}/oauth2/token \
  \
  local:///opt/app/etl/medallion_etl_adls.py
