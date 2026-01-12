# spark-on-kubernetes-medallion
Azure ETL project using kubernetes, spark and docker

docker build and run commands:
## Running locally
docker build -t spark-medallion-local -f docker/Dockerfile .
docker run --rm -it -v "$PWD/outputs:/opt/app/outputs" spark-medallion-local


## Setting up ACR (Azure container Registry or docker imaage) and Kubernetes
1. Create ACR
az acr create \
  --resource-group <RG_NAME> \
  --name <ACR_NAME> \
  --sku Basic

2. Build and push to ACR (use amd64)
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  -f docker/Dockerfile_native_spark_on_k8 \
  -t sparkdocker.azurecr.io/spark-medallion:latest \
  --push \
  .



3. Create AKS
az aks create \
  --resource-group <RG_NAME> \
  --name <AKS_NAME> \
  --node-count 2 \
  --generate-ssh-keys

4. Attach ACR to AKS (so AKS can pull your image)
az aks update \
  --resource-group <RG_NAME> \
  --name <AKS_NAME> \
  --attach-acr <ACR_NAME>

5. Get kubeconfig
az aks get-credentials --resource-group <RG_NAME> --name <AKS_NAME>

6. Add SP config to kubernetes
kubectl -n spark create secret generic adls-sp-credentials \
  --from-literal=AZURE_CLIENT_ID="<CLIENT_ID>" \
  --from-literal=AZURE_CLIENT_SECRET="<CLIENT_SECRET>" \
  --from-literal=AZURE_TENANT_ID="<TENANT_ID>"


## Submitting spark job to Kubernetes

1. Create a temporary container that has same spark version as in the docker image
docker run --rm -it \
  -v ~/.kube:/home/spark/.kube \
  -e KUBECONFIG=/home/spark/.kube/config \
  -v "$(pwd)":/workspace \
  spark-submit:3.5.1 \
  /bin/bash

2. run script inside container
cd scripts
chmod +x submit_native_aks.sh
create temp files for spark to write to 
mkdir -p /tmp/spark-upload /tmp/spark-local /tmp/ivy
./submit_native_aks.sh



