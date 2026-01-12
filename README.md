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

2. Create AKS
az aks create \
  --resource-group <RG_NAME> \
  --name <AKS_NAME> \
  --node-count 2 \
  --generate-ssh-keys

3. Attach ACR to AKS (so AKS can pull your image)
az aks update \
  --resource-group <RG_NAME> \
  --name <AKS_NAME> \
  --attach-acr <ACR_NAME>

4. Get kubeconfig
az aks get-credentials --resource-group <RG_NAME> --name <AKS_NAME>
