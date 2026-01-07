# spark-on-kubernetes-medallion
Azure ETL project using kubernetes, spark and docker

docker build and run commands:
## Running locally
docker build -t spark-medallion-local -f docker/Dockerfile .
docker run --rm -it -v "$PWD/outputs:/opt/app/outputs" spark-medallion-local
