#!/bin/bash

IMAGE=dataplex-oracle-pyspark:0.0.2
PROJECT=<PROJECT_ID>
REGION=us-central1

REPO_IMAGE=${REGION}-docker.pkg.dev/${PROJECT}/docker-repo/dataplex-oracle-pyspark

docker build -t "${IMAGE}" .

# Tag and push to GCP container registry
gcloud config set project ${PROJECT}
gcloud auth configure-docker ${REGION}-docker.pkg.dev
docker tag "${IMAGE}" "${REPO_IMAGE}"
docker push "${REPO_IMAGE}"
