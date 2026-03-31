#!bin/sh
DIR=`dirname $0`
cd "${DIR}" || exit 1

REPO="dilyin/data-lake-sync"
TAG="1"

docker build -t "${REPO}:latest" .
docker tag "${REPO}:latest" "${REPO}:${TAG}"
