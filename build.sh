#!bin/sh
DIR=`dirname $0`
cd "${DIR}" || exit 1

REPO="dilyin/data-lake-sync"
TAG="latest"

sudo docker build -t "${REPO}:${TAG}" .
