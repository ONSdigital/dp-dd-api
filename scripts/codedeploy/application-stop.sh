#!/bin/bash

CONTAINER_ID=$(docker ps | grep dp-dd-database-loader | awk '{print $1}')

if [[ -n $CONTAINER_ID ]]; then
  docker stop $CONTAINER_ID
fi
