#!/bin/bash -eux

pushd dp-dd-database-loader
  sbt test
popd
