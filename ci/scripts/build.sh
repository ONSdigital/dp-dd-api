#!/bin/bash -eux

pushd dp-dd-database-loader
  sbt assembly
popd

cp -r dp-dd-database-loader/target/* target/
