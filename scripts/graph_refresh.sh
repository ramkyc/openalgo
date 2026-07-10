#!/usr/bin/env bash

set -e

echo "Refreshing graph..."

rm -rf graphify-out

graphify build .

echo "Graph refreshed."