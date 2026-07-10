#!/usr/bin/env bash

set -e

echo "Generating project snapshot..."

tree -L 4 > PROJECT_TREE.txt

find . \
  -name "*.py" \
  -o -name "*.ts" \
  -o -name "*.js" \
  > FILE_INDEX.txt

echo "Done."