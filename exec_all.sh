#!/bin/bash

# This script executes all the scripts in the current directory.

for f in execute*.sh; do
    echo "Executing $f"
    ./$f
done
