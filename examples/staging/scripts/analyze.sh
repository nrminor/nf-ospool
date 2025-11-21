#!/bin/bash
# Example script to demonstrate directory staging

input="$1"

echo "Analyzing: ${input}"
echo "Script location: $(readlink -f $0)"
echo "Current directory: $(pwd)"
echo "Host: $(hostname)"
date
