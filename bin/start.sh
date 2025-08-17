#!/bin/bash

set -e

echo "Starting the application..."
echo "Environment: ${ENV:-development}"

uvicorn main:app --reload