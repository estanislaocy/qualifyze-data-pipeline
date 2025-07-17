#!/bin/bash

# Load environment variables from .env file
export $(cat .env | grep -v '^#' | xargs)

# Run dbt with all arguments passed to this script
dbt "$@" 