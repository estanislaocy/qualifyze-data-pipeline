#!/bin/bash

# Load environment variables from .env file
export $(cat .env | grep -v '^#' | xargs)
