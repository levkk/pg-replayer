#!/bin/bash

# Run the container!
echo "Building the container and running tests..."

docker run $(docker build . -q | sed 's/sha256://')
