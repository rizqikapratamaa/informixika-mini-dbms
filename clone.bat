#!/bin/bash

# Clone submodules with their specific branches
git submodule init
git submodule update --init --recursive

echo "All submodules cloned and checked out successfully!"
