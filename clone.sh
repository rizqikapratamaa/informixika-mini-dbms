#!/bin/bash

# Clone submodules
git submodule init
git submodule update --init --recursive

echo "All submodules cloned successfully!"
