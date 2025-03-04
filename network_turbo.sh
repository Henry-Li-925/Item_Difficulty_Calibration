#!/bin/bash

# 1. Source the network turbo
source /etc/network_turbo

# 2. Export the HF model cache into the data drive
export HF_HOME=/root/autodl-tmp/cache/

# 3. Display the result
echo "Network turbo sourced and HF HOME set to $HF_HOME"