#!/usr/bin/env bash

for dir in data helpers exercises; do curl -fsSL "https://api.github.com/repos/sturc/big-data-engineering/git/trees/main?recursive=1" | jq -r --arg d "$dir" '.tree[] | select(.path | startswith($d + "/")) | select(.type == "blob") | [.path, ("https://raw.githubusercontent.com/sturc/big-data-engineering/main/" + .path)] | @tsv' | while IFS=$'\t' read -r path url; do mkdir -p "/home/coder/examples/$(dirname "$path")" && curl -fsSL "$url" -o "/home/coder/examples/$path"; done; done
