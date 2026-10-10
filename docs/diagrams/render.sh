#!/usr/bin/env bash
# Renders every .mmd in this directory to <name>-light.png and <name>-dark.png.
#
# The README shows these images instead of live Mermaid blocks: GitHub's Mermaid embedding
# intermittently fails ("Unable to render rich display") or clips the diagram, and an image
# always displays. Transparent backgrounds, one per colour scheme, so they sit on GitHub's
# light and dark themes alike. Run after editing a .mmd file:
#
#   docs/diagrams/render.sh
set -euo pipefail
cd "$(dirname "$0")"
for source in *.mmd; do
  name=${source%.mmd}
  for theme in light dark; do
    mermaid_theme=default; [[ $theme == dark ]] && mermaid_theme=dark
    docker run --rm -u root -v "$PWD":/data minlag/mermaid-cli:latest \
      -i "/data/$source" -o "/data/$name-$theme.png" \
      -t "$mermaid_theme" -b transparent -s 2 -p /puppeteer-config.json >/dev/null
    echo "rendered $name-$theme.png"
  done
done
