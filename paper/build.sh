#!/usr/bin/env bash
# Build the paper PDF. Requires latexmk + a TeX distribution (texlive-full on Debian,
# mactex on macOS). Safe to re-run; latexmk handles incremental passes.
set -euo pipefail
cd "$(dirname "$0")"
latexmk -pdf -interaction=nonstopmode -halt-on-error multimodal-query-demo.tex
