#!/usr/bin/env bash
set -euo pipefail

cd /workspace

echo "==> Installing Python dependencies..."
pip install --no-cache-dir ".[dev]"

echo "==> Installing pre-commit hooks..."
pip install --no-cache-dir pre-commit
pre-commit install

echo "==> Copying .env.example to .env (if not present)..."
if [ ! -f .env ]; then
  cp .env.example .env
  echo "    .env created — fill in your API tokens before running."
fi

echo "==> Dev container ready."
echo "    Run 'pytest tests/' to verify setup."
