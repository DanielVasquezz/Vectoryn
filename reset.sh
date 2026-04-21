#!/bin/bash
# reset.sh — Full cleanup to restart Vectoryn on Linux/Mac
# Usage: chmod +x reset.sh && ./reset.sh
set -e

echo "Stopping containers..."
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose --profile observability down -v --remove-orphans 2>/dev/null || true

echo "Pruning orphan networks..."
docker network prune -f

echo "Pruning orphan volumes..."
docker volume prune -f

echo ""
echo "Done. Start with:"
echo "  docker compose up -d --build"
echo "  # or with observability:"
echo "  docker compose --profile observability up -d --build"