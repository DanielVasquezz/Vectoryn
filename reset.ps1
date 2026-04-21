# reset.ps1 — Full cleanup to restart Vectoryn on Windows
# Run with: .\reset.ps1
# FIX: This script resolves the "network not found" error that occurs
# when "docker compose down -v" fails to clean up properly on Windows.

Write-Host "Stopping containers..." -ForegroundColor Yellow
docker compose down -v --remove-orphans 2>$null
docker compose --profile observability down -v --remove-orphans 2>$null

Write-Host "Pruning orphan networks..." -ForegroundColor Yellow
docker network prune -f

Write-Host "Pruning orphan volumes..." -ForegroundColor Yellow  
docker volume prune -f

Write-Host "Done. You can now start the system with:" -ForegroundColor Green
Write-Host "  docker compose up -d --build" -ForegroundColor Cyan
Write-Host "  # or with observability:" -ForegroundColor Gray
Write-Host "  docker compose --profile observability up -d --build" -ForegroundColor Cyan