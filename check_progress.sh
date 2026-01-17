#!/bin/bash
echo "=== GEOCODING PROGRESS CHECK ==="
echo "Time: $(date '+%H:%M:%S')"
echo ""
tail -15 'C:\Users\levin\AppData\Local\Temp\claude\C--Users-levin-Downloads-raw-x-data\tasks\bffe46b.output' 2>/dev/null || echo "Output file not found"
echo ""
if [ -f "sf_cities_geocoding_progress.json" ]; then
    echo "Checkpoint file exists - progress is being saved"
else
    echo "Checkpoint file not yet created (created every 100 locations)"
fi
