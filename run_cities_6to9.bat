@echo off
REM Run pipeline for cities 6-9: rockford, san_francisco, scranton, southbend

echo ============================================================
echo PIPELINE: Cities 6-9
echo ============================================================

python step1_self_reported_locations.py --range 6-9
python step2_star_users.py --range 6-9

echo.
echo Cities 6-9 DONE!
