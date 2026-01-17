@echo off
REM Run pipeline for cities 1-5: baltimore, buffalo, el paso, fayetteville, portland

echo ============================================================
echo PIPELINE: Cities 1-5
echo ============================================================

python step1_self_reported_locations.py --range 1-5
python step2_star_users.py --range 1-5

echo.
echo Cities 1-5 DONE!
