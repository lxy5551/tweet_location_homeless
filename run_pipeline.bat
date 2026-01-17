@echo off
REM Location Analysis Pipeline
REM Usage:
REM   run_pipeline.bat 1 --range 1-5          (run step 1: self-reported locations)
REM   run_pipeline.bat 1.5 --range 1-5        (run step 1.5: posts location signals)
REM   run_pipeline.bat 2 --cities "baltimore" (run step 2: classify users)
REM   run_pipeline.bat 3 --range 1-5          (run step 3: all friend analysis)
REM   run_pipeline.bat 3.1 --range 1-5        (run step 3.1: fetch followers/followings)
REM   run_pipeline.bat 3.2 --range 1-5        (run step 3.2: extract friend profiles)
REM   run_pipeline.bat 3.3 --range 1-5        (run step 3.3: geocode locations)
REM   run_pipeline.bat 3.4 --range 1-5        (run step 3.4: analyze user locations)
REM   run_pipeline.bat 3s --range 1-5         (run step 3 for star users)
REM   run_pipeline.bat 3.1s --range 1-5       (run step 3.1 for star users)
REM   run_pipeline.bat 4 --range 1-5          (run step 4: merge all results)
REM   run_pipeline.bat all --range 1-5        (run all steps)
REM
REM Multi-threading (for step 3.1/3.1s - fetch followers/followings):
REM   run_pipeline.bat 3.1 --cities "portland" --threads 5
REM   run_pipeline.bat 3.1s --cities "portland" --threads 5
REM
REM Chunk support for remaining users (step 3.x only, NOT for star users):
REM   run_pipeline.bat 3.1 --cities "portland" --chunk 1/20   (process chunk 1 of 20)
REM   run_pipeline.bat show-chunks 20 --cities "portland"     (preview chunk distribution)
REM
REM Star users: Add 's' suffix to step 3.x (no chunk needed, use --threads instead)
REM   e.g., 3s, 3.1s, 3.2s, 3.3s, 3.4s

setlocal enabledelayedexpansion

set STEP=%1

if "%STEP%"=="" (
    echo Usage: run_pipeline.bat [step] [options]
    echo   step: 1, 1.5, 2, 3, 3.1-3.4, 3s, 3.1s-3.4s, 4, show-chunks, or all
    echo   options: --range 1-5 or --cities "city1,city2"
    echo            --threads N (for step 3.1/3.1s, use N threads)
    echo            --chunk N/M (for step 3.x remaining users only)
    echo.
    echo Steps:
    echo   1   - Self-reported locations (geocode user profiles)
    echo   1.5 - Posts location (geo-tag, network, text mentions)
    echo   2   - Classify users (star vs remaining)
    echo   3   - Friend analysis for remaining users (all sub-steps)
    echo   3.1 - Fetch followers/followings (slow, use --threads)
    echo   3.2 - Extract friend profiles (fast)
    echo   3.3 - Geocode friend locations (slow, API calls)
    echo   3.4 - Analyze user locations (fast)
    echo   3s  - Friend analysis for star users (all sub-steps)
    echo   3.1s-3.4s - Individual sub-steps for star users
    echo   4   - Merge all results to final_user_locations/
    echo.
    echo Utilities:
    echo   show-chunks N - Preview how users split into N chunks
    goto :eof
)

set ARGS=%2 %3 %4 %5 %6 %7

echo ============================================================
echo LOCATION ANALYSIS PIPELINE - STEP %STEP%
echo ============================================================

if "%STEP%"=="1" (
    echo Running Step 1: Self-reported locations...
    python step1_self_reported_locations.py %ARGS%
    goto done
)
if "%STEP%"=="1.5" (
    REM Extract location from posts: geo-tag, network interactions, text mentions
    echo Running Step 1.5: Posts location signals...
    python step1_5_posts_location.py %ARGS%
    goto done
)
if "%STEP%"=="2" (
    echo Running Step 2: Star users...
    python step2_star_users.py %ARGS%
    goto done
)
if "%STEP%"=="3" (
    echo Running Step 3: Friend analysis (all sub-steps)...
    python step3_friend_analysis.py %ARGS%
    goto done
)
if "%STEP%"=="3.1" (
    echo Running Step 3.1: Fetch followers/followings...
    python step3_friend_analysis.py --substep 3.1 %ARGS%
    goto done
)
if "%STEP%"=="3.2" (
    echo Running Step 3.2: Extract friend profiles...
    python step3_friend_analysis.py --substep 3.2 %ARGS%
    goto done
)
if "%STEP%"=="3.3" (
    echo Running Step 3.3: Geocode friend locations...
    python step3_friend_analysis.py --substep 3.3 %ARGS%
    goto done
)
if "%STEP%"=="3.4" (
    echo Running Step 3.4: Analyze user locations...
    python step3_friend_analysis.py --substep 3.4 %ARGS%
    goto done
)
if "%STEP%"=="3s" (
    REM Run all step 3 sub-steps for star users
    echo Running Step 3: Friend analysis for STAR USERS...
    python step3_friend_analysis.py --user-type star %ARGS%
    goto done
)
if "%STEP%"=="3.1s" (
    echo Running Step 3.1: Fetch followers/followings for STAR USERS...
    python step3_friend_analysis.py --substep 3.1 --user-type star %ARGS%
    goto done
)
if "%STEP%"=="3.2s" (
    echo Running Step 3.2: Extract friend profiles for STAR USERS...
    python step3_friend_analysis.py --substep 3.2 --user-type star %ARGS%
    goto done
)
if "%STEP%"=="3.3s" (
    echo Running Step 3.3: Geocode friend locations for STAR USERS...
    python step3_friend_analysis.py --substep 3.3 --user-type star %ARGS%
    goto done
)
if "%STEP%"=="3.4s" (
    echo Running Step 3.4: Analyze user locations for STAR USERS...
    python step3_friend_analysis.py --substep 3.4 --user-type star %ARGS%
    goto done
)
if "%STEP%"=="4" (
    REM Merge all results (self-reported, star-users, friend-analysis)
    REM Output: final_user_locations/{city}_final.json
    echo Running Step 4: Merge all results...
    python step4_merge_results.py %ARGS%
    goto done
)
if "%STEP%"=="show-chunks" (
    set NUM_CHUNKS=%2
    set CHUNK_ARGS=%3 %4 %5 %6 %7
    echo Showing chunk distribution for %NUM_CHUNKS% chunks...
    python step3_friend_analysis.py --show-chunks %NUM_CHUNKS% %CHUNK_ARGS%
    goto done
)
if "%STEP%"=="all" (
    echo Running Step 1: Self-reported locations...
    python step1_self_reported_locations.py %ARGS%
    if errorlevel 1 (
        echo Step 1 failed!
        goto :eof
    )
    echo.
    REM Extract location from posts for remaining users
    echo Running Step 1.5: Posts location signals...
    python step1_5_posts_location.py %ARGS%
    if errorlevel 1 (
        echo Step 1.5 failed!
        goto :eof
    )
    echo.
    echo Running Step 2: Star users + geocode raw_location...
    python step2_star_users.py %ARGS%
    if errorlevel 1 (
        echo Step 2 failed!
        goto :eof
    )
    echo.
    echo Running Step 3: Friend analysis (remaining users)...
    python step3_friend_analysis.py %ARGS%
    if errorlevel 1 (
        echo Step 3 failed!
        goto :eof
    )
    echo.
    REM Run friend analysis for star users
    echo Running Step 3s: Friend analysis (star users)...
    python step3_friend_analysis.py --user-type star %ARGS%
    if errorlevel 1 (
        echo Step 3s failed!
        goto :eof
    )
    echo.
    REM Merge all results to final_user_locations/
    echo Running Step 4: Merge all results...
    python step4_merge_results.py %ARGS%
    goto done
)

echo Invalid step: %STEP%. Use 1, 1.5, 2, 3, 3.1-3.4, 3s, 3.1s-3.4s, 4, show-chunks, or all.
goto :eof

:done
echo.
echo ============================================================
echo DONE!
echo ============================================================
