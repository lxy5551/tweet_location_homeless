#!/bin/bash
# Location Analysis Pipeline
# Usage:
#   ./run_pipeline.sh 1 --range 1-5          (run step 1: self-reported locations)
#   ./run_pipeline.sh 1.5 --range 1-5        (run step 1.5: posts location signals)
#   ./run_pipeline.sh 2 --cities "baltimore" (run step 2: classify users)
#   ./run_pipeline.sh 3 --range 1-5          (run step 3: all friend analysis)
#   ./run_pipeline.sh 3.1 --range 1-5        (run step 3.1: fetch followers/followings)
#   ./run_pipeline.sh 3.2 --range 1-5        (run step 3.2: extract friend profiles)
#   ./run_pipeline.sh 3.3 --range 1-5        (run step 3.3: geocode locations)
#   ./run_pipeline.sh 3.4 --range 1-5        (run step 3.4: analyze user locations)
#   ./run_pipeline.sh 3s --range 1-5         (run step 3 for star users)
#   ./run_pipeline.sh 3.1s --range 1-5       (run step 3.1 for star users)
#   ./run_pipeline.sh 4 --range 1-5          (run step 4: merge all results)
#   ./run_pipeline.sh 5 --range 1-5          (run step 5: export CSV)
#   ./run_pipeline.sh all --range 1-5        (run all steps)
#
# Multi-threading (for step 3.1/3.1s - fetch followers/followings):
#   ./run_pipeline.sh 3.1 --cities "portland" --threads 5
#   ./run_pipeline.sh 3.1s --cities "portland" --threads 5
#
# Chunk support for remaining users (step 3.x only, NOT for star users):
#   ./run_pipeline.sh 3.1 --cities "portland" --chunk 1/20   (process chunk 1 of 20)
#   ./run_pipeline.sh show-chunks 20 --cities "portland"     (preview chunk distribution)
#
# Star users: Add 's' suffix to step 3.x (no chunk needed, use --threads instead)
#   e.g., 3s, 3.1s, 3.2s, 3.3s, 3.4s
#
# Note: Uses caffeinate to prevent Mac from sleeping during execution

if [ "$#" -eq 0 ]; then
    echo "Usage: run_pipeline.sh [step] [options]"
    echo "  step: 1, 1.5, 2, 3, 3.1-3.4, 3s, 3.1s-3.4s, 4, show-chunks, or all"
    echo "  options: --range 1-5 or --cities \"city1,city2\""
    echo "           --threads N (for step 3.1/3.1s, use N threads)"
    echo "           --chunk N/M (for step 3.x remaining users only)"
    echo ""
    echo "Steps:"
    echo "  1   - Self-reported locations (geocode user profiles)"
    echo "  1.5 - Posts location (geo-tag, network, text mentions)"
    echo "  2   - Classify users (star vs remaining)"
    echo "  3   - Friend analysis for remaining users (all sub-steps)"
    echo "  3.1 - Fetch followers/followings (slow, use --threads)"
    echo "  3.2 - Extract friend profiles (fast)"
    echo "  3.3 - Geocode friend locations (slow, API calls)"
    echo "  3.4 - Analyze user locations (fast)"
    echo "  3s  - Friend analysis for star users (all sub-steps)"
    echo "  3.1s-3.4s - Individual sub-steps for star users"
    echo "  4   - Merge all results to final_user_locations/"
    echo "  5   - Export location statistics to CSV"
    echo ""
    echo "Utilities:"
    echo "  show-chunks N - Preview how users split into N chunks"
    exit 1
fi

STEP=$1
shift

# Collect remaining arguments
ARGS="$@"

echo "============================================================"
echo "LOCATION ANALYSIS PIPELINE - STEP $STEP"
echo "============================================================"

case "$STEP" in
    "1")
        echo "Running Step 1: Self-reported locations..."
        caffeinate -s python step1_self_reported_locations.py $ARGS
        ;;
    "1.5")
        # Extract location from posts: geo-tag, network interactions, text mentions
        # Input: posts_english_2015-2025_all_info.json
        # Output: {city}_posts-location.json, {city}_no-location-users.json
        echo "Running Step 1.5: Posts location signals..."
        caffeinate -s python step1_5_posts_location.py $ARGS
        ;;
    "2")
        echo "Running Step 2: Star users..."
        caffeinate -s python step2_star_users.py $ARGS
        ;;
    "3")
        echo "Running Step 3: Friend analysis (all sub-steps)..."
        caffeinate -s python step3_friend_analysis.py $ARGS
        ;;
    "3.1")
        echo "Running Step 3.1: Fetch followers/followings..."
        caffeinate -s python step3_friend_analysis.py --substep 3.1 $ARGS
        ;;
    "3.2")
        echo "Running Step 3.2: Extract friend profiles..."
        caffeinate -s python step3_friend_analysis.py --substep 3.2 $ARGS
        ;;
    "3.3")
        echo "Running Step 3.3: Geocode friend locations..."
        caffeinate -s python step3_friend_analysis.py --substep 3.3 $ARGS
        ;;
    "3.4")
        echo "Running Step 3.4: Analyze user locations..."
        caffeinate -s python step3_friend_analysis.py --substep 3.4 $ARGS
        ;;
    "3s")
        # Run all step 3 sub-steps for star users
        echo "Running Step 3: Friend analysis (all sub-steps) for STAR USERS..."
        caffeinate -s python step3_friend_analysis.py --user-type star $ARGS
        ;;
    "3.1s")
        echo "Running Step 3.1: Fetch followers/followings for STAR USERS..."
        caffeinate -s python step3_friend_analysis.py --substep 3.1 --user-type star $ARGS
        ;;
    "3.2s")
        echo "Running Step 3.2: Extract friend profiles for STAR USERS..."
        caffeinate -s python step3_friend_analysis.py --substep 3.2 --user-type star $ARGS
        ;;
    "3.3s")
        echo "Running Step 3.3: Geocode friend locations for STAR USERS..."
        caffeinate -s python step3_friend_analysis.py --substep 3.3 --user-type star $ARGS
        ;;
    "3.4s")
        echo "Running Step 3.4: Analyze user locations for STAR USERS..."
        caffeinate -s python step3_friend_analysis.py --substep 3.4 --user-type star $ARGS
        ;;
    "4")
        # Merge all results (self-reported, star-users, friend-analysis)
        # Output: final_user_locations/{city}_final.json
        echo "Running Step 4: Merge all results..."
        caffeinate -s python step4_merge_results.py $ARGS
        ;;
    "5")
        # Export location statistics to Excel
        # Output: final_user_locations/location_stats.xlsx
        echo "Running Step 5: Export Excel..."
        caffeinate -s python step5_export_csv.py
        ;;
    "show-chunks")
        # show-chunks N --cities "city" format
        NUM_CHUNKS=$1
        shift
        ARGS="$@"
        echo "Showing chunk distribution for $NUM_CHUNKS chunks..."
        python step3_friend_analysis.py --show-chunks $NUM_CHUNKS $ARGS
        ;;
    "all")
        echo "Running Step 1: Self-reported locations..."
        caffeinate -s python step1_self_reported_locations.py $ARGS
        if [ $? -ne 0 ]; then
            echo "Step 1 failed!"
            exit 1
        fi
        echo
        # Extract location from posts for remaining users
        echo "Running Step 1.5: Posts location signals..."
        caffeinate -s python step1_5_posts_location.py $ARGS
        if [ $? -ne 0 ]; then
            echo "Step 1.5 failed!"
            exit 1
        fi
        echo
        echo "Running Step 2: Star users + geocode raw_location..."
        caffeinate -s python step2_star_users.py $ARGS
        if [ $? -ne 0 ]; then
            echo "Step 2 failed!"
            exit 1
        fi
        echo
        echo "Running Step 3: Friend analysis (remaining users)..."
        caffeinate -s python step3_friend_analysis.py $ARGS
        if [ $? -ne 0 ]; then
            echo "Step 3 failed!"
            exit 1
        fi
        echo
        # Run friend analysis for star users
        echo "Running Step 3s: Friend analysis (star users)..."
        caffeinate -s python step3_friend_analysis.py --user-type star $ARGS
        if [ $? -ne 0 ]; then
            echo "Step 3s failed!"
            exit 1
        fi
        echo
        # Merge all results to final_user_locations/
        echo "Running Step 4: Merge all results..."
        caffeinate -s python step4_merge_results.py $ARGS
        ;;
    *)
        echo "Invalid step: $STEP. Use 1, 1.5, 2, 3, 3.1-3.4, 3s, 3.1s-3.4s, 4, show-chunks, or all."
        exit 1
        ;;
esac

echo
echo "============================================================"
echo "DONE!"
echo "============================================================"
