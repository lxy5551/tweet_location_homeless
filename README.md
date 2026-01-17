# Tweet Location Analysis Pipeline

Analyze Twitter user locations using self-reported data, star users, and friend network analysis.

## Quick Start

```bash
# Mac
./run_pipeline.sh 1 --cities "baltimore"

# Windows
run_pipeline.bat 1 --cities "baltimore"
```

## Pipeline Steps

| Step | Description | Speed |
|------|-------------|-------|
| 1 | Self-reported locations (geocode user profiles) | Medium |
| 2 | Star users (identify users with >1000 followers) | Fast |
| 3 | Friend analysis (all sub-steps below) | Slow |
| 3.1 | Fetch followers/followings | Slow (API) |
| 3.2 | Extract friend profiles | Fast |
| 3.3 | Geocode friend locations | Slow (API) |
| 3.4 | Analyze user locations | Fast |

## Available Cities

| # | City | Option |
|---|------|--------|
| 1 | Baltimore | `baltimore` |
| 2 | Buffalo | `buffalo` |
| 3 | El Paso | `el paso` |
| 4 | Fayetteville | `fayetteville` |
| 5 | Portland | `portland` |
| 6 | Rockford | `rockford` |
| 7 | San Francisco | `san_francisco` |
| 8 | Scranton | `scranton` |
| 9 | South Bend | `southbend` |

## Usage Examples

### Run by city name
```bash
./run_pipeline.sh 1 --cities "baltimore"
./run_pipeline.sh 1 --cities "baltimore,buffalo,el paso"
```

### Run by range (1-indexed)
```bash
./run_pipeline.sh 1 --range 1-3    # baltimore, buffalo, el paso
./run_pipeline.sh 1 --range 4-6    # fayetteville, portland, rockford
```

### Run all cities
```bash
./run_pipeline.sh 1    # no --cities or --range = all cities
```

### Run specific sub-step
```bash
./run_pipeline.sh 3.1 --cities "portland"   # fetch followers/followings only
./run_pipeline.sh 3.2 --cities "portland"   # extract friend profiles only
./run_pipeline.sh 3.3 --cities "portland"   # geocode locations only
./run_pipeline.sh 3.4 --cities "portland"   # analyze locations only
```

## Parallel Processing with Chunks

For large cities (portland: 1757 users, san_francisco: 2544 users), you can split the work across multiple PCs.

### Preview chunk distribution
```bash
./run_pipeline.sh show-chunks 20 --cities "portland,san_francisco"
```

### Run specific chunk
```bash
./run_pipeline.sh 3.1 --cities "portland" --chunk 1/20   # chunk 1 of 20
./run_pipeline.sh 3.1 --cities "portland" --chunk 2/20   # chunk 2 of 20
```

### Example: Distribute across 3 PCs overnight

**PC 1:**
```bash
for i in 1 2 3 4 5 6 7; do
  ./run_pipeline.sh 3.1 --cities "portland" --chunk $i/20
done
```

**PC 2:**
```bash
for i in 8 9 10 11 12 13 14; do
  ./run_pipeline.sh 3.1 --cities "portland" --chunk $i/20
done
```

**PC 3:**
```bash
for i in 15 16 17 18 19 20; do
  ./run_pipeline.sh 3.1 --cities "portland" --chunk $i/20
done
```

**After all PCs finish step 3.1**, run steps 3.2-3.4 on one PC:
```bash
./run_pipeline.sh 3.2 --cities "portland"
./run_pipeline.sh 3.3 --cities "portland"
./run_pipeline.sh 3.4 --cities "portland"
```

## Output Files

| File | Description |
|------|-------------|
| `User_Location_Analysis/{city}_self-reported.json` | Step 1 results |
| `User_Location_Analysis/{city}_star-users.json` | Step 2 results |
| `User_Location_Analysis/{city}_remaining-users.json` | Users for step 3 |
| `User_Location_Analysis/{city}_friend-analysis.json` | Step 3 final results |
| `raw_x_data/{city}/follower_{city}.json` | Followers data |
| `raw_x_data/{city}/following_{city}.json` | Followings data |
| `{city}_friend-info/` | Friend profiles per user |
| `{city}_friend-location/` | Geocoded friend locations |
| `geocode_cache.json` | Shared geocoding cache |

## Caching

### Geocode Cache
- `geocode_cache.json` stores all geocoded locations
- Shared across all cities (reduces duplicate API calls)
- Copy this file between PCs to share results

### Incremental Processing
- Each step skips already-processed users
- Safe to restart if interrupted
- Delete output files to force reprocessing

## Prevent Mac Sleep

The Mac version uses `caffeinate` to prevent sleep during execution. This is automatic when using `run_pipeline.sh`.

For manual runs:
```bash
caffeinate -s python step3_friend_analysis.py --cities "portland"
```

## Windows Notes

Windows version uses `run_pipeline.bat` with same options:
```batch
run_pipeline.bat 3.1 --cities "portland" --chunk 1/20
```

To prevent Windows sleep, adjust Power Settings or use:
```batch
powercfg -change -standby-timeout-ac 0
```

## Troubleshooting

### Missing module error
```bash
pip install geopy
```

### API rate limiting
Step 3.1 and 3.3 are slow due to API rate limits. Use chunks to parallelize.

### Out of memory
Large cities may use significant memory. Process one city at a time.

## Typical Workflow

1. Run steps 1-2 for all cities (relatively fast)
2. Check remaining users count per city
3. For small cities (<100 users): run step 3 directly
4. For large cities: use chunks across multiple PCs
5. After step 3.1 completes, run 3.2-3.4 (fast)
