"""
Location Analysis Pipeline - Cities 1-5
baltimore, buffalo, el paso, fayetteville, portland
"""

from location_analysis_pipeline import run_pipeline, safe_print

def main():
    cities = [
        'baltimore',
        'buffalo',
        'el paso',
        'fayetteville',
        'portland'
    ]

    safe_print("="*60)
    safe_print("LOCATION ANALYSIS PIPELINE - CITIES 1-5")
    safe_print("="*60)
    safe_print(f"Cities to process: {cities}")

    for city in cities:
        try:
            run_pipeline(city)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()
            continue

    safe_print("\n" + "="*60)
    safe_print("CITIES 1-5 DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
