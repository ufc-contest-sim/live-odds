import pandas as pd
import json
from pathlib import Path
import glob
from datetime import datetime
import re
def extract_contest_name(filename):
    """Extract contest name from filename like PostContest_Results_Multi_..._UFC_300K_Knockout_Special.csv"""
    # Try to find the contest name after the timestamp
    # Pattern: PostContest_Results_Multi_250000_..._20260125_204616_CONTESTNAME.csv
    match = re.search(r'_(\d{8}_\d{6})_(.+)\.csv$', filename)
    if match:
        contest_name = match.group(2).replace('_', ' ')
        return contest_name

    # Fallback: just use filename
    return Path(filename).stem
def extract_entry_fee(df):
    """Extract entry fee from the dataframe"""
    if 'EntryFee' in df.columns:
        return float(df['EntryFee'].iloc[0])
    return 0

def extract_total_prizes(contest_name):
    """Extract total prize pool from contest name like 'UFC 150K Knockout' -> 150000"""
    match = re.search(r'(\d+(?:\.\d+)?)\s*[Kk]', contest_name)
    if match:
        return int(float(match.group(1)) * 1000)
    match = re.search(r'(\d+(?:\.\d+)?)\s*[Mm]', contest_name)
    if match:
        return int(float(match.group(1)) * 1000000)
    return 0
def sanitize_filename(name):
    """Convert contest name to safe filename"""
    # Remove special characters, replace spaces with underscores
    safe = re.sub(r'[^\w\s-]', '', name)
    safe = re.sub(r'\s+', '_', safe)
    return safe
def publish_results():
    try:
        print("Looking for result files...")
        print(f"Current directory: {Path.cwd()}")

        # Find ALL PostContest_Results CSV files
        results_files = glob.glob("PostContest_Results_*.csv")

        if not results_files:
            print("ERROR: No PostContest_Results_*.csv files found!")
            input("Press Enter to close...")
            return

        print(f"Found {len(results_files)} result file(s)")

        # Setup folders
        web_folder = Path("web")
        web_folder.mkdir(exist_ok=True)

        contests_config = []

        # Process each results file
        for results_file in results_files:
            print(f"\n{'='*50}")
            print(f"Processing: {results_file}")
            print(f"{'='*50}")

            # Read the data
            results_df = pd.read_csv(results_file)
            print(f"Loaded {len(results_df):,} lineups")

            # Extract contest info
            contest_name = extract_contest_name(results_file)
            entry_fee = extract_entry_fee(results_df)

            print(f"Contest: {contest_name}")
            print(f"Entry Fee: ${entry_fee:.2f}")

            # Get top stats for summary
            top_winpct = float(results_df['WinPct'].max()) if 'WinPct' in results_df.columns else 0
            top_netev = float(results_df['NetEV'].max()) if 'NetEV' in results_df.columns else 0
            top_roi = float(results_df['ROI%'].max()) if 'ROI%' in results_df.columns else 0

            # Replace NaN with empty string
            results_df = results_df.fillna('')

            # Look for companion _meta.json (fight_card + payouts from PostContestSim)
            csv_stem = Path(results_file).stem  # e.g. PostContest_Results_Multi_..._ContestName
            meta_json_path_candidate = Path(csv_stem + "_meta.json")
            fight_card = None
            payouts = None
            salary_map = None
            if meta_json_path_candidate.exists():
                try:
                    with open(meta_json_path_candidate, 'r') as mf:
                        meta_data = json.load(mf)
                    fight_card = meta_data.get("fight_card")
                    payouts = meta_data.get("payouts")
                    salary_map = meta_data.get("salary_map")
                    print(f"  Found companion meta: {meta_json_path_candidate}")
                    if fight_card:
                        print(f"  Fight card: {len(fight_card)} fights")
                    if payouts:
                        print(f"  Payouts: {len(payouts)} ranks")
                except Exception as e:
                    print(f"  Warning: Could not read meta file: {e}")

            # Create JSON structure
            output = {
                "contest_name": contest_name,
                "last_updated": datetime.now().isoformat(),
                "total_lineups": len(results_df),
                "summary": {
                    "top_win_pct": round(top_winpct, 2),
                    "top_netev": round(top_netev, 2),
                    "top_roi": round(top_roi, 2)
                },
                "leaderboard": results_df.to_dict('records')
            }
            if fight_card is not None:
                output["fight_card"] = fight_card
            if payouts is not None:
                output["payouts"] = payouts
            if salary_map is not None:
                output["salary_map"] = salary_map

            # Create safe filename for JSON
            json_filename = f"{sanitize_filename(contest_name)}.json"
            json_path = web_folder / json_filename

            # Save JSON
            with open(json_path, 'w') as f:
                json.dump(output, f, indent=2, allow_nan=False)

            print(f"✓ Saved: {json_path}")
            print(f"  Top Win%: {top_winpct:.2f}%")
            print(f"  Top NetEV: ${top_netev:,.2f}")
            print(f"  Top ROI: {top_roi:.2f}%")

            # Add to contests config
            total_prizes = extract_total_prizes(contest_name)
            contests_config.append({
                "id": sanitize_filename(contest_name).lower(),
                "name": contest_name,
                "file": json_filename,
                "entryFee": entry_fee,
                "totalPrizes": total_prizes,
                "entrants": len(results_df)
            })

        # Update contests.json
        contests_json_path = web_folder / "contests.json"
        with open(contests_json_path, 'w') as f:
            json.dump({"contests": contests_config}, f, indent=2)

        # Copy portfolio percentiles file if it exists
        pct_files = glob.glob("*_portfolio_percentiles.json")
        if pct_files:
            # Use the most recent one
            pct_file = sorted(pct_files)[-1]
            try:
                with open(pct_file, 'r') as pf:
                    pct_data = json.load(pf)
                pct_out_path = web_folder / "portfolio_percentiles.json"
                with open(pct_out_path, 'w') as pf:
                    json.dump(pct_data, pf, indent=2)
                print(f"\n✓ Portfolio percentiles: {pct_out_path} ({len(pct_data)} users)")
            except Exception as e:
                print(f"\n⚠ Warning: Could not process portfolio percentiles: {e}")
        else:
            print("\n⚠ No portfolio percentiles file found (run PostContestSim with latest version to generate)")

        print(f"\n{'='*50}")
        print(f"✓ ALL CONTESTS PUBLISHED!")
        print(f"{'='*50}")
        print(f"✓ Total contests: {len(contests_config)}")
        print(f"✓ Updated: {contests_json_path}")
        print(f"\nContests:")
        for c in contests_config:
            print(f"  - {c['name']} (${c['entryFee']} entry)")

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()

    input("\nPress Enter to close...")
if __name__ == "__main__":
    publish_results()
