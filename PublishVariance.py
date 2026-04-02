#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Variance Simulator — Generates bankroll trajectory data for a target user.

Reads the raw per-user outcomes saved by PostContestSim.py (_user_outcomes.npz)
and produces a JSON file consumable by variance.html.

Output includes:
  - Downsampled per-contest iteration outcomes (for client-side combo generation)
  - Pre-computed "all contests" variance block as a default view
  - The HTML does the heavy lifting for arbitrary contest combinations
"""
import os
import sys
import json
import glob
import argparse
import numpy as np
from pathlib import Path


def log(msg):
    print(msg, flush=True)


def find_outcomes_file():
    """Find the most recent _user_outcomes.npz file."""
    files = glob.glob("*_user_outcomes.npz")
    if not files:
        return None
    return sorted(files)[-1]


def compute_variance_block(user_outcomes, num_paths, path_length, rng):
    """Compute variance simulation data for a 1D outcomes array.

    Returns a dict with: envelope, paths, drawdown_stats, upswing_stats,
    single_event stats, and histogram.
    """
    total_iters = len(user_outcomes)

    # Single-event stats
    hist_bins = 50
    hist_counts, hist_edges = np.histogram(user_outcomes, bins=hist_bins)
    single_event = {
        "mean": round(float(user_outcomes.mean()), 2),
        "std": round(float(user_outcomes.std()), 2),
        "min": round(float(user_outcomes.min()), 2),
        "max": round(float(user_outcomes.max()), 2),
        "median": round(float(np.median(user_outcomes)), 2),
        "histogram": {
            "counts": hist_counts.tolist(),
            "bin_edges": hist_edges.round(2).tolist(),
        },
    }

    # Generate bankroll paths by bootstrap resampling
    paths = np.zeros((num_paths, path_length), dtype=np.float64)
    for p in range(num_paths):
        sample_indices = rng.integers(0, total_iters, size=path_length)
        sampled = user_outcomes[sample_indices]
        paths[p] = np.cumsum(sampled)

    # Percentile envelope at each event step
    envelope_pcts = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    envelope = {}
    for pct in envelope_pcts:
        envelope[f"p{pct}"] = np.percentile(paths, pct, axis=0).round(2).tolist()

    # Drawdown / upswing analysis
    max_drawdowns = np.zeros(num_paths, dtype=np.float64)
    max_upswings = np.zeros(num_paths, dtype=np.float64)
    for p in range(num_paths):
        full_path = np.concatenate([[0], paths[p]])
        running_max = np.maximum.accumulate(full_path)
        drawdowns = running_max - full_path
        max_drawdowns[p] = drawdowns.max()

        running_min = np.minimum.accumulate(full_path)
        upswings = full_path - running_min
        max_upswings[p] = upswings.max()

    # Probability thresholds
    thresholds = [5000, 10000, 15000, 20000, 25000, 30000, 40000, 50000,
                  75000, 100000, 150000, 200000]
    drawdown_probs = {}
    upswing_probs = {}
    for t in thresholds:
        dd_pct = float((max_drawdowns >= t).mean() * 100)
        us_pct = float((max_upswings >= t).mean() * 100)
        if dd_pct > 0.01 or t <= 50000:
            drawdown_probs[str(t)] = round(dd_pct, 2)
        if us_pct > 0.01 or t <= 50000:
            upswing_probs[str(t)] = round(us_pct, 2)

    # Sample display paths (evenly spaced by final value for good coverage)
    final_values = paths[:, -1]
    sorted_path_indices = np.argsort(final_values)
    display_path_count = min(100, num_paths)
    selected_indices = np.linspace(0, num_paths - 1, display_path_count, dtype=int)
    selected_indices = sorted_path_indices[selected_indices]

    display_paths = []
    for idx in selected_indices:
        path_data = paths[idx]
        if path_length > 500:
            step = path_length // 500
            path_data = path_data[::step]
        display_paths.append(path_data.round(2).tolist())

    return {
        "single_event": single_event,
        "envelope": envelope,
        "paths": display_paths,
        "config": {
            "num_paths": num_paths,
            "path_length": path_length,
            "display_paths": display_path_count,
        },
        "drawdown_stats": {
            "mean": round(float(max_drawdowns.mean()), 2),
            "median": round(float(np.median(max_drawdowns)), 2),
            "p95": round(float(np.percentile(max_drawdowns, 95)), 2),
            "p99": round(float(np.percentile(max_drawdowns, 99)), 2),
            "max": round(float(max_drawdowns.max()), 2),
            "probabilities": drawdown_probs,
        },
        "upswing_stats": {
            "mean": round(float(max_upswings.mean()), 2),
            "median": round(float(np.median(max_upswings)), 2),
            "p95": round(float(np.percentile(max_upswings, 95)), 2),
            "p99": round(float(np.percentile(max_upswings, 99)), 2),
            "max": round(float(max_upswings.max()), 2),
            "probabilities": upswing_probs,
        },
    }


def generate_variance_data(
    outcomes_file: str,
    target_user: str,
    num_paths: int = 500,
    path_length: int = 100,
    seed: int = 42,
    sample_size: int = 10000,
):
    """Generate variance simulation data for a target user.

    Args:
        outcomes_file: Path to the _user_outcomes.npz file
        target_user: Username to analyze (case-insensitive match)
        num_paths: Number of simulated bankroll paths to generate
        path_length: Number of events per path
        seed: Random seed for reproducibility
        sample_size: Number of per-contest iteration samples to export for
                     client-side combo generation

    Returns:
        dict: Variance data ready for JSON serialization
    """
    log(f"Loading outcomes from: {outcomes_file}")
    data = np.load(outcomes_file, allow_pickle=True)
    outcomes = data["outcomes"]  # (total_iters, num_users)
    per_contest = data["per_contest"]  # (K, total_iters, num_users)
    user_names = data["user_names"].tolist()
    contest_names = data["contest_names"].tolist()

    total_iters, num_users = outcomes.shape
    log(f"Loaded: {total_iters:,} iterations x {num_users} users")
    log(f"Contests: {contest_names}")

    # Find target user (case-insensitive)
    target_lower = target_user.lower()
    user_idx = None
    matched_name = None
    for i, name in enumerate(user_names):
        if name and name.lower() == target_lower:
            user_idx = i
            matched_name = name
            break

    if user_idx is None:
        log(f"ERROR: User '{target_user}' not found!")
        log(f"Available users: {[n for n in user_names if n][:20]}...")
        return None

    log(f"Found user: {matched_name} (index {user_idx})")

    # Extract this user's per-iteration net profit (all contests combined)
    user_outcomes = outcomes[:, user_idx].astype(np.float64)
    log(f"User outcomes: mean=${user_outcomes.mean():.2f}, std=${user_outcomes.std():.2f}")
    log(f"  min=${user_outcomes.min():.2f}, max=${user_outcomes.max():.2f}")

    rng = np.random.default_rng(seed)

    # Generate "all contests" block
    log(f"Generating ALL CONTESTS: {num_paths} paths x {path_length} events...")
    all_block = compute_variance_block(user_outcomes, num_paths, path_length, rng)

    # Export downsampled per-contest iteration outcomes for client-side combos.
    # We sample the SAME iteration indices across all contests so they stay
    # correlated (same fight outcomes per row).
    actual_sample = min(sample_size, total_iters)
    sample_idx = rng.choice(total_iters, size=actual_sample, replace=False)
    sample_idx.sort()

    raw_contest_samples = {}  # contest_name -> list of floats (length = actual_sample)
    active_contests = []
    for k, cname in enumerate(contest_names):
        contest_data = per_contest[k, :, user_idx].astype(np.float64)
        if np.any(contest_data != 0):
            active_contests.append(cname)
            raw_contest_samples[cname] = np.round(contest_data[sample_idx], 2).tolist()
            log(f"  {cname}: mean=${contest_data.mean():.2f}, exported {actual_sample:,} samples")

    result = {
        "user": matched_name,
        "total_iterations": int(total_iters),
        "contests": active_contests,
        # "all" is the combined pre-computed view
        **all_block,
        # Raw per-contest samples for client-side combination
        "contest_samples": raw_contest_samples,
        "sample_size": actual_sample,
        # Config for client-side generation
        "num_paths": num_paths,
        "path_length": path_length,
    }

    return result


def main():
    ap = argparse.ArgumentParser(description="Generate variance simulation data for a user")
    ap.add_argument("--user", default="DHollis24", help="Target username (default: DHollis24)")
    ap.add_argument("--file", default=None, help="Path to _user_outcomes.npz (auto-detected if omitted)")
    ap.add_argument("--paths", type=int, default=500, help="Number of simulated paths (default: 500)")
    ap.add_argument("--length", type=int, default=100, help="Events per path (default: 100)")
    ap.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    ap.add_argument("--samples", type=int, default=10000, help="Per-contest samples for combo mode (default: 10000)")
    ap.add_argument("--output", default=None, help="Output JSON path (default: variance_data.json)")
    args = ap.parse_args()

    outcomes_file = args.file or find_outcomes_file()
    if not outcomes_file or not Path(outcomes_file).exists():
        log("ERROR: No _user_outcomes.npz file found!")
        log("Run PostContestSim.py first to generate the outcomes data.")
        log("(Make sure you are using the latest version that saves raw outcomes.)")
        try:
            input("Press Enter to close...")
        except Exception:
            pass
        return

    result = generate_variance_data(
        outcomes_file=outcomes_file,
        target_user=args.user,
        num_paths=args.paths,
        path_length=args.length,
        seed=args.seed,
        sample_size=args.samples,
    )

    if result is None:
        try:
            input("Press Enter to close...")
        except Exception:
            pass
        return

    out_path = args.output or "variance_data.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f)

    file_size = os.path.getsize(out_path) / 1024
    log(f"\n[done] Wrote variance data: {out_path} ({file_size:.0f} KB)")
    log(f"  User: {result['user']}")
    log(f"  Paths: {result['num_paths']} x {result['path_length']} events")
    log(f"  Single-event EV: ${result['single_event']['mean']:.2f}")
    log(f"  Avg max drawdown: ${result['drawdown_stats']['mean']:,.2f}")
    log(f"  Avg max upswing: ${result['upswing_stats']['mean']:,.2f}")
    log(f"  Contest samples exported: {result['sample_size']:,} per contest")
    log(f"  Contests: {result['contests']}")

    try:
        input("\nPress Enter to close...")
    except Exception:
        pass


if __name__ == "__main__":
    main()
