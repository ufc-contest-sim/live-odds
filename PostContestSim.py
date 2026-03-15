#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UFC Post-Contest Simulator — Multi-Contest (shared fight sampling) + HP + MP
Behavior:
- Multi-contest: contests defined on workbook sheet 'Contests'
    Required columns (case-insensitive):
      ContestName, LineupsSheet, PayoutsSheet, EntryFee
    Optional:
      PrefixSheet   (cumulative payout prefix; validated; otherwise computed from payouts)
- LineupsSheet can be either:
    * An Excel sheet name (original behavior)
    * A DraftKings CSV file path ending in .csv (e.g. "contest1.csv")
      CSV files are resolved relative to the workbook directory.
      Expected CSV columns: Rank, EntryId, EntryName, TimeRemaining, Points, Lineup
- Fight score sampling is done ONCE per iteration and reused across contests.
- Missing/empty fight sim sheet(s) => fail fast.
- Fighter name normalization (whitespace collapse). Unmapped fighters score 0.
- Blank lineup rows with a username are kept and will score 0.
- Outputs per-contest results CSVs (one file per contest). No portfolio summary, no series.
- Sanity check per contest: avg EV equals prize_pool/n and avg total paid equals prize_pool.
Backwards compatible fallback:
- If 'Contests' sheet is missing, runs single contest:
    ContestName='Main', LineupsSheet='Post Contest Sim', PayoutsSheet='Payouts',
    PrefixSheet='PayoutPrefix' (if present), EntryFee read from 'DraftKings Fighter Pool'!D2.
"""
import os, sys, math, time, argparse, datetime, re, tempfile, uuid, json
from pathlib import Path
from typing import Optional
import numpy as np
import pandas as pd
from openpyxl import load_workbook
from concurrent.futures import ProcessPoolExecutor, as_completed
Z99 = 2.326347874
_money_re = re.compile(r'[^0-9.\-]')
_nbsp = '\xa0'
import atexit
def _pause_on_exit():
    try:
        if sys.stdin and sys.stdin.isatty():
            input("Press Enter to close...")
    except Exception:
        pass
atexit.register(_pause_on_exit)
# -------------------- Utilities --------------------
def to_money(x):
    if x is None: return 0.0
    if isinstance(x, float) and math.isnan(x): return 0.0
    try: s = str(x)
    except Exception: return 0.0
    s = _money_re.sub('', s)
    if s in ('', '-', '.', '-.'): return 0.0
    try: return float(s)
    except Exception: return 0.0
def safe_str(x) -> str:
    if x is None: return ""
    if isinstance(x, float) and math.isnan(x): return ""
    try: s = str(x).strip()
    except Exception: return ""
    if s.lower() in ("nan","<na>","none"): return ""
    return s
def norm_name_fighter(s: str) -> str:
    s = safe_str(s).replace(_nbsp, ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s
def norm_user(u: str) -> str:
    s = safe_str(u).replace(_nbsp, ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s.casefold()
def log(msg): print(msg, flush=True)
def safe_filename(s: str) -> str:
    s = safe_str(s)
    s = re.sub(r'[^A-Za-z0-9 _\-]+', '', s)
    s = s.strip().replace(' ', '_')
    return s if s else "Contest"
# -------------------- DraftKings CSV helpers --------------------
def strip_entry_number(username: str) -> str:
    """Strip DraftKings entry number suffix like ' (2)' or ' (1/9)' from username.

    Examples:
        'donnytsunami (2)'    -> 'donnytsunami'
        'DHollis24 (1/9)'     -> 'DHollis24'
        'molecul0'            -> 'molecul0'
    """
    return re.sub(r'\s*\(\d+(?:/\d+)?\)\s*$', '', safe_str(username).strip())

def parse_dk_lineup_string(s: str) -> list:
    """Parse a DraftKings lineup string into individual fighter names.

    Input format:  'F Anthony Hernandez F Melquizael Costa F Zach Reese ...'
    Output:        ['Anthony Hernandez', 'Melquizael Costa', 'Zach Reese', ...]
    """
    s = safe_str(s).strip()
    if not s:
        return []
    # Split on whitespace followed by 'F' followed by whitespace (position delimiter)
    parts = re.split(r'\s+F\s+', s)
    # First part starts with "F " — strip the position prefix
    if parts and parts[0].startswith('F '):
        parts[0] = parts[0][2:]
    fighters = [p.strip() for p in parts if p.strip()]
    return fighters

def read_lineups_csv(csv_path: str):
    """Read lineups from a DraftKings contest export CSV file.

    Expected columns: Rank, EntryId, EntryName, TimeRemaining, Points, Lineup
    - EntryName: username; parenthetical entry numbers like (2) are stripped.
    - Lineup: 'F Fighter1 F Fighter2 F Fighter3 F Fighter4 F Fighter5 F Fighter6'

    Returns the same format as read_lineups_sheet: (fighters_array, users_list)
    """
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"DraftKings CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # Find columns by name (case-insensitive)
    col_map = {c.strip().lower(): c for c in df.columns}
    entry_col = col_map.get('entryname')
    lineup_col = col_map.get('lineup')

    if entry_col is None:
        raise ValueError(f"CSV file '{csv_path}' missing 'EntryName' column. "
                         f"Found columns: {list(df.columns)}")
    if lineup_col is None:
        raise ValueError(f"CSV file '{csv_path}' missing 'Lineup' column. "
                         f"Found columns: {list(df.columns)}")

    fighters_list = []
    users = []

    for _, row in df.iterrows():
        username = strip_entry_number(safe_str(row[entry_col]))
        lineup_str = safe_str(row[lineup_col])
        parsed = parse_dk_lineup_string(lineup_str)

        # Pad to 6 fighters or truncate if somehow more
        while len(parsed) < 6:
            parsed.append("")
        parsed = parsed[:6]

        fighters_list.append(parsed)
        users.append(username)

    fighters = np.array(fighters_list, dtype=object)

    # Keep rows if they have any fighter OR a username (blank lineup with username stays; scores 0)
    mask = np.array([(any(bool(fighters[i, c]) for c in range(6)) or bool(users[i]))
                     for i in range(len(fighters))])

    log(f"[csv] loaded {mask.sum():,} entries from {Path(csv_path).name}")
    return fighters[mask], [u for i, u in enumerate(users) if mask[i]]

# -------------------- Workbook readers --------------------
def read_entry_fee_fallback(wb_path: str) -> float:
    """Fallback location: DraftKings Fighter Pool!D2."""
    wb = load_workbook(wb_path, data_only=True, read_only=True)
    if "DraftKings Fighter Pool" not in wb.sheetnames:
        raise FileNotFoundError("Sheet 'DraftKings Fighter Pool' not found.")
    ws = wb["DraftKings Fighter Pool"]
    fee = to_money(ws["D2"].value)
    if fee <= 0:
        raise ValueError("Entry fee missing/invalid at DraftKings Fighter Pool!D2.")
    return float(fee)
def read_prefix_sheet_named(xl: pd.ExcelFile, prefix_sheet: Optional[str]):
    if not prefix_sheet:
        return None
    try:
        return pd.read_excel(xl, sheet_name=prefix_sheet, engine="openpyxl").iloc[:, :2]
    except Exception:
        return None
def read_payouts_named(xl: pd.ExcelFile, payouts_sheet: str, prefix_sheet: Optional[str]):
    # Per-rank payouts
    df = pd.read_excel(xl, sheet_name=payouts_sheet, engine="openpyxl").iloc[:, :2].copy()
    df = df.dropna(how="all")
    if df.shape[1] < 2:
        raise ValueError(f"Payouts sheet '{payouts_sheet}' must have at least 2 columns (Rank, Payout).")
    rank_col, money_col = df.columns[:2]
    df[rank_col]  = pd.to_numeric(df[rank_col], errors="coerce")
    df[money_col] = pd.to_numeric(df[money_col].map(to_money), errors="coerce").astype("float64")
    df = df.dropna(subset=[rank_col]).sort_values(rank_col)
    if df.empty:
        raise ValueError(f"Payouts sheet '{payouts_sheet}' is empty/invalid.")
    max_rank = int(df[rank_col].max())
    payouts = [0.0] * (max_rank + 1)
    for _, row in df.iterrows():
        r = int(row[rank_col])
        if 1 <= r <= max_rank:
            payouts[r] = float(row[money_col])
    last_paid = max([i for i, v in enumerate(payouts) if v > 0], default=0)
    sum_payouts = float(np.nansum(payouts))
    # Optional cumulative prefix sheet; validate it
    prefix = None
    dfp = read_prefix_sheet_named(xl, prefix_sheet)
    if dfp is not None:
        try:
            if dfp.shape[1] < 2:
                raise ValueError("Prefix sheet must have 2 columns (Rank, CumulativePayout).")
            rcol, pcol = dfp.columns[:2]
            dfp = dfp[[rcol, pcol]].copy()
            dfp[rcol] = pd.to_numeric(dfp[rcol], errors="coerce")
            dfp[pcol] = pd.to_numeric(dfp[pcol].map(to_money), errors="coerce").astype("float64")
            dfp = dfp.dropna(subset=[rcol]).sort_values(rcol)
            max_rank2 = int(dfp[rcol].max())
            M = max(max_rank, max_rank2)
            if M > max_rank:
                payouts += [0.0] * (M - max_rank)
                max_rank = M
            prefix = [0.0] * (max_rank + 1)
            for _, row in dfp.iterrows():
                r = int(row[rcol])
                if 1 <= r <= max_rank:
                    prefix[r] = float(row[pcol])
            # Forward-fill missing cumulative by adding per-rank payouts
            for r in range(1, max_rank + 1):
                if prefix[r] == 0.0:
                    prefix[r] = prefix[r-1] + (payouts[r] if r < len(payouts) else 0.0)
            if last_paid < len(prefix) and abs(prefix[last_paid] - sum_payouts) > 1e-6:
                log(f"[warn] Prefix sheet '{prefix_sheet}' doesn't match per-rank payouts in '{payouts_sheet}'; recomputing prefix from payouts.")
                prefix = None
        except Exception:
            prefix = None
    if prefix is None:
        prefix = [0.0] * len(payouts)
        for r in range(1, len(payouts)):
            prefix[r] = prefix[r-1] + payouts[r]
    return np.array(payouts, dtype=np.float64), np.array(prefix, dtype=np.float64), int(last_paid)
def read_fighter_map(xl: pd.ExcelFile):
    # Columns: Fighter (A), FightID (B), optional Score (C)
    df = pd.read_excel(xl, sheet_name="DraftKings Fighter Pool", engine="openpyxl")
    # Use first 3 columns if available, otherwise pad
    if df.shape[1] >= 3:
        df = df.iloc[:, :3]
        df.columns = ["Fighter", "FightID", "Score"]
    else:
        df = df.iloc[:, :2]
        df.columns = ["Fighter", "FightID"]
        df["Score"] = np.nan
    keep = ~(df["Fighter"].isna() & df["FightID"].isna())
    df = df.loc[keep].reset_index(drop=True)
    seen = {}
    fmap = {}
    fixed_scores = {}  # fighter_name -> fixed DK score (float)
    fighter_order = []  # ordered list of (name, fid, slot) for fight card
    for _, row in df.iterrows():
        name = norm_name_fighter(row["Fighter"])
        if not name:
            continue
        try:
            fid = int(pd.to_numeric(row["FightID"], errors="coerce"))
        except Exception:
            continue
        # Order in the pool defines slot 1 then slot 2 for a FightID
        if fid not in seen:
            seen[fid] = 1
            slot = 1
        else:
            seen[fid] += 1
            slot = 2
        fmap[name] = (fid, slot)
        # Check for fixed score in column C
        score_val = pd.to_numeric(row["Score"], errors="coerce")
        if not (score_val is None or (isinstance(score_val, float) and math.isnan(score_val))):
            fixed_scores[name] = float(score_val)
        fighter_order.append({"name": name, "fight_id": fid, "slot": slot})
    return fmap, fixed_scores, fighter_order
def read_lineups_sheet(xl: pd.ExcelFile, sheet_name: str):
    # columns A:G => F1..F6 + Username
    df = pd.read_excel(xl, sheet_name=sheet_name, engine="openpyxl", usecols="A:G")
    if df.shape[1] < 7:
        for _ in range(7 - df.shape[1]):
            df[df.columns[-1] + "_pad"] = ""
    df = df.iloc[:, :7]
    fighters = np.empty((len(df), 6), dtype=object)
    users = []
    for i in range(len(df)):
        row = df.iloc[i]
        for c in range(6):
            fighters[i, c] = safe_str(row.iloc[c])
        users.append(strip_entry_number(safe_str(row.iloc[6])))
    # Keep rows if they have any fighter OR a username (blank lineup with username stays; scores 0)
    mask = np.array([(any(bool(fighters[i, c]) for c in range(6)) or bool(users[i]))
                     for i in range(len(df))])
    return fighters[mask], [u for i, u in enumerate(users) if mask[i]]

def read_lineups(xl: pd.ExcelFile, lineups_ref: str, wb_path: str):
    """Read lineups from either a CSV file or an Excel sheet.

    If lineups_ref ends with '.csv', reads from a DraftKings CSV file
    (resolved relative to the workbook directory).
    Otherwise, reads from the named Excel sheet (original behavior).
    """
    if lineups_ref.lower().endswith('.csv'):
        csv_path = Path(wb_path).resolve().parent / lineups_ref
        return read_lineups_csv(str(csv_path))
    else:
        return read_lineups_sheet(xl, lineups_ref)

def read_contests(xl: pd.ExcelFile, wb_path: str):
    """
    If 'Contests' sheet exists, use it. Otherwise, default single contest.
    Required columns (case-insensitive):
      ContestName, LineupsSheet, PayoutsSheet, EntryFee
    Optional:
      PrefixSheet
    """
    if "Contests" not in xl.sheet_names:
        return [{
            "ContestName": "Main",
            "LineupsSheet": "Post Contest Sim",
            "PayoutsSheet": "Payouts",
            "PrefixSheet": "PayoutPrefix" if "PayoutPrefix" in xl.sheet_names else None,
            "EntryFee": read_entry_fee_fallback(wb_path),
        }]
    df = pd.read_excel(xl, sheet_name="Contests", engine="openpyxl")
    if df.empty:
        raise ValueError("Contests sheet exists but is empty.")
    cols = {c: safe_str(c).strip().casefold() for c in df.columns}
    inv = {v: k for k, v in cols.items()}
    def get_col(key, required=False):
        k = key.casefold()
        if k not in inv:
            if required:
                raise ValueError(f"Contests sheet missing required column: {key}")
            return None
        return inv[k]
    c_contest = get_col("ContestName", required=True)
    c_lineups = get_col("LineupsSheet", required=True)
    c_payouts = get_col("PayoutsSheet", required=True)
    c_prefix  = get_col("PrefixSheet", required=False)
    c_fee     = get_col("EntryFee", required=True)
    contests = []
    for _, r in df.iterrows():
        name    = safe_str(r[c_contest]) or "Contest"
        lineups = safe_str(r[c_lineups])
        payouts = safe_str(r[c_payouts])
        prefix  = safe_str(r[c_prefix]) if c_prefix else ""
        fee     = to_money(r[c_fee])
        if not lineups or not payouts:
            continue
        if fee <= 0:
            raise ValueError(f"Invalid EntryFee for contest '{name}' in Contests sheet.")
        contests.append({
            "ContestName": name,
            "LineupsSheet": lineups,
            "PayoutsSheet": payouts,
            "PrefixSheet": prefix if prefix else None,
            "EntryFee": float(fee),
        })
    if not contests:
        raise ValueError("No valid contest rows found in Contests sheet.")
    return contests
# -------------------- Copies / keys --------------------
def lineup_key(row6) -> str:
    names = [safe_str(x) for x in row6 if safe_str(x)]
    names.sort()
    return "|".join(names)
def compute_copies_and_keys(fighters_obj_array: np.ndarray):
    n = fighters_obj_array.shape[0]
    keys = [lineup_key(fighters_obj_array[i, :]) for i in range(n)]
    counts = {}
    for k in keys:
        counts[k] = counts.get(k, 0) + 1
    copies = np.array([counts[k] for k in keys], dtype=np.int32)
    return keys, copies
# -------------------- Fight sims --------------------
def load_fight_sims(xl: pd.ExcelFile, fight_ids):
    """Load per-fight sim columns (B=DK F1, C=DK F2). Fail if any fight sheet is missing/empty."""
    S1, S2, N = [], [], []
    missing_or_empty = []
    for fid in fight_ids:
        ok = True
        try:
            df = pd.read_excel(xl, sheet_name=str(fid), engine="openpyxl", usecols="A:C")
            s1 = pd.to_numeric(df.iloc[:, 1], errors="coerce").dropna().to_numpy(np.float32)
            s2 = pd.to_numeric(df.iloc[:, 2], errors="coerce").dropna().to_numpy(np.float32)
            m = int(min(len(s1), len(s2)))
            if m <= 0:
                ok = False
        except Exception:
            ok = False
            s1 = np.zeros(0, np.float32)
            s2 = np.zeros(0, np.float32)
            m = 0
        if not ok:
            missing_or_empty.append(fid)
        S1.append(s1[:m])
        S2.append(s2[:m])
        N.append(m)
    if missing_or_empty:
        raise ValueError(f"Missing or empty sim sheet(s) for FightID(s): {sorted(missing_or_empty)}")
    return S1, S2, N
def build_mats(fighters, fmap, id2idx):
    n = fighters.shape[0]
    F = len(id2idx)
    C1 = np.zeros((n, F), dtype=np.int8)
    C2 = np.zeros((n, F), dtype=np.int8)
    mapped6 = partial = empty = 0
    for i in range(n):
        mapped = 0
        for c in range(6):
            name = norm_name_fighter(fighters[i, c])
            t = fmap.get(name)
            if not t:
                continue  # unmapped => contributes 0
            fid, slot = t
            j = id2idx.get(fid)
            if j is None:
                continue
            if slot == 1:
                C1[i, j] += 1
            else:
                C2[i, j] += 1
            mapped += 1
        if mapped == 6:
            mapped6 += 1
        elif mapped == 0:
            empty += 1
        else:
            partial += 1
    log(f"[map] lineups mapped: 6/6={mapped6:,} | 1–5/6={partial:,} | 0/6={empty:,} (unmapped fighters score 0)")
    return C1, C2
# -------------------- Vectorized per-sim aggregation --------------------
# Uses np.sort + np.searchsorted instead of np.argsort + Python tie-walking.
# All payout/tie logic is handled with vectorized NumPy operations.
# -------------------- Worker --------------------
def worker_run(idx: int, npz_path: str, iters: int, batch: int, seed: int,
               mem_budget_mb: int = 256):
    rng = np.random.default_rng(seed)
    data = np.load(npz_path, allow_pickle=False)
    # shared fight sampling
    S1_stack = data["S1_stack"]
    S2_stack = data["S2_stack"]
    N = data["N"].astype(np.int64)
    F = int(data["F"])
    # contests packed
    K = int(data["K"])
    last_paid = data["last_paid"].astype(np.int64)      # (K,)
    prefix_mat = data["prefix_mat"].astype(np.float64)  # (K, max_rank+1)
    n_list = data["n_list"].astype(np.int64)            # (K,)
    offsets = data["offsets"].astype(np.int64)          # (K+1,)
    C1_concat = data["C1_concat"].astype(np.int8)       # (sum_n, F)
    C2_concat = data["C2_concat"].astype(np.int8)       # (sum_n, F)
    # portfolio tracking
    user_map_concat = data["user_map_concat"].astype(np.int64)
    num_users = int(data["num_users"])
    user_total_fees = data["user_total_fees"].astype(np.float64)
    user_contest_fees = data["user_contest_fees"].astype(np.float64)  # (K, num_users)
    # per-contest user maps
    user_map_list = []
    for k in range(K):
        a = int(offsets[k]); b = int(offsets[k+1])
        user_map_list.append(user_map_concat[a:b])
    # per-contest transposed matrices
    C1T_list = []
    C2T_list = []
    for k in range(K):
        a = int(offsets[k]); b = int(offsets[k+1])
        C1T_list.append(C1_concat[a:b].T.astype(np.float32, copy=False))  # (F x n_k)
        C2T_list.append(C2_concat[a:b].T.astype(np.float32, copy=False))
    # accumulators
    sum_scores = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    sumsq_scores = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    total_payout = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    wins = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    win_total = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    cashes = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    seconds = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    thirds = [np.zeros(int(n_list[k]), dtype=np.float64) for k in range(K)]
    # Portfolio outcome tracking: store per-user net profit for each iteration
    user_outcomes = np.zeros((iters, num_users), dtype=np.float32)
    user_outcomes_per_contest = np.zeros((K, iters, num_users), dtype=np.float32)
    iter_cursor = 0  # tracks which iteration we're writing to in user_outcomes
    done_total = 0
    while done_total < iters:
        B = min(batch, iters - done_total)
        bytes_budget = int(mem_budget_mb) * (1 << 20)
        bytes_per_score = 4  # float32 matmul output
        max_n = int(n_list.max()) if len(n_list) else 1
        micro_b = max(1, min(B, bytes_budget // max(1, (max_n * bytes_per_score))))
        off = 0
        while off < B:
            m = min(micro_b, B - off)
            # sample fights ONCE for m rows
            s1 = np.empty((m, F), dtype=np.float32)
            s2 = np.empty((m, F), dtype=np.float32)
            for f in range(F):
                idxs = rng.integers(0, N[f], size=m, dtype=np.int64)
                s1[:, f] = S1_stack[f, idxs]
                s2[:, f] = S2_stack[f, idxs]
            # per-user payout accumulator for this micro-batch
            user_payout_batch = np.zeros((m, num_users), dtype=np.float64)
            contest_user_payouts = np.zeros((K, m, num_users), dtype=np.float64)
            # evaluate each contest
            for k in range(K):
                C1T = C1T_list[k]
                C2T = C2T_list[k]
                prefix = prefix_mat[k]
                lp = int(last_paid[k])
                n_k = int(n_list[k])
                scores = s1 @ C1T + s2 @ C2T  # (m x n_k) — FAST matmul
                scores_f64 = scores.astype(np.float64)
                # Vectorized score accumulation across entire micro-batch
                sum_scores[k] += scores_f64.sum(axis=0)
                sumsq_scores[k] += np.einsum('ij,ij->j', scores_f64, scores_f64)
                # Per-iteration payout distribution via sort + searchsorted
                for i in range(m):
                    sc = scores_f64[i]
                    neg_sc = -sc
                    neg_sorted = np.sort(neg_sc)  # ascending = highest scores first
                    # Vectorized rank computation
                    left = np.searchsorted(neg_sorted, neg_sc, side='left')
                    right = np.searchsorted(neg_sorted, neg_sc, side='right')
                    group_sizes = (right - left).astype(np.float64)
                    # Tie-split payouts: prefix[end] - prefix[start] / group_size
                    safe_left = np.minimum(left, lp)
                    safe_right = np.minimum(right, lp)
                    payout = (prefix[safe_right] - prefix[safe_left]) / np.maximum(group_sizes, 1.0)
                    total_payout[k] += payout
                    # Portfolio: accumulate per-user payouts for this iteration
                    bc = np.bincount(user_map_list[k], weights=payout, minlength=num_users)
                    user_payout_batch[i] += bc
                    contest_user_payouts[k, i] = bc
                    # Cashes
                    is_cash = payout > 0.0
                    cashes[k][is_cash] += 1.0
                    # 1st place: entries matching top score
                    val_1st = neg_sorted[0]
                    is_win = neg_sc == val_1st
                    wins[k][is_win] += 1.0
                    win_total[k] += np.where(is_win, payout, 0.0)
                    # 2nd place: next score group after 1st
                    g1_end = np.searchsorted(neg_sorted, val_1st, side='right')
                    if g1_end < n_k:
                        val_2nd = neg_sorted[g1_end]
                        seconds[k][neg_sc == val_2nd] += 1.0
                        # 3rd place: next score group after 2nd
                        g2_end = np.searchsorted(neg_sorted, val_2nd, side='right')
                        if g2_end < n_k:
                            val_3rd = neg_sorted[g2_end]
                            thirds[k][neg_sc == val_3rd] += 1.0
            # Store per-user net profit (payouts - entry fees) for this micro-batch
            user_payout_batch -= user_total_fees  # subtract fees to get net profit
            user_outcomes[iter_cursor:iter_cursor + m] = user_payout_batch.astype(np.float32)
            # Per-contest net profit
            for k in range(K):
                contest_user_payouts[k] -= user_contest_fees[k]
            user_outcomes_per_contest[:, iter_cursor:iter_cursor + m, :] = contest_user_payouts.astype(np.float32)
            iter_cursor += m
            off += m
        done_total += B
    # Save user_outcomes to temp files to avoid pipe size limits on Windows
    outcomes_path = npz_path + f".user_outcomes_{idx}.npy"
    np.save(outcomes_path, user_outcomes)
    per_contest_path = npz_path + f".user_outcomes_per_contest_{idx}.npy"
    np.save(per_contest_path, user_outcomes_per_contest)
    return (idx, done_total, sum_scores, sumsq_scores, total_payout, wins, win_total, cashes, seconds, thirds, outcomes_path, per_contest_path)
# -------------------- Pack workbook once --------------------
def pack_npz_multi(wb_path: str, temp_dir: Path):
    xl = pd.ExcelFile(wb_path, engine="openpyxl")
    contests = read_contests(xl, wb_path)
    fmap, fixed_scores, fighter_order = read_fighter_map(xl)
    # Build fight_card from fighter_order (preserves DK Fighter Pool sheet order)
    fight_card_map = {}
    fight_card_order = []
    for f in fighter_order:
        fid = f["fight_id"]
        if fid not in fight_card_map:
            fight_card_map[fid] = {"fight_id": fid, "fighter1": None, "fighter2": None,
                                   "fighter1_score": None, "fighter2_score": None}
            fight_card_order.append(fid)
        if f["slot"] == 1:
            fight_card_map[fid]["fighter1"] = f["name"]
            fight_card_map[fid]["fighter1_score"] = fixed_scores.get(f["name"])
        else:
            fight_card_map[fid]["fighter2"] = f["name"]
            fight_card_map[fid]["fighter2_score"] = fixed_scores.get(f["name"])
    fight_card = [fight_card_map[fid] for fid in fight_card_order]
    fights = sorted(set(fid for (fid, _) in fmap.values()))
    id2idx = {fid: i for i, fid in enumerate(fights)}
    # Build fight_fighters map FIRST to determine which fights have fixed scores
    fight_fighters = {}  # fid -> {slot: (name, score_or_None)}
    for name, (fid, slot) in fmap.items():
        if fid not in fight_fighters:
            fight_fighters[fid] = {}
        fight_fighters[fid][slot] = (name, fixed_scores.get(name))
    # Determine which fights are fully fixed (both fighters have scores)
    fixed_fights = set()
    for fid in fights:
        ff = fight_fighters.get(fid, {})
        f1 = ff.get(1)
        f2 = ff.get(2)
        if f1 and f2 and f1[1] is not None and f2[1] is not None:
            fixed_fights.add(fid)
    sim_fights = [fid for fid in fights if fid not in fixed_fights]
    # Only load sim sheets for fights that need them
    if sim_fights:
        sim_S1, sim_S2, sim_N = load_fight_sims(xl, sim_fights)
        sim_map = {fid: i for i, fid in enumerate(sim_fights)}
    # Build final S1/S2/N lists in fight order
    S1_list, S2_list, N_list = [], [], []
    for fid in fights:
        if fid in fixed_fights:
            ff = fight_fighters[fid]
            f1 = ff[1]
            f2 = ff[2]
            S1_list.append(np.array([f1[1]], dtype=np.float32))
            S2_list.append(np.array([f2[1]], dtype=np.float32))
            N_list.append(1)
            log(f"[fixed] fight {fid}: {f1[0]}={f1[1]:.2f}, {f2[0]}={f2[1]:.2f}")
        else:
            si = sim_map[fid]
            S1_list.append(sim_S1[si])
            S2_list.append(sim_S2[si])
            N_list.append(sim_N[si])
    fixed_count = len(fixed_fights)
    if fixed_count:
        log(f"[info] {fixed_count} fight(s) locked with fixed scores, {len(fights) - fixed_count} fight(s) simulated")
    F = len(fights)
    maxN = int(max(N_list)) if N_list else 1
    S1_stack = np.zeros((F, maxN), dtype=np.float32)
    S2_stack = np.zeros((F, maxN), dtype=np.float32)
    N = np.zeros(F, dtype=np.int64)
    for i in range(F):
        Ni = int(N_list[i]); N[i] = Ni
        S1_stack[i, :Ni] = S1_list[i][:Ni]
        S2_stack[i, :Ni] = S2_list[i][:Ni]
    contest_meta = []
    C1_blocks = []
    C2_blocks = []
    n_list = []
    entry_fees = []
    last_paid_list = []
    prefix_list = []
    user_map_blocks = []  # per-contest array mapping lineup_idx -> user_idx
    all_users_set = {}    # username -> user_idx (case-preserved, keyed by casefold)
    for c in contests:
        name = c["ContestName"]
        lineups_ref = c["LineupsSheet"]
        payouts_sheet = c["PayoutsSheet"]
        prefix_sheet = c.get("PrefixSheet", None)
        entry = float(c["EntryFee"])
        payouts_arr, prefix, last_paid = read_payouts_named(xl, payouts_sheet, prefix_sheet)
        prize_pool = float(prefix[last_paid])
        # Read lineups from CSV file or Excel sheet
        fighters, users = read_lineups(xl, lineups_ref, wb_path)
        lineup_keys, copies = compute_copies_and_keys(fighters)
        C1, C2 = build_mats(fighters, fmap, id2idx)
        contest_meta.append({
            "Contest": name,
            "LineupsSheet": lineups_ref,
            "PayoutsSheet": payouts_sheet,
            "PrefixSheet": prefix_sheet,
            "EntryFee": entry,
            "PrizePool": prize_pool,
            "fighters": fighters,
            "users": users,
            "lineup_keys": lineup_keys,
            "copies": copies,
            "n": int(fighters.shape[0]),
            "last_paid": int(last_paid),
            "payouts_array": payouts_arr.tolist(),
        })
        # Build per-lineup user index mapping for portfolio tracking
        n_k = int(fighters.shape[0])
        umap_k = np.zeros(n_k, dtype=np.int64)
        for i_lu, u in enumerate(users):
            ukey = u.casefold() if u else ""
            if ukey not in all_users_set:
                all_users_set[ukey] = (len(all_users_set), u)  # (idx, display_name)
            umap_k[i_lu] = all_users_set[ukey][0]
        user_map_blocks.append(umap_k)
        C1_blocks.append(C1)
        C2_blocks.append(C2)
        n_list.append(n_k)
        entry_fees.append(float(entry))
        last_paid_list.append(int(last_paid))
        prefix_list.append(prefix.astype(np.float64, copy=False))
    # Build user arrays for portfolio tracking
    num_users = len(all_users_set)
    user_display_names = [""] * num_users
    for ukey, (uidx, dname) in all_users_set.items():
        user_display_names[uidx] = dname
    # Per-user total entry fees across all contests + per-contest fees
    user_total_fees = np.zeros(num_users, dtype=np.float64)
    user_contest_fees = np.zeros((len(contest_meta), num_users), dtype=np.float64)
    for k_idx, meta in enumerate(contest_meta):
        entry_k = float(meta["EntryFee"])
        for i_lu in range(len(user_map_blocks[k_idx])):
            uid = user_map_blocks[k_idx][i_lu]
            user_total_fees[uid] += entry_k
            user_contest_fees[k_idx, uid] += entry_k
    user_map_concat = np.concatenate(user_map_blocks).astype(np.int64)
    log(f"[info] portfolio tracking: {num_users:,} unique users across {len(contest_meta)} contests")
    # pack prefix into matrix with padding
    max_rank = max(len(p) for p in prefix_list) - 1
    prefix_mat = np.zeros((len(prefix_list), max_rank + 1), dtype=np.float64)
    for k, p in enumerate(prefix_list):
        L = len(p)
        prefix_mat[k, :L] = p
        if L < max_rank + 1:
            prefix_mat[k, L:] = p[-1]  # keep cumulative flat after end
    # concatenate lineups
    offsets = [0]
    for n in n_list:
        offsets.append(offsets[-1] + n)
    offsets = np.array(offsets, dtype=np.int64)
    C1_concat = np.vstack(C1_blocks).astype(np.int8, copy=False)
    C2_concat = np.vstack(C2_blocks).astype(np.int8, copy=False)
    npz_path = temp_dir / f"post_bundle_multi_{uuid.uuid4().hex}.npz"
    np.savez_compressed(
        npz_path,
        # shared fights
        S1_stack=S1_stack, S2_stack=S2_stack, N=N,
        F=np.array(F, dtype=np.int64),
        # contests
        K=np.array(len(contest_meta), dtype=np.int64),
        entry_fees=np.array(entry_fees, dtype=np.float64),
        last_paid=np.array(last_paid_list, dtype=np.int64),
        prefix_mat=prefix_mat,
        n_list=np.array(n_list, dtype=np.int64),
        offsets=offsets,
        C1_concat=C1_concat,
        C2_concat=C2_concat,
        # portfolio tracking
        user_map_concat=user_map_concat,
        num_users=np.array(num_users, dtype=np.int64),
        user_total_fees=user_total_fees,
        user_contest_fees=user_contest_fees,
    )
    return str(npz_path), contest_meta, fight_card, user_display_names
# -------------------- Main --------------------
def ask_int(prompt: str, default: int, min_val: int = 1) -> int:
    while True:
        s = input(f"{prompt} [{default}]: ").strip()
        if not s:
            return default
        try:
            v = int(s)
            if v >= min_val:
                return v
        except Exception:
            pass
        print(f"Please enter an integer >= {min_val}.")
def main():
    try:
        os.chdir(Path(__file__).resolve().parent)
    except Exception:
        pass
    DEFAULT_WB = "Post Contest Sim.xlsm"
    DEFAULT_ITERS = 200000
    DEFAULT_WORKERS = 4
    DEFAULT_BATCH = 32768
    DEFAULT_STEP = 100000
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--workbook")
    ap.add_argument("--iters", type=int)
    ap.add_argument("--workers", type=int)
    ap.add_argument("--batch", type=int)
    ap.add_argument("--progress_step", type=int)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--out")
    args, _ = ap.parse_known_args()
    wb = args.workbook or DEFAULT_WB
    if not Path(wb).exists():
        s = input(f"Workbook not found ({wb}). Enter full path or press Enter to abort: ").strip()
        if s:
            wb = s
    if not Path(wb).exists():
        print(f"Workbook not found: {wb}")
        try: input("Press Enter to close...")
        except Exception: pass
        return
    iters = args.iters if (args.iters and args.iters > 0) else ask_int("Number of iterations", DEFAULT_ITERS)
    workers = args.workers if (args.workers and args.workers > 0) else ask_int("Number of worker processes (try 4, 6, 8)", DEFAULT_WORKERS)
    if args.batch and args.batch > 0:
        batch = args.batch
    else:
        print("Batch size options: 8192, 16384, 32768, 65536, 131072")
        batch = ask_int("Batch size", DEFAULT_BATCH)
    step = args.progress_step if (args.progress_step and args.progress_step > 0) else ask_int("Progress step (iters per update)", DEFAULT_STEP)
    if args.seed is None:
        seed = int.from_bytes(os.urandom(8), "little") & 0x7FFFFFFFFFFFFFFF
        log(f"[info] Using RANDOM seed: {seed}")
    else:
        seed = int(args.seed)
        log(f"[info] Using FIXED seed: {seed}")
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.out or f"PostContest_Results_Multi_{iters}_{seed}_{ts}.csv"
    out_dir = str(Path(out_path).resolve().parent)
    base_stem = Path(out_path).stem
    t0 = time.time()
    with tempfile.TemporaryDirectory() as td:
        bundle, contest_meta, fight_card, user_display_names = pack_npz_multi(wb, Path(td))
        K = len(contest_meta)
        log(f"[info] contests={K} | workers={workers} | batch={batch}")
        for c in contest_meta:
            log(f"  - {c['Contest']}: lineups={c['n']:,} | entry=${c['EntryFee']:.2f} | prize_pool=${c['PrizePool']:.2f} | last_paid={c['last_paid']}")
        log("[info] using vectorized sort+searchsorted payout engine")
        # Build chunk plan
        chunks = []
        remaining = iters
        while remaining > 0:
            c = step if remaining > step else remaining
            chunks.append(c)
            remaining -= c
        rng = np.random.default_rng(seed)
        child_seeds = rng.integers(0, 2**63 - 1, size=len(chunks), dtype=np.int64)
        # per contest accumulators
        sum_scores   = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        sumsq_scores = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        total_payout = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        wins         = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        win_total    = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        cashes       = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        seconds      = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        thirds       = [np.zeros(c["n"], dtype=np.float64) for c in contest_meta]
        done_iters = 0
        all_user_outcomes = []  # collect per-worker user outcome arrays
        all_user_outcomes_per_contest = []  # collect per-worker per-contest outcome arrays
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(worker_run, i, bundle, int(chunks[i]), int(batch), int(child_seeds[i]))
                    for i in range(len(chunks))]
            for fut in as_completed(futs):
                (idx, its, s_list, ss_list, tp_list, w_list, wt_list, c_list, sec_list, thi_list, outcomes_path, per_contest_path) = fut.result()
                for k in range(K):
                    sum_scores[k]   += s_list[k]
                    sumsq_scores[k] += ss_list[k]
                    total_payout[k] += tp_list[k]
                    wins[k]         += w_list[k]
                    win_total[k]    += wt_list[k]
                    cashes[k]       += c_list[k]
                    seconds[k]      += sec_list[k]
                    thirds[k]       += thi_list[k]
                all_user_outcomes.append(np.load(outcomes_path))
                os.remove(outcomes_path)
                all_user_outcomes_per_contest.append(np.load(per_contest_path))
                os.remove(per_contest_path)
                done_iters += its
                rate = done_iters / max(1e-9, (time.time() - t0))
                log(f"[progress] {done_iters:,}/{iters:,} ({done_iters/iters:,.1%}) | {rate:,.0f} it/s")
        elapsed = time.time() - t0
        log(f"[timing] total wall: {elapsed:,.2f}s | iters/sec: {iters/elapsed:,.0f}")
        # Write output per contest (one CSV each)
        for k, meta in enumerate(contest_meta):
            entry = float(meta["EntryFee"])
            n = int(meta["n"])
            mean = sum_scores[k] / max(1, iters)
            var  = (sumsq_scores[k] / max(1, iters)) - (mean * mean)
            var[var < 0] = 0.0
            sd   = np.sqrt(var * iters / max(1, iters - 1)) if iters > 1 else np.zeros_like(var)
            p99  = mean + Z99 * sd
            EV   = total_payout[k] / max(1, iters)
            NetEV = EV - entry
            ROI   = np.where(entry > 0, NetEV / entry * 100.0, 0.0)
            WinPct = wins[k] / max(1, iters) * 100.0
            SecondPct = seconds[k] / max(1, iters) * 100.0
            ThirdPct = thirds[k] / max(1, iters) * 100.0
            CashPct = cashes[k] / max(1, iters) * 100.0
            AvgWinPayout = np.where(wins[k] > 0, win_total[k] / np.maximum(wins[k], 1.0), 0.0)
            fighters = meta["fighters"]
            users    = meta["users"]
            keys     = meta["lineup_keys"]
            copies   = meta["copies"]
            rows = []
            for i in range(n):
                rows.append([
                    meta["Contest"],
                    i+1, users[i], keys[i], int(copies[i]),
                    safe_str(fighters[i,0]), safe_str(fighters[i,1]), safe_str(fighters[i,2]),
                    safe_str(fighters[i,3]), safe_str(fighters[i,4]), safe_str(fighters[i,5]),
                    float(entry), float(AvgWinPayout[i]),
                    float(EV[i]), float(NetEV[i]), float(ROI[i]), float(WinPct[i]),
                    float(SecondPct[i]), float(ThirdPct[i]),
                    float(CashPct[i]),
                    float(mean[i]), float(sd[i]), float(p99[i]),
                    float(total_payout[k][i])
                ])
            cols = [
                "Contest",
                "Row","Username","LineupKey","Copies",
                "F1","F2","F3","F4","F5","F6",
                "EntryFee","AvgWinPayout",
                "EV","NetEV","ROI%","WinPct","SecondPct","ThirdPct","CashPct",
                "MeanScore","SDScore","P99Score",
                "TotalPayout"
            ]
            df_k = pd.DataFrame(rows, columns=cols)
            cname = safe_filename(meta["Contest"])
            per_path = os.path.join(out_dir, f"{base_stem}_{cname}.csv")
            df_k.to_csv(per_path, index=False, encoding="utf-8")
            log(f"[done] wrote {per_path}")
            # Write companion meta JSON with fight_card and payouts for What If feature
            meta_json_path = os.path.join(out_dir, f"{base_stem}_{cname}_meta.json")
            meta_json = {
                "fight_card": fight_card,
                "payouts": meta["payouts_array"],
                "entry_fee": float(entry),
            }
            with open(meta_json_path, 'w', encoding='utf-8') as mf:
                json.dump(meta_json, mf, indent=2)
            log(f"[done] wrote {meta_json_path}")
            # sanity check per contest
            prize_pool = float(meta["PrizePool"])
            avg_ev = float(EV.mean()) if n else 0.0
            expected_ev = float(prize_pool / n) if n else 0.0
            total_paid_per_contest = float(total_payout[k].sum() / iters) if iters else 0.0
            log(f"[check:{meta['Contest']}] prize_pool=${prize_pool:,.2f} | entries={n:,} | "
                f"avg EV=${avg_ev:.4f} (expected ${expected_ev:.4f}) | "
                f"avg total paid=${total_paid_per_contest:,.2f}")
            if abs(avg_ev - expected_ev) > 1e-4 or abs(total_paid_per_contest - prize_pool) > 1e-2:
                log(f"[warn:{meta['Contest']}] EV/payout conservation check failed; verify payout prefix/tie-split logic and inputs.")
        # Compute and write portfolio percentile distributions
        combined_outcomes = np.vstack(all_user_outcomes)  # (total_iters, num_users)
        combined_per_contest = np.concatenate(all_user_outcomes_per_contest, axis=1)  # (K, total_iters, num_users)
        del all_user_outcomes, all_user_outcomes_per_contest  # free memory
        num_users = combined_outcomes.shape[1]
        percentile_points = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 99]
        contest_names = [meta["Contest"] for meta in contest_meta]

        def compute_pctiles(sorted_data):
            pctiles = {"min": round(float(sorted_data[0]), 2), "max": round(float(sorted_data[-1]), 2)}
            for p in percentile_points:
                idx_p = int(np.floor(p / 100.0 * len(sorted_data)))
                idx_p = min(idx_p, len(sorted_data) - 1)
                pctiles[f"p{p}"] = round(float(sorted_data[idx_p]), 2)
            return pctiles

        portfolio_percentiles = {"contests": contest_names, "users": {}}
        for u_idx in range(num_users):
            uname = user_display_names[u_idx]
            if not uname:
                continue
            user_data = combined_outcomes[:, u_idx]
            sorted_data = np.sort(user_data)
            user_entry = {"all": compute_pctiles(sorted_data), "by_contest": {}}
            # Per-contest percentiles
            for k_idx, cname in enumerate(contest_names):
                contest_data = combined_per_contest[k_idx, :, u_idx]
                # Only include if user has entries in this contest (fees > 0)
                if np.any(contest_data != 0):
                    sorted_contest = np.sort(contest_data)
                    user_entry["by_contest"][cname] = compute_pctiles(sorted_contest)
            portfolio_percentiles["users"][uname] = user_entry
        del combined_outcomes, combined_per_contest  # free memory
        pct_path = os.path.join(out_dir, f"{base_stem}_portfolio_percentiles.json")
        with open(pct_path, 'w', encoding='utf-8') as pf:
            json.dump(portfolio_percentiles, pf, indent=2)
        log(f"[done] wrote portfolio percentiles: {pct_path} ({num_users} users, {K} contests)")
    try:
        input("Press Enter to close...")
    except Exception:
        pass
# ---- safe entrypoint for double-click on Windows ----
def _safe_main():
    try:
        import multiprocessing as mp
        try:
            mp.set_start_method("spawn", force=True)
        except Exception:
            pass
        mp.freeze_support()
    except Exception:
        pass
    try:
        main()
    except Exception as e:
        print("FATAL:", e)
        import traceback
        print(traceback.format_exc())
        try:
            input("Press Enter to close...")
        except Exception:
            pass
if __name__ == "__main__":
    _safe_main()
