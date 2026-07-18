#!/usr/bin/env python3
"""
scrape_dk_contests.py — pull the DraftKings MMA lobby and per-contest payout
structures straight from DraftKings' public JSON endpoints, so you no longer
have to read/enter payout tables by hand.

Two public endpoints are used (no login required):

  1. https://www.draftkings.com/lobby/getcontests?sport=MMA
        The whole MMA lobby. Each contest gives us name, id, entry fee,
        prize pool, current/max entries and the draft group.

  2. https://api.draftkings.com/contests/v1/contests/<id>?format=json
        Contest detail. Its `payoutSummary` is a list of tiers
        {minPosition, maxPosition, value}. Expanding those tiers into a
        per-rank list is exactly the "Payouts" sheet the sim needs.

Typical use
-----------
  # See the whole MMA slate as a table, and save it to dk_lobby.json:
  python scrape_dk_contests.py

  # Also fetch + expand payouts for the contests you care about:
  python scrape_dk_contests.py --payouts --min-fee 5           # everything >= $5
  python scrape_dk_contests.py --payouts --contains Thunderdome # by name
  python scrape_dk_contests.py --payouts --ids 191819063,192029298

  # Forgot to scrape and a contest FILLED UP (so DK hid it from the lobby)?
  # Pass its id (or its draft/gamecenter URL) — it's fetched directly by id:
  python scrape_dk_contests.py --payouts --ids 192029298,192029306

Already-saved contests are skipped (a payout table never changes once posted);
pass --refresh to re-scrape them anyway.

Payouts are written one file per contest to  dk_payouts/<id>.json  as:
  { contest_id, name, entry_fee, entrants, max_entries, prize_pool,
    last_paid_rank, total_payout, payouts: [rank1, rank2, ...] }
where payouts[0] is rank 1's prize (0 = unpaid rank).

Nothing here overwrites your existing contests.json or the sim output files;
it only writes dk_lobby.json and dk_payouts/. Wiring the payouts into
PostContestSim.py is a separate, opt-in step.
"""

import argparse
import json
import os
import re
import sys
import time

import requests

LOBBY_URL = "https://www.draftkings.com/lobby/getcontests?sport={sport}"
DETAIL_URL = "https://api.draftkings.com/contests/v1/contests/{cid}?format=json"

# A normal browser UA keeps the endpoints happy.
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
}

MONEY_RE = re.compile(r"[-+]?[\d,]*\.?\d+")


def get_json(url, tries=3, pause=1.5):
    """GET with a couple of polite retries; returns parsed JSON or raises."""
    last = None
    for attempt in range(tries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001 - surface after retries
            last = e
            if attempt < tries - 1:
                time.sleep(pause * (attempt + 1))
    raise RuntimeError(f"GET failed after {tries} tries: {url}\n  {last}")


def parse_money(s):
    """'$200,000.00' -> 200000.0 ; returns 0.0 if nothing numeric."""
    if s is None:
        return 0.0
    m = MONEY_RE.search(str(s).replace(",", ""))
    return float(m.group()) if m else 0.0


def normalize_contest(c):
    """Pull the fields we care about out of a getcontests entry."""
    attr = c.get("attr") or {}
    return {
        "id": c.get("id"),
        "name": c.get("n", ""),
        "entry_fee": float(c.get("a", 0) or 0),
        "prize_pool": float(c.get("po", 0) or 0),
        "entrants": int(c.get("nt", 0) or 0),
        "max_entries": int(c.get("m", 0) or 0),
        "max_per_user": int(c.get("mec", 0) or 0),
        "draft_group": c.get("dg"),
        "game_type": c.get("gameType", ""),
        "guaranteed": str(attr.get("IsGuaranteed", "")).lower() == "true",
        "starts": c.get("sdstring", ""),
    }


def fetch_lobby(sport="MMA"):
    data = get_json(LOBBY_URL.format(sport=sport))
    contests = [normalize_contest(c) for c in data.get("Contests", [])]
    # Classic MMA only (skip Pick6 / Tiers / snake if they appear).
    contests = [c for c in contests if c["game_type"] == "Classic"]
    contests.sort(key=lambda c: c["entry_fee"], reverse=True)
    return contests


def tier_value(tier):
    """Cash value of one payoutSummary tier (0 for ticket-only tiers)."""
    for pd in tier.get("payoutDescriptions", []) or []:
        if pd.get("payoutDescriptionType") == "Text" and pd.get("value"):
            return float(pd["value"])
    cash = (tier.get("tierPayoutDescriptions") or {}).get("Cash")
    return parse_money(cash)


def fetch_payouts(cid):
    """Return a per-rank payout list [rank1, rank2, ...] for one contest."""
    data = get_json(DETAIL_URL.format(cid=cid))
    detail = data.get("contestDetail", data)
    tiers = detail.get("payoutSummary") or detail.get("PayoutSummary") or []
    if not tiers:
        return [], 0, 0.0
    last_rank = max(int(t.get("maxPosition", 0)) for t in tiers)
    payouts = [0.0] * (last_rank + 1)  # index 0 unused; rank r at payouts[r]
    for t in tiers:
        val = tier_value(t)
        lo, hi = int(t.get("minPosition", 0)), int(t.get("maxPosition", 0))
        for r in range(lo, hi + 1):
            if 1 <= r < len(payouts):
                payouts[r] = val
    per_rank = payouts[1:]  # payouts[0] -> rank 1
    last_paid = max((i + 1 for i, v in enumerate(per_rank) if v > 0), default=0)
    return per_rank, last_paid, float(sum(per_rank))


def fetch_contest_by_id(cid):
    """Normalize one contest straight from the detail endpoint. Works even when
    the contest has left the lobby — a contest that FILLED to capacity is hidden
    from the lobby feed just like a finished one, so if it filled before you ran
    the scraper its payouts are still recoverable by id. Returns a
    normalize_contest-shaped dict, or None if the id isn't retrievable."""
    try:
        data = get_json(DETAIL_URL.format(cid=cid))
    except Exception:  # noqa: BLE001
        return None
    d = data.get("contestDetail", data) or {}
    name = d.get("name")
    if not name:
        return None
    return {
        "id": cid,
        "name": name,
        "entry_fee": float(d.get("entryFee", 0) or 0),
        "prize_pool": 0.0,          # derived from the expanded payouts
        "entrants": int(d.get("entries", 0) or 0),
        "max_entries": int(d.get("maximumEntries", 0) or 0),
        "max_per_user": int(d.get("maximumEntriesPerUser", 0) or 0),
        "draft_group": d.get("draftGroupId"),
        "game_type": "Classic",
        "guaranteed": False,
        "starts": "",
    }


def select(contests, args):
    out = contests
    if args.ids:
        want = {s.strip() for s in args.ids.split(",") if s.strip()}
        out = [c for c in out if str(c["id"]) in want]
    if args.contains:
        needle = args.contains.lower()
        out = [c for c in out if needle in c["name"].lower()]
    if args.min_fee is not None:
        out = [c for c in out if c["entry_fee"] >= args.min_fee]
    if args.guaranteed:
        out = [c for c in out if c["guaranteed"]]
    if args.limit:
        out = out[: args.limit]
    return out


def fmt_money(v):
    return f"${v:,.0f}" if v >= 1 or v == 0 else f"${v:,.2f}"


def print_table(contests):
    print(f"\n{'ENTRY':>9}  {'PRIZES':>10}  {'ENTRIES':>13}  {'ID':>10}  NAME")
    print("-" * 88)
    for c in contests:
        entries = f"{c['entrants']:,}/{c['max_entries']:,}"
        print(f"{fmt_money(c['entry_fee']):>9}  {fmt_money(c['prize_pool']):>10}  "
              f"{entries:>13}  {c['id']:>10}  {c['name']}")
    print(f"\n{len(contests)} contest(s).")


def main():
    ap = argparse.ArgumentParser(description="Scrape DraftKings MMA contests + payouts.")
    ap.add_argument("--sport", default="MMA")
    ap.add_argument("--payouts", action="store_true",
                    help="also fetch + expand payout tables for the selected contests")
    ap.add_argument("--min-fee", type=float, default=None, help="only contests with entry fee >= this")
    ap.add_argument("--contains", help="only contests whose name contains this (case-insensitive)")
    ap.add_argument("--ids", help="comma-separated contest IDs (or draft/gamecenter URLs). "
                    "IDs not in the lobby (contests that filled to capacity and got hidden) "
                    "are fetched directly by id.")
    ap.add_argument("--guaranteed", action="store_true", help="only guaranteed (GPP) contests")
    ap.add_argument("--limit", type=int, default=None, help="keep at most N (after sorting by entry fee)")
    ap.add_argument("--out-dir", default="dk_payouts", help="folder for per-contest payout files")
    ap.add_argument("--pause", type=float, default=0.6, help="seconds between payout requests (be polite)")
    ap.add_argument("--refresh", action="store_true",
                    help="re-scrape even contests already saved in out-dir (default: skip cached)")
    args = ap.parse_args()

    # Pull bare ids out of whatever was passed to --ids (bare id or a URL).
    requested_ids = []
    if args.ids:
        for tok in args.ids.split(","):
            m = re.search(r"(\d{5,})", tok)
            if m and m.group(1) not in requested_ids:
                requested_ids.append(m.group(1))

    try:
        lobby = fetch_lobby(args.sport)
    except Exception as e:  # noqa: BLE001
        if not requested_ids:
            print(f"Could not load the {args.sport} lobby: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"[warn] lobby unavailable ({e}); continuing with --ids only.", file=sys.stderr)
        lobby = []

    if lobby:
        with open("dk_lobby.json", "w", encoding="utf-8") as f:
            json.dump(lobby, f, indent=2)

    if not lobby and not requested_ids:
        print(f"No Classic {args.sport} contests are open right now.")
        return

    picked = select(lobby, args)
    # Recover any requested ids missing from the lobby (filled/hidden) by id.
    if requested_ids:
        have = {str(c["id"]) for c in picked}
        for cid in requested_ids:
            if cid in have:
                continue
            c = fetch_contest_by_id(cid)
            if c:
                picked.append(c)
                print(f"  (by id) {cid}: {c['name']}")
            else:
                print(f"  !  {cid}: couldn't fetch by id (bad id, or DK returned no detail)")

    if lobby:
        print_table(picked)
        print("Full lobby saved to dk_lobby.json")

    if not args.payouts:
        print("\n(Add --payouts to also fetch payout tables for the contests above.)")
        return

    os.makedirs(args.out_dir, exist_ok=True)
    print(f"\nFetching payouts for {len(picked)} contest(s) -> {args.out_dir}/")
    ok = skipped = 0
    for c in picked:
        out_path = os.path.join(args.out_dir, f"{c['id']}.json")
        # Cache: a contest we've already saved never changes its payout table, so
        # skip it. --refresh forces a re-scrape.
        if os.path.exists(out_path) and not args.refresh:
            print(f"  cached {c['name']} ({c['id']})")
            skipped += 1
            continue
        try:
            per_rank, last_paid, total = fetch_payouts(c["id"])
        except Exception as e:  # noqa: BLE001
            print(f"  ! {c['name']} ({c['id']}): {e}")
            continue
        if not per_rank:
            print(f"  ? {c['name']} ({c['id']}): no payout summary (not finalized yet?)")
            continue
        # by-id contests carry no advertised pool; use the expanded total.
        pool = c["prize_pool"] if c["prize_pool"] > 0 else total
        rec = {
            "contest_id": c["id"], "name": c["name"], "entry_fee": c["entry_fee"],
            "entrants": c["entrants"], "max_entries": c["max_entries"],
            "prize_pool": pool, "last_paid_rank": last_paid,
            "total_payout": round(total, 2), "payouts": [round(v, 2) for v in per_rank],
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rec, f, indent=2)
        # Sanity check: expanded payouts should roughly match the advertised pool.
        flag = "" if abs(total - pool) <= max(1.0, 0.02 * pool) else "  <-- differs from pool"
        print(f"  ok {c['name']}: pays top {last_paid:,}, total {fmt_money(total)}{flag}")
        ok += 1
        time.sleep(args.pause)
    tail = f", {skipped} cached" if skipped else ""
    print(f"\nDone. {ok} payout file(s) written to {args.out_dir}/{tail}")


if __name__ == "__main__":
    main()
