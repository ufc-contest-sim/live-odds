#!/usr/bin/env python3
"""
Comprehensive UFC stats scraper — pulls every stat from every fight in UFC history.

Scrapes ufcstats.com:
  1. All completed events
  2. All fights per event (summary + detail pages)
  3. Per-fighter totals AND per-round breakdowns
  4. Sig strike breakdowns by target (head/body/leg) and position (distance/clinch/ground)

Data is cached in ufc_scrape_cache.json for resumability.
Final output is ufc_fight_stats.csv (one row per fighter per fight).

Usage:
    python scrape_ufc_stats.py                  # Full scrape (resumes from cache)
    python scrape_ufc_stats.py --export-only     # Just export cache to CSV
    python scrape_ufc_stats.py --since 2020-01-01  # Only events after a date
    python scrape_ufc_stats.py --event-limit 10  # Scrape only N events (for testing)
    python scrape_ufc_stats.py --delay 0.2       # Add delay between requests if needed
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

BASE_URL = "http://ufcstats.com"
CACHE_FILE = "ufc_scrape_cache.json"
OUTPUT_CSV = "ufc_fight_stats.csv"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

# Rate limiting (set via --delay flag; default 0 = no delay)
REQUEST_DELAY = 0.0


# ── Cache ────────────────────────────────────────────────────────────

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {"events": {}, "fights": {}, "meta": {"last_updated": None}}


def save_cache(cache: dict):
    cache["meta"]["last_updated"] = datetime.now().isoformat()
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


# ── HTTP helpers ─────────────────────────────────────────────────────

def _is_challenge(html: str) -> bool:
    """Detect ufcstats.com's JS proof-of-work anti-bot interstitial."""
    return "Checking your browser" in html or ("/__c" in html and "nonce=" in html)


def _solve_challenge(html: str) -> bool:
    """Solve the proof-of-work interstitial and register clearance on the session.

    The page asks for the smallest n such that sha256(nonce + ':' + n) starts with
    a run of '0' hex chars, then POSTs {nonce, n} to /__c to obtain a cookie.
    Returns True if a challenge was detected and a solution submitted.
    """
    nonce_m = re.search(r'nonce="([^"]+)"', html)
    target_m = re.search(r"target=new Array\((\d+)\+1\)", html)
    if not nonce_m or not target_m:
        return False

    nonce = nonce_m.group(1)
    prefix = "0" * int(target_m.group(1))
    n = 0
    while not hashlib.sha256(f"{nonce}:{n}".encode()).hexdigest().startswith(prefix):
        n += 1
    SESSION.post(f"{BASE_URL}/__c", data={"nonce": nonce, "n": n}, timeout=15)
    return True


def fetch(url: str, retries: int = 3) -> str | None:
    """Fetch a URL with retries, transparently clearing the anti-bot challenge."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=15)
            if REQUEST_DELAY > 0:
                time.sleep(REQUEST_DELAY)
            if r.status_code == 200:
                html = r.text
                # If we hit the proof-of-work wall, solve it and re-fetch once.
                if _is_challenge(html) and _solve_challenge(html):
                    r = SESSION.get(url, timeout=15)
                    if r.status_code == 200:
                        return r.text
                    print(f"    HTTP {r.status_code} for {url} (after challenge)")
                    continue
                return html
            print(f"    HTTP {r.status_code} for {url}")
        except requests.RequestException as e:
            print(f"    Request error ({attempt+1}/{retries}): {e}")
            time.sleep(2 ** (attempt + 1))
    return None


# ── Parsing helpers ──────────────────────────────────────────────────

def parse_time_mmss(s: str) -> int:
    """Parse 'M:SS' to total seconds."""
    s = s.strip()
    if not s or s == "--" or s == "---":
        return 0
    parts = s.split(":")
    if len(parts) == 2:
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except ValueError:
            return 0
    return 0


def parse_landed_of(s: str) -> tuple[int, int]:
    """Parse '36 of 55' → (36, 55)."""
    m = re.search(r"(\d+)\s+of\s+(\d+)", s.strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    return 0, 0


def parse_int(s: str) -> int:
    s = s.strip()
    try:
        return int(s)
    except ValueError:
        return 0


def parse_pct(s: str) -> float:
    """Parse '65%' → 0.65."""
    m = re.search(r"([\d.]+)%", s.strip())
    if m:
        return float(m.group(1)) / 100.0
    return 0.0


def parse_event_date(s: str) -> str | None:
    """Parse 'March 14, 2026' → '2026-03-14'."""
    s = s.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%b. %d, %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ── Scrape: Event list ───────────────────────────────────────────────

def scrape_event_list() -> list[dict]:
    """Get all completed events from ufcstats.com."""
    print("Fetching event list...")
    html = fetch(f"{BASE_URL}/statistics/events/completed?page=all")
    if not html:
        print("ERROR: Could not fetch event list.")
        return []

    soup = BeautifulSoup(html, "html.parser")
    events = []

    for row in soup.select("tr.b-statistics__table-row"):
        link = row.select_one("a.b-link")
        if not link:
            continue
        href = link.get("href", "")
        if "event-details" not in href:
            continue

        name = link.text.strip()
        date_span = row.select_one("span.b-statistics__date")
        date_str = parse_event_date(date_span.text) if date_span else None

        location_col = row.select("td.b-statistics__table-col")
        location = ""
        if len(location_col) >= 2:
            location = location_col[1].text.strip()

        # Normalize URL to use ufcstats.com
        event_id = href.split("/")[-1]
        events.append({
            "event_id": event_id,
            "url": f"{BASE_URL}/event-details/{event_id}",
            "name": name,
            "date": date_str,
            "location": location,
        })

    print(f"  Found {len(events)} events.")
    return events


# ── Scrape: Event detail → fight list ────────────────────────────────

def scrape_event_fights(event_url: str) -> list[dict]:
    """Scrape an event page for the list of fights with summary stats."""
    html = fetch(event_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    fights = []

    for row in soup.select("tr.b-fight-details__table-row"):
        # Get fight URL
        fight_url = row.get("data-link", "")
        if "fight-details" not in fight_url:
            continue

        fight_id = fight_url.split("/")[-1]
        cols = row.select("td.b-fight-details__table-col")
        if len(cols) < 10:
            continue

        # W/L status
        wl_texts = [p.text.strip().lower() for p in cols[0].select("p")]

        # Fighter names
        names = [a.text.strip() for a in cols[1].select("a")]
        if len(names) < 2:
            names_text = [p.text.strip() for p in cols[1].select("p") if p.text.strip()]
            names = names_text[:2] if len(names_text) >= 2 else names

        # Weight class
        weight_class = cols[6].select_one("p")
        weight_class = weight_class.text.strip().split("\n")[0].strip() if weight_class else ""

        # Method
        method_ps = cols[7].select("p")
        method = method_ps[0].text.strip() if method_ps else ""
        method_detail = method_ps[1].text.strip() if len(method_ps) > 1 else ""

        # Round and Time
        round_num = parse_int(cols[8].text)
        fight_time = cols[9].text.strip()

        fights.append({
            "fight_id": fight_id,
            "url": f"{BASE_URL}/fight-details/{fight_id}",
            "fighter1": names[0] if len(names) > 0 else "",
            "fighter2": names[1] if len(names) > 1 else "",
            "weight_class": weight_class,
            "method": method,
            "method_detail": method_detail,
            "round": round_num,
            "time": fight_time,
        })

    return fights


# ── Scrape: Fight detail → all stats ─────────────────────────────────

def _parse_two_fighter_col(col) -> list[str]:
    """Extract two values from a column with two <p> tags."""
    ps = col.select("p.b-fight-details__table-text")
    return [p.text.strip() for p in ps][:2]


def _parse_totals_table(table) -> dict | None:
    """Parse the Totals table (Fighter, KD, Sig.str, Sig.str%, Total str, Td, Td%, Sub.att, Rev, Ctrl)."""
    rows = table.select("tbody tr.b-fight-details__table-row")
    if not rows:
        return None

    row = rows[0]
    cols = row.select("td")
    if len(cols) < 10:
        return None

    # Fighter names
    names_raw = _parse_two_fighter_col(cols[0])
    names = []
    for cell_html in cols[0].select("p"):
        a = cell_html.select_one("a")
        names.append(a.text.strip() if a else cell_html.text.strip())

    kd = _parse_two_fighter_col(cols[1])
    sig_str = _parse_two_fighter_col(cols[2])
    sig_pct = _parse_two_fighter_col(cols[3])
    total_str = _parse_two_fighter_col(cols[4])
    td = _parse_two_fighter_col(cols[5])
    td_pct = _parse_two_fighter_col(cols[6])
    sub_att = _parse_two_fighter_col(cols[7])
    rev = _parse_two_fighter_col(cols[8])
    ctrl = _parse_two_fighter_col(cols[9])

    fighters = []
    for i in range(min(2, len(names))):
        ss_l, ss_a = parse_landed_of(sig_str[i]) if i < len(sig_str) else (0, 0)
        ts_l, ts_a = parse_landed_of(total_str[i]) if i < len(total_str) else (0, 0)
        td_l, td_a = parse_landed_of(td[i]) if i < len(td) else (0, 0)

        fighters.append({
            "name": names[i] if i < len(names) else "",
            "kd": parse_int(kd[i]) if i < len(kd) else 0,
            "sig_str_landed": ss_l,
            "sig_str_attempted": ss_a,
            "sig_str_pct": parse_pct(sig_pct[i]) if i < len(sig_pct) else 0,
            "total_str_landed": ts_l,
            "total_str_attempted": ts_a,
            "td_landed": td_l,
            "td_attempted": td_a,
            "td_pct": parse_pct(td_pct[i]) if i < len(td_pct) else 0,
            "sub_att": parse_int(sub_att[i]) if i < len(sub_att) else 0,
            "rev": parse_int(rev[i]) if i < len(rev) else 0,
            "ctrl_sec": parse_time_mmss(ctrl[i]) if i < len(ctrl) else 0,
        })

    return {"fighters": fighters}


def _parse_sig_strikes_table(table) -> dict | None:
    """Parse the Sig Strikes breakdown table (Sig.str, %, Head, Body, Leg, Distance, Clinch, Ground)."""
    rows = table.select("tbody tr.b-fight-details__table-row")
    if not rows:
        return None

    row = rows[0]
    cols = row.select("td")
    if len(cols) < 9:
        return None

    # cols: 0=Fighter, 1=Sig.str, 2=%, 3=Head, 4=Body, 5=Leg, 6=Distance, 7=Clinch, 8=Ground
    head = _parse_two_fighter_col(cols[3])
    body = _parse_two_fighter_col(cols[4])
    leg = _parse_two_fighter_col(cols[5])
    distance = _parse_two_fighter_col(cols[6])
    clinch = _parse_two_fighter_col(cols[7])
    ground = _parse_two_fighter_col(cols[8])

    fighters = []
    for i in range(2):
        h_l, h_a = parse_landed_of(head[i]) if i < len(head) else (0, 0)
        b_l, b_a = parse_landed_of(body[i]) if i < len(body) else (0, 0)
        lg_l, lg_a = parse_landed_of(leg[i]) if i < len(leg) else (0, 0)
        d_l, d_a = parse_landed_of(distance[i]) if i < len(distance) else (0, 0)
        c_l, c_a = parse_landed_of(clinch[i]) if i < len(clinch) else (0, 0)
        g_l, g_a = parse_landed_of(ground[i]) if i < len(ground) else (0, 0)

        fighters.append({
            "ss_head_landed": h_l, "ss_head_attempted": h_a,
            "ss_body_landed": b_l, "ss_body_attempted": b_a,
            "ss_leg_landed": lg_l, "ss_leg_attempted": lg_a,
            "ss_distance_landed": d_l, "ss_distance_attempted": d_a,
            "ss_clinch_landed": c_l, "ss_clinch_attempted": c_a,
            "ss_ground_landed": g_l, "ss_ground_attempted": g_a,
        })

    return {"fighters": fighters}


def _parse_per_round_table(table) -> list[dict]:
    """Parse a per-round table. Returns list of round dicts."""
    rounds = []
    current_round = None

    for element in table.select("thead, tr"):
        # Round header
        round_header = element.select_one("th[colspan]")
        if round_header and "Round" in round_header.text:
            m = re.search(r"Round\s+(\d+)", round_header.text)
            if m:
                current_round = int(m.group(1))
            continue

        # Data row
        if current_round is None:
            continue

        cols = element.select("td")
        if len(cols) < 10:
            continue

        kd = _parse_two_fighter_col(cols[1])
        sig_str = _parse_two_fighter_col(cols[2])
        sig_pct = _parse_two_fighter_col(cols[3])
        total_str = _parse_two_fighter_col(cols[4])
        td = _parse_two_fighter_col(cols[5])
        td_pct = _parse_two_fighter_col(cols[6])
        sub_att = _parse_two_fighter_col(cols[7])
        rev = _parse_two_fighter_col(cols[8])
        ctrl = _parse_two_fighter_col(cols[9])

        fighters = []
        for i in range(2):
            ss_l, ss_a = parse_landed_of(sig_str[i]) if i < len(sig_str) else (0, 0)
            ts_l, ts_a = parse_landed_of(total_str[i]) if i < len(total_str) else (0, 0)
            td_l, td_a = parse_landed_of(td[i]) if i < len(td) else (0, 0)

            fighters.append({
                "kd": parse_int(kd[i]) if i < len(kd) else 0,
                "sig_str_landed": ss_l, "sig_str_attempted": ss_a,
                "sig_str_pct": parse_pct(sig_pct[i]) if i < len(sig_pct) else 0,
                "total_str_landed": ts_l, "total_str_attempted": ts_a,
                "td_landed": td_l, "td_attempted": td_a,
                "td_pct": parse_pct(td_pct[i]) if i < len(td_pct) else 0,
                "sub_att": parse_int(sub_att[i]) if i < len(sub_att) else 0,
                "rev": parse_int(rev[i]) if i < len(rev) else 0,
                "ctrl_sec": parse_time_mmss(ctrl[i]) if i < len(ctrl) else 0,
            })

        rounds.append({"round": current_round, "fighters": fighters})
        current_round = None  # Reset so we don't double-count

    return rounds


def _parse_sig_per_round_table(table) -> list[dict]:
    """Parse the sig strikes per-round breakdown table."""
    rounds = []
    current_round = None

    for element in table.select("thead, tr"):
        round_header = element.select_one("th[colspan]")
        if round_header and "Round" in round_header.text:
            m = re.search(r"Round\s+(\d+)", round_header.text)
            if m:
                current_round = int(m.group(1))
            continue

        if current_round is None:
            continue

        cols = element.select("td")
        if len(cols) < 9:
            continue

        head = _parse_two_fighter_col(cols[3])
        body = _parse_two_fighter_col(cols[4])
        leg = _parse_two_fighter_col(cols[5])
        distance = _parse_two_fighter_col(cols[6])
        clinch = _parse_two_fighter_col(cols[7])
        ground = _parse_two_fighter_col(cols[8])

        fighters = []
        for i in range(2):
            h_l, h_a = parse_landed_of(head[i]) if i < len(head) else (0, 0)
            b_l, b_a = parse_landed_of(body[i]) if i < len(body) else (0, 0)
            lg_l, lg_a = parse_landed_of(leg[i]) if i < len(leg) else (0, 0)
            d_l, d_a = parse_landed_of(distance[i]) if i < len(distance) else (0, 0)
            c_l, c_a = parse_landed_of(clinch[i]) if i < len(clinch) else (0, 0)
            g_l, g_a = parse_landed_of(ground[i]) if i < len(ground) else (0, 0)

            fighters.append({
                "ss_head_landed": h_l, "ss_head_attempted": h_a,
                "ss_body_landed": b_l, "ss_body_attempted": b_a,
                "ss_leg_landed": lg_l, "ss_leg_attempted": lg_a,
                "ss_distance_landed": d_l, "ss_distance_attempted": d_a,
                "ss_clinch_landed": c_l, "ss_clinch_attempted": c_a,
                "ss_ground_landed": g_l, "ss_ground_attempted": g_a,
            })

        rounds.append({"round": current_round, "fighters": fighters})
        current_round = None

    return rounds


def scrape_fight_detail(fight_url: str) -> dict | None:
    """Scrape a fight detail page for all stats."""
    html = fetch(fight_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    result = {}

    # ── Fight metadata ──
    # Winner/Loser
    persons = soup.select("div.b-fight-details__person")
    fighter_status = []
    for person in persons:
        status_el = person.select_one("i.b-fight-details__person-status")
        name_el = person.select_one("a.b-fight-details__person-link")
        status = status_el.text.strip() if status_el else ""
        name = name_el.text.strip() if name_el else ""
        fighter_status.append({"name": name, "status": status})

    result["fighter_status"] = fighter_status

    # Bout type (e.g. "Featherweight Bout")
    bout_el = soup.select_one("i.b-fight-details__fight-title")
    if bout_el:
        # Remove img tags and get text
        result["bout_type"] = bout_el.get_text(strip=True)

    # Method, Round, Time, Time format, Referee
    for item in soup.select("i.b-fight-details__text-item_first, i.b-fight-details__text-item"):
        label_el = item.select_one("i.b-fight-details__label")
        if not label_el:
            continue
        label = label_el.text.strip().rstrip(":")
        # Value is the text after the label
        value = item.get_text(strip=True).replace(label_el.get_text(strip=True), "").strip()

        if label == "Method":
            result["method"] = value
        elif label == "Round":
            result["round"] = parse_int(value)
        elif label == "Time":
            result["time"] = value
            result["time_seconds"] = parse_time_mmss(value)
        elif label == "Time format":
            result["time_format"] = value
        elif label == "Referee":
            result["referee"] = value

    # Compute total fight time in seconds
    rnd = result.get("round", 0)
    last_rnd_sec = result.get("time_seconds", 0)
    result["fight_time_seconds"] = (rnd - 1) * 300 + last_rnd_sec if rnd > 0 else 0

    # ── Tables ──
    # The page has sections: Totals, Per-round, Sig Strikes totals, Sig Strikes per-round
    sections = soup.select("section.b-fight-details__section")
    all_tables = soup.select("table")

    # Identify tables by their preceding section header
    totals_data = None
    per_round_data = []
    sig_totals_data = None
    sig_per_round_data = []

    # Strategy: tables alternate between totals and per-round
    # Table layout (by order):
    #   0: Totals table (after "Totals" section)
    #   1: Per-round table (has round headers)
    #   2: Sig strikes totals table (after "Significant Strikes" section)
    #   3: Sig strikes per-round table

    for i, tbl in enumerate(all_tables):
        # Check preceding section text
        prev = tbl.find_previous("section")
        section_text = prev.get_text(strip=True) if prev else ""

        rows = tbl.select("tbody tr.b-fight-details__table-row")
        has_round_headers = bool(tbl.select("th[colspan]"))
        headers = [th.text.strip() for th in tbl.select("thead th")]

        if "Ctrl" in " ".join(headers):
            # This is either Totals or Per-round for main stats
            if has_round_headers:
                per_round_data = _parse_per_round_table(tbl)
            elif totals_data is None:
                totals_data = _parse_totals_table(tbl)
        elif "Head" in " ".join(headers) or "Distance" in " ".join(headers):
            # Sig strikes breakdown table
            if has_round_headers:
                sig_per_round_data = _parse_sig_per_round_table(tbl)
            elif sig_totals_data is None:
                sig_totals_data = _parse_sig_strikes_table(tbl)

    if totals_data:
        result["totals"] = totals_data
    if per_round_data:
        result["per_round"] = per_round_data
    if sig_totals_data:
        result["sig_strikes"] = sig_totals_data
    if sig_per_round_data:
        result["sig_strikes_per_round"] = sig_per_round_data

    return result


# ── Export to CSV ────────────────────────────────────────────────────

def export_csv(cache: dict, output_path: str = OUTPUT_CSV):
    """Export cached data to CSV with one row per fighter per fight."""
    events = cache.get("events", {})
    fights = cache.get("fights", {})

    # Build event lookup
    event_lookup = {}
    for eid, edata in events.items():
        event_lookup[eid] = edata
        for f in edata.get("fights", []):
            event_lookup[f["fight_id"]] = {
                "event_id": eid,
                "event_name": edata.get("name", ""),
                "event_date": edata.get("date", ""),
                "event_location": edata.get("location", ""),
            }

    # CSV columns
    fieldnames = [
        # Event
        "event_date", "event_name", "event_location",
        # Fight
        "fight_id", "weight_class", "method", "method_detail", "round", "time",
        "fight_time_seconds", "time_format", "referee",
        # Fighter
        "fighter", "opponent", "result",  # W / L / D / NC
        # Totals
        "kd", "sig_str_landed", "sig_str_attempted", "sig_str_pct",
        "total_str_landed", "total_str_attempted",
        "td_landed", "td_attempted", "td_pct",
        "sub_att", "rev", "ctrl_sec",
        # Sig strike breakdown
        "ss_head_landed", "ss_head_attempted",
        "ss_body_landed", "ss_body_attempted",
        "ss_leg_landed", "ss_leg_attempted",
        "ss_distance_landed", "ss_distance_attempted",
        "ss_clinch_landed", "ss_clinch_attempted",
        "ss_ground_landed", "ss_ground_attempted",
        # Per-round (flattened: r1_kd, r1_sig_str_landed, ... r5_ctrl_sec)
    ]

    # Add per-round columns (up to 5 rounds)
    round_stats = [
        "kd", "sig_str_landed", "sig_str_attempted",
        "total_str_landed", "total_str_attempted",
        "td_landed", "td_attempted", "sub_att", "rev", "ctrl_sec",
        "ss_head_landed", "ss_head_attempted",
        "ss_body_landed", "ss_body_attempted",
        "ss_leg_landed", "ss_leg_attempted",
        "ss_distance_landed", "ss_distance_attempted",
        "ss_clinch_landed", "ss_clinch_attempted",
        "ss_ground_landed", "ss_ground_attempted",
    ]
    for r in range(1, 6):
        for stat in round_stats:
            fieldnames.append(f"r{r}_{stat}")

    rows = []
    for fight_id, fdata in fights.items():
        if not fdata:
            continue

        # Find event info
        einfo = event_lookup.get(fight_id, {})

        totals = fdata.get("totals", {})
        sig = fdata.get("sig_strikes", {})
        per_round = fdata.get("per_round", [])
        sig_pr = fdata.get("sig_strikes_per_round", [])
        statuses = fdata.get("fighter_status", [])

        fighters_totals = totals.get("fighters", [])
        fighters_sig = sig.get("fighters", [])

        for i in range(min(2, len(fighters_totals))):
            ft = fighters_totals[i]
            fs = fighters_sig[i] if i < len(fighters_sig) else {}

            # Determine result
            result_str = ""
            if i < len(statuses):
                s = statuses[i].get("status", "").upper()
                if s == "W":
                    result_str = "W"
                elif s == "L":
                    result_str = "L"
                elif s == "D":
                    result_str = "D"
                elif s == "NC":
                    result_str = "NC"

            opp_idx = 1 - i
            opp_name = ""
            if opp_idx < len(fighters_totals):
                opp_name = fighters_totals[opp_idx].get("name", "")

            row = {
                "event_date": einfo.get("event_date", ""),
                "event_name": einfo.get("event_name", ""),
                "event_location": einfo.get("event_location", ""),
                "fight_id": fight_id,
                "weight_class": fdata.get("bout_type", ""),
                "method": fdata.get("method", ""),
                "method_detail": "",
                "round": fdata.get("round", 0),
                "time": fdata.get("time", ""),
                "fight_time_seconds": fdata.get("fight_time_seconds", 0),
                "time_format": fdata.get("time_format", ""),
                "referee": fdata.get("referee", ""),
                "fighter": ft.get("name", ""),
                "opponent": opp_name,
                "result": result_str,
                **{k: ft.get(k, 0) for k in [
                    "kd", "sig_str_landed", "sig_str_attempted", "sig_str_pct",
                    "total_str_landed", "total_str_attempted",
                    "td_landed", "td_attempted", "td_pct",
                    "sub_att", "rev", "ctrl_sec",
                ]},
                **{k: fs.get(k, 0) for k in [
                    "ss_head_landed", "ss_head_attempted",
                    "ss_body_landed", "ss_body_attempted",
                    "ss_leg_landed", "ss_leg_attempted",
                    "ss_distance_landed", "ss_distance_attempted",
                    "ss_clinch_landed", "ss_clinch_attempted",
                    "ss_ground_landed", "ss_ground_attempted",
                ]},
            }

            # Per-round data
            # Build round lookup from per_round and sig_pr
            round_lookup = {}
            for rd in per_round:
                rn = rd["round"]
                if i < len(rd["fighters"]):
                    round_lookup.setdefault(rn, {}).update(rd["fighters"][i])

            for rd in sig_pr:
                rn = rd["round"]
                if i < len(rd["fighters"]):
                    round_lookup.setdefault(rn, {}).update(rd["fighters"][i])

            for r in range(1, 6):
                rd_data = round_lookup.get(r, {})
                for stat in round_stats:
                    row[f"r{r}_{stat}"] = rd_data.get(stat, "")

            rows.append(row)

    # Sort by date (newest first), then event, then fight
    rows.sort(key=lambda r: (r.get("event_date", "") or "", r.get("fight_id", "")), reverse=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  Exported {len(rows)} fighter-fight rows to {output_path}")
    return len(rows)


# ── Main scrape loop ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape UFC stats from ufcstats.com")
    parser.add_argument("--export-only", action="store_true",
                        help="Just export existing cache to CSV")
    parser.add_argument("--since", type=str, default=None,
                        help="Only scrape events after YYYY-MM-DD")
    parser.add_argument("--event-limit", type=int, default=0,
                        help="Max events to scrape (0=all)")
    parser.add_argument("--output", type=str, default=OUTPUT_CSV,
                        help="Output CSV path")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="Seconds between requests (default: 0)")
    args = parser.parse_args()

    global REQUEST_DELAY
    REQUEST_DELAY = args.delay

    cache = load_cache()

    if args.export_only:
        export_csv(cache, args.output)
        return

    # Step 1: Get event list
    events = scrape_event_list()
    if not events:
        print("No events found.")
        return

    # Filter by date
    if args.since:
        events = [e for e in events if e.get("date") and e["date"] >= args.since]
        print(f"  Filtered to {len(events)} events since {args.since}")

    # Apply limit
    if args.event_limit > 0:
        events = events[:args.event_limit]
        print(f"  Limited to {args.event_limit} events")

    # Step 2: For each event, get fights
    total_fights = 0
    total_new_fights = 0

    for ei, event in enumerate(events):
        eid = event["event_id"]

        # Check if event already fully scraped
        cached_event = cache["events"].get(eid, {})
        cached_fights = cached_event.get("fights", [])
        all_cached = all(
            cache["fights"].get(f["fight_id"]) is not None
            for f in cached_fights
        ) if cached_fights else False

        if all_cached and cached_fights:
            total_fights += len(cached_fights)
            print(f"  [{ei+1}/{len(events)}] {event['date']} {event['name']}"
                  f" — {len(cached_fights)} fights (cached)")
            continue

        print(f"  [{ei+1}/{len(events)}] {event['date']} {event['name']}...", end="", flush=True)

        fight_list = scrape_event_fights(event["url"])
        if not fight_list:
            print(" no fights found")
            continue

        # Store event data
        cache["events"][eid] = {
            "name": event["name"],
            "date": event["date"],
            "location": event["location"],
            "fights": fight_list,
        }

        # Step 3: For each fight, get detailed stats
        new_count = 0
        for fi, fight in enumerate(fight_list):
            fid = fight["fight_id"]
            if fid in cache["fights"] and cache["fights"][fid] is not None:
                continue

            detail = scrape_fight_detail(fight["url"])
            cache["fights"][fid] = detail
            new_count += 1

        total_fights += len(fight_list)
        total_new_fights += new_count

        status = f" {len(fight_list)} fights"
        if new_count > 0:
            status += f" ({new_count} new)"
        else:
            status += " (all cached)"
        print(status)

        # Save cache periodically (every event)
        save_cache(cache)

    # Final save
    save_cache(cache)

    print(f"\n{'='*60}")
    print(f"  Scrape complete!")
    print(f"  Events: {len(events)}")
    print(f"  Total fights: {total_fights}")
    print(f"  New fights scraped: {total_new_fights}")
    print(f"  Cache: {CACHE_FILE}")
    print(f"{'='*60}")

    # Export to CSV
    export_csv(cache, args.output)


if __name__ == "__main__":
    main()
