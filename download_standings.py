#!/usr/bin/env python3
"""
download_standings.py — download DraftKings contest standings CSVs for a list of
contests you track, using your own logged-in DraftKings session, and save each as
contest-standings-<id>.csv (or .zip) right where the sim's discovery looks.

List the contests in contests_to_track.txt (one per line; a full contest URL or a
bare contest id both work; blank lines and #comments are ignored). For example:

    https://www.draftkings.com/draft/contest/191819063
    192029298

Authentication (DraftKings only serves standings to a logged-in account):
  1. Preferred — your browser. If `browser_cookie3` is installed and you're
     logged into DraftKings in Chrome/Edge/Firefox, this reads your session
     automatically:  pip install browser_cookie3
  2. Fallback — paste your DK Cookie header into a file named `dk_cookie.txt`
     next to this script (from your browser's DevTools > Network > any
     draftkings.com request > Request Headers > Cookie).

Standings only exist AFTER a contest locks, so run this after the slate starts.

    python download_standings.py                       # uses contests_to_track.txt
    python download_standings.py --ids 191819063,192029298
    python download_standings.py --file my_list.txt --out "C:\\path\\to\\workbook_folder"

Nothing is uploaded anywhere; this only downloads your accessible standings to disk.
"""

import argparse
import os
import re
import sys
import time
from pathlib import Path

import requests

EXPORT_URL = "https://www.draftkings.com/contest/exportfullstandingscsv/{cid}"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/csv, application/octet-stream, application/zip, */*",
}
ID_RE = re.compile(r"(\d{6,})")
CD_FILE_RE = re.compile(r'filename\*?="?([^";]+)"?', re.I)


def ids_from_lines(lines):
    """Pull a contest id out of each line (URL or bare id); keep order, de-dupe."""
    out, seen = [], set()
    for ln in lines:
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        m = ID_RE.search(ln)
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            out.append(m.group(1))
    return out


def load_auth():
    """Return ('browser', cookiejar) or ('file', cookie_header) or (None, None)."""
    try:
        import browser_cookie3 as bc
        cj = bc.load(domain_name="draftkings.com")
        if cj and len(cj):
            return "browser", cj
    except Exception:
        pass
    p = Path("dk_cookie.txt")
    if p.exists():
        raw = p.read_text(encoding="utf-8").strip()
        if raw:
            return "file", raw
    return None, None


def out_name(resp, cid):
    """Use DraftKings' own filename if given; else sniff csv vs zip."""
    cd = resp.headers.get("content-disposition", "")
    m = CD_FILE_RE.search(cd)
    if m and m.group(1).strip():
        return os.path.basename(m.group(1).strip())
    ext = "zip" if resp.content[:2] == b"PK" else "csv"
    return f"contest-standings-{cid}.{ext}"


def looks_like_login(resp):
    """True if we got bounced to a login/HTML page instead of the CSV."""
    if "sitelogin" in resp.url.lower() or "/login" in resp.url.lower():
        return True
    ctype = resp.headers.get("content-type", "").lower()
    return "text/html" in ctype


def download_one(cid, mode, auth, outdir):
    url = EXPORT_URL.format(cid=cid)
    kw = {"headers": dict(HEADERS), "timeout": 90, "allow_redirects": True}
    if mode == "browser":
        kw["cookies"] = auth
    else:
        kw["headers"]["Cookie"] = auth
    r = requests.get(url, **kw)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    if looks_like_login(r):
        return None, "not logged in (DraftKings returned a login page)"
    if not r.content:
        return None, "empty response (contest not locked yet, or no standings)"
    name = out_name(r, cid)
    path = os.path.join(outdir, name)
    with open(path, "wb") as f:
        f.write(r.content)
    return name, None


def main():
    ap = argparse.ArgumentParser(description="Download DraftKings standings CSVs for tracked contests.")
    ap.add_argument("--file", default="contests_to_track.txt", help="list of contest URLs/ids (default contests_to_track.txt)")
    ap.add_argument("--ids", help="comma-separated ids/urls instead of the file")
    ap.add_argument("--out", default=".", help="folder to save the standings into (default: current folder)")
    ap.add_argument("--pause", type=float, default=1.0, help="seconds between contests (be polite)")
    args = ap.parse_args()

    if args.ids:
        ids = ids_from_lines(args.ids.split(","))
    else:
        p = Path(args.file)
        if not p.exists():
            print(f"No '{args.file}' found. Put one contest URL or id per line, or pass --ids.")
            sys.exit(1)
        ids = ids_from_lines(p.read_text(encoding="utf-8").splitlines())
    if not ids:
        print("No contest ids found in the list.")
        sys.exit(1)

    mode, auth = load_auth()
    if not mode:
        print("Could not find your DraftKings login.\n"
              "  - Easiest: pip install browser_cookie3, then just be logged into DraftKings in your browser.\n"
              "  - Or paste your DK 'Cookie' header into a file named dk_cookie.txt next to this script.")
        sys.exit(1)

    os.makedirs(args.out, exist_ok=True)
    print(f"Auth: {mode}. Downloading {len(ids)} contest(s) -> {os.path.abspath(args.out)}\n")
    ok = fail = 0
    for cid in ids:
        try:
            name, err = download_one(cid, mode, auth, args.out)
        except Exception as e:  # noqa: BLE001
            name, err = None, str(e)
        if name:
            print(f"  ok  {cid} -> {name}")
            ok += 1
        else:
            print(f"  !   {cid}: {err}")
            fail += 1
            if err and "not logged in" in err:
                print("\nStopping — log into DraftKings (in your browser, or refresh dk_cookie.txt) and re-run.")
                break
        time.sleep(args.pause)
    print(f"\nDone. {ok} saved, {fail} failed.")


if __name__ == "__main__":
    main()
