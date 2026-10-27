"""Write fighter_images.json so the website can show each fighter's UFC.com photo.

Reads this week's event page on UFC.com, takes the photo UFC shows for each
fighter on the fight card, and saves the photo links by fighter name, using
the DraftKings spelling from DKSalaries.csv where the names match. Only the
links are saved; the photos themselves stay on UFC's image server.

The event is found on UFC.com's events page (the upcoming event whose
fighters best match DKSalaries.csv), or you can name it:

Usage:  python export_fighter_images.py [event URL]
        python export_fighter_images.py https://www.ufc.com/event/ufc-fight-night-october-31-2026

publish.bat runs this before it stages the files. It never stops a publish:
if UFC.com can't be reached or nothing matches, it prints why, leaves the
existing fighter_images.json as it is, and exits normally.
"""
import csv
import html as htmllib
import json
import re
import sys
import unicodedata
import urllib.request
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "fighter_images.json"
BASE = "https://www.ufc.com"
TIMEOUT = 15
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/130.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}
# UFC shows a stand-in outline when it has no photo of a fighter yet
PLACEHOLDER = re.compile(r"silhouette|no-profile|default|placeholder", re.I)


def say(msg):
    print(f"export_fighter_images: {msg}")


def norm(name):
    s = unicodedata.normalize("NFD", str(name or "")).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"\s+(jr|sr|ii|iii|iv)\.?$", "", s.strip())
    return re.sub(r"[^a-z]", "", s)


def slate():
    """DraftKings names by normalized name."""
    path = HERE / "DKSalaries.csv"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        return {norm(r["Name"]): r["Name"].strip() for r in csv.DictReader(f) if r.get("Name")}


def fetch(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.read().decode("utf-8", "replace")
    except Exception as e:
        say(f"could not load {url} ({e})")
        return None


def text(fragment):
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment))).strip()


def parse_event(page):
    """[(ufc_name, photo_url or None, athlete_url or None)] from an event page."""
    out = []
    # each bout is a c-listing-fight block; split the page on them
    blocks = re.split(r'<div[^>]+class="c-listing-fight"', page)[1:]
    for block in blocks:
        for corner in ("red", "blue"):
            name, photo, link = None, None, None
            m = re.search(r'c-listing-fight__corner-name--%s"[^>]*>(.*?)</div>' % corner, block, re.S)
            if m:
                name = text(m.group(1))
                lm = re.search(r'href="([^"]*/athlete/[^"]+)"', m.group(1))
                if lm:
                    link = lm.group(1) if lm.group(1).startswith("http") else BASE + lm.group(1)
            m = re.search(r'c-listing-fight__corner-image--%s"[^>]*>(.*?)</div>' % corner, block, re.S)
            if m:
                im = re.search(r'<img[^>]+?(?:data-src|src)="([^"]+)"', m.group(1))
                if im:
                    photo = htmllib.unescape(im.group(1))
                    if photo.startswith("/"):
                        photo = BASE + photo
                    if PLACEHOLDER.search(photo):
                        photo = None
                if not name:
                    am = re.search(r'alt="([^"]+)"', m.group(1))
                    if am:
                        name = text(am.group(1))
            if name:
                out.append((name, photo, link))
    return out


def event_title(page):
    m = re.search(r"<title>(.*?)</title>", page, re.S | re.I)
    return text(m.group(1)).split("|")[0].strip() if m else ""


def upcoming_events():
    page = fetch(BASE + "/events")
    if not page:
        return []
    links = []
    for href in re.findall(r'href="(/event/[^"#?]+)', page):
        url = BASE + href
        if url not in links:
            links.append(url)
    return links[:5]


def main():
    names = slate()
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    candidates = [args[0].split("#")[0]] if args else upcoming_events()
    if not candidates:
        say("no UFC.com event to read; fighter_images.json left as it is.")
        return 0

    best = None
    for url in candidates:
        page = fetch(url)
        if not page:
            continue
        corners = parse_event(page)
        hits = sum(1 for n, _, _ in corners if norm(n) in names)
        if corners and (best is None or hits > best[0]):
            best = (hits, url, event_title(page), corners)
        if names and hits >= len(names) * 0.8:
            break   # this is the slate's event; no need to read the rest

    if not best:
        say("found no fight card on UFC.com; fighter_images.json left as it is.")
        return 0
    hits, url, title, corners = best
    if names and not args and hits < max(2, len(names) // 2):
        say(f"no UFC.com event matches DKSalaries.csv (best: {title}, {hits} names); "
            "fighter_images.json left as it is.")
        return 0

    fighters, missing = {}, []
    for ufc_name, photo, link in corners:
        key = names.get(norm(ufc_name), ufc_name)   # DraftKings spelling when it matches
        if photo:
            fighters[key] = {"photo": photo, "ufc_name": ufc_name, "profile": link}
        else:
            missing.append(key)
    out = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "event": title,
        "event_url": url,
        "fighters": fighters,
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    unmatched = [n for n, _, _ in corners if names and norm(n) not in names]
    say(f"wrote fighter_images.json for {title}: {len(fighters)} photos"
        + (f", no photo yet for {', '.join(missing)}" if missing else "")
        + (f"; not on the DraftKings slate: {', '.join(unmatched)}" if unmatched else ""))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:   # never stop a publish over photos
        say(f"skipped ({e})")
        sys.exit(0)
