"""Save UFC.com photo links so the website can show each fighter's photo.

Event mode (what publish.bat runs) reads this week's event page on UFC.com,
takes the full-body shot UFC shows for each fighter on the fight card, and
adds the links to fighter_images.json under the DraftKings spelling from
DKSalaries.csv where the names match. Fighters already in the file from
earlier weeks are kept, so the file builds up over time. The event is found
on UFC.com's events page (the upcoming event whose fighters best match
DKSalaries.csv), or you can name one or more events yourself.

Roster mode reads every page of UFC.com's athlete list and saves each
athlete's headshot link to fighter_headshots.json. It is a one-off (run it
again now and then for new signings), it waits a moment between pages, and
it takes a few minutes. It is not part of publish.bat.

Only links are saved; the photos themselves stay on UFC's image server.

Usage:  python export_fighter_images.py                      this week's event
        python export_fighter_images.py URL [URL ...]          named events
        python export_fighter_images.py https://www.ufc.com/event/ufc-333
        python export_fighter_images.py --roster               every athlete's headshot

It never stops a publish: if UFC.com can't be reached or nothing matches,
it prints why, leaves the files as they are, and exits normally.
"""
import csv
import html as htmllib
import json
import re
import sys
import time
import unicodedata
import urllib.request
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "fighter_images.json"
HEADS = HERE / "fighter_headshots.json"
ROSTER = "https://www.ufc.com/athletes/all"
ROSTER_PAUSE = 1.0      # seconds between roster pages, to go easy on UFC.com
ROSTER_MAX_PAGES = 400  # the list is a few hundred pages at most
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


def loose(name):
    """Last name plus first initial, for first names spelled differently
    ("Zachary Reese" on UFC.com, "Zach Reese" on DraftKings)."""
    w = re.sub(r"[^a-z\s-]", "", unicodedata.normalize("NFD", str(name or "")).encode("ascii", "ignore").decode().lower())
    w = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", w.strip()).split()
    return f"{w[-1]}|{w[0][0]}" if len(w) > 1 else ""


def dk_name(ufc_name, names, loose_names):
    """The DraftKings spelling for a UFC.com name, or None."""
    return names.get(norm(ufc_name)) or loose_names.get(loose(ufc_name))


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


def load(path):
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def save(path, data):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, ensure_ascii=False)
    tmp.replace(path)


def parse_roster(page):
    """[(name, headshot url or None)] from one page of the athlete list."""
    out = []
    for card in re.split(r'class="c-listing-athlete-flipcard[ "]', page)[1:]:
        m = re.search(r'c-listing-athlete__name"[^>]*>(.*?)</', card, re.S)
        if not m:
            continue
        name = text(m.group(1))
        photo = None
        t = re.search(r'c-listing-athlete__thumbnail"[^>]*>(.*?)</div>\s*</div>', card, re.S)
        im = re.search(r'<img[^>]+?(?:data-src|src)="([^"]+)"', t.group(1) if t else card)
        if im:
            photo = htmllib.unescape(im.group(1))
            if photo.startswith("/"):
                photo = BASE + photo
            if PLACEHOLDER.search(photo):
                photo = None
        if name:
            out.append((name, photo))
    return out


def roster():
    have = load(HEADS).get("fighters", {})
    found, seen_first, pages = {}, None, 0
    for n in range(ROSTER_MAX_PAGES):
        page = fetch(f"{ROSTER}?page={n}")
        if not page:
            break
        cards = parse_roster(page)
        if not cards:
            if n == 0:
                (HERE / "roster_page_debug.html").write_text(page, encoding="utf-8")
                say("found no athletes on the first roster page; saved it as roster_page_debug.html "
                    "so the reader can be fixed. fighter_headshots.json left as it is.")
                return 0
            break
        if cards[0][0] == seen_first:
            break   # past the last page the list repeats itself
        seen_first = cards[0][0]
        pages += 1
        for name, photo in cards:
            if photo:
                found[name] = photo
        if pages % 25 == 0:
            say(f"read {pages} pages, {len(found)} headshots so far...")
        time.sleep(ROSTER_PAUSE)
    if not found:
        say("no headshots found; fighter_headshots.json left as it is.")
        return 0
    merged = dict(have)
    merged.update(found)
    save(HEADS, {"generated": datetime.now().isoformat(timespec="seconds"), "source": ROSTER, "fighters": merged})
    say(f"wrote fighter_headshots.json: {len(found)} headshots from {pages} pages ({len(merged)} in the file)")
    return 0


def events(urls, names, loose_names):
    """Read each event page; with no URLs given, find this week's on UFC.com."""
    named = bool(urls)
    candidates = urls or upcoming_events()
    if not candidates:
        say("no UFC.com event to read; fighter_images.json left as it is.")
        return []
    read = []
    best = None
    for url in candidates:
        page = fetch(url)
        if not page:
            continue
        corners = parse_event(page)
        if not corners:
            if named:
                say(f"found no fight card on {url}")
            continue
        hits = sum(1 for n, _, _ in corners if dk_name(n, names, loose_names))
        if named:
            read.append((hits, url, event_title(page), corners))   # every named event counts
            continue
        if best is None or hits > best[0]:
            best = (hits, url, event_title(page), corners)
        if names and hits >= len(names) * 0.8:
            break   # this is the slate's event; no need to read the rest
    if named:
        return read
    if not best:
        say("found no fight card on UFC.com; fighter_images.json left as it is.")
        return []
    if names and best[0] < max(2, len(names) // 2):
        say(f"no UFC.com event matches DKSalaries.csv (best: {best[2]}, {best[0]} names); "
            "fighter_images.json left as it is.")
        return []
    return [best]


def main():
    if "--roster" in sys.argv:
        return roster()
    names = slate()
    counts = {}
    for n in names.values():
        counts[loose(n)] = counts.get(loose(n), 0) + 1
    # a loose key two slate fighters share would be a guess, so it is left out
    loose_names = {loose(n): n for n in names.values() if loose(n) and counts[loose(n)] == 1}
    urls = [a.split("#")[0] for a in sys.argv[1:] if not a.startswith("--")]
    read = events(urls, names, loose_names)
    if not read:
        return 0

    # keep every fighter from earlier weeks; this run adds to or refreshes them
    old = load(OUT)
    fighters = dict(old.get("fighters", {}))
    for hits, url, title, corners in read:
        added, missing = 0, []
        for ufc_name, photo, link in corners:
            key = dk_name(ufc_name, names, loose_names) or ufc_name   # DraftKings spelling when it matches
            if photo:
                fighters[key] = {"photo": photo, "ufc_name": ufc_name, "profile": link}
                added += 1
            else:
                missing.append(key)
        say(f"{title}: {added} photos" + (f", no photo yet for {', '.join(missing)}" if missing else ""))
    hits, url, title, corners = read[-1] if urls else read[0]
    save(OUT, {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "event": title,
        "event_url": url,
        "fighters": fighters,
    })
    say(f"wrote fighter_images.json ({len(fighters)} fighters in the file)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:   # never stop a publish over photos
        say(f"skipped ({e})")
        sys.exit(0)
