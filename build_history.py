#!/usr/bin/env python3
"""
build_history.py - one row per fighter per DraftKings MMA card, for the
site's salary and ownership history.

Inputs (all in this folder):
  dk_salaries/*.csv        every week's DKSalaries export (the archive)
  DKSalaries.csv           this week's export; copied into the archive
                           under its slate date if not already there
  ownership_history.csv    date,event,fighter,projected,actual (the model)
  ufc_fight_stats.csv      the fight database (result, method, stat line)
  contests.json + <contest>.json   the published contests; the Main GPP
                           (largest prize pool) supplies actual ownership
                           for any ownership row whose actual is blank

Output:
  fighter_history.csv      date,event,fighter,opponent,salary,projected,
                           actual,dk_score,result,method,round,time
  ownership_history.csv    rewritten only to fill blank actuals

No third-party packages. Safe to run every publish.
"""
import csv, glob, json, os, re, sys, unicodedata
from datetime import date, datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
P = lambda *a: os.path.join(HERE, *a)
WIN_FINISH = {1: 90, 2: 70, 3: 45, 4: 40, 5: 40}


def norm(s):
    s = unicodedata.normalize('NFD', s or '').encode('ascii', 'ignore').decode().lower()
    s = re.sub(r"[^a-z ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def tokens(s):
    return [t for t in norm(s).split() if t not in ('jr', 'sr', 'ii', 'iii', 'iv')]


def names_match(a, b):
    """Same person? exact, reordered (Song Yadong / Yadong Song), or one side
    a single token contained in the other (Aoriqileng / Aori Aoriqileng)."""
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return False
    if ta == tb or set(ta) == set(tb):
        return True
    if len(ta) == 1 and ta[0] in tb:
        return True
    if len(tb) == 1 and tb[0] in ta:
        return True
    return False


def find_name(name, pool):
    """pool: dict norm-name -> original. Exact first, then the loose rule."""
    n = norm(name)
    if n in pool:
        return pool[n]
    hits = [v for k, v in pool.items() if names_match(name, v)]
    return hits[0] if len(hits) == 1 else None


def num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def time_to_sec(t):
    m = re.match(r"^\s*(\d+):(\d{2})\s*$", str(t or ''))
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def dk_points(r):
    """Mirror of the site's dkPoints(): strikes, kd, td, rev, control, win bonus, quick win."""
    pts = 0.2 * num(r.get('total_str_landed')) + 0.2 * num(r.get('sig_str_landed'))
    pts += 10 * num(r.get('kd')) + 5 * num(r.get('td_landed')) + 5 * num(r.get('rev')) + 0.03 * num(r.get('ctrl_sec'))
    if r.get('result') == 'W':
        rnd = int(round(num(r.get('round'))))
        if (r.get('method') or '').startswith('Decision'):
            pts += 30
        else:
            pts += WIN_FINISH.get(rnd, 0)
            secs = time_to_sec(r.get('time'))
            if rnd == 1 and secs is not None and secs <= 60:
                pts += 25
    return round(pts, 2)


def read_csv(path, enc='utf-8-sig'):
    try:
        with open(path, encoding=enc, newline='') as f:
            return list(csv.DictReader(f))
    except UnicodeDecodeError:
        with open(path, encoding='latin-1', newline='') as f:
            return list(csv.DictReader(f))


def slate_date(rows):
    """Earliest bout date on a DK salary export (Game Info: 'A@B 10/24/2026 10:40AM ET')."""
    best = None
    for r in rows:
        m = re.search(r"(\d{2})/(\d{2})/(\d{4})", r.get('Game Info') or '')
        if m:
            d = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            best = d if best is None or d < best else best
    return best


def load_salary_files():
    """Archive files plus this week's DKSalaries.csv (copied into the archive)."""
    files = sorted(glob.glob(P('dk_salaries', '*.csv')))
    cur = P('DKSalaries.csv')
    if os.path.exists(cur):
        rows = read_csv(cur)
        d = slate_date(rows)
        if d:
            dest = P('dk_salaries', f'DK Salaries {d.strftime("%m.%d.%Y")}.csv')
            if not os.path.exists(dest):
                with open(cur, 'rb') as a, open(dest, 'wb') as b:
                    b.write(a.read())
                files.append(dest)
                print(f'[salaries] archived this week as {os.path.basename(dest)}')
    out = {}
    for f in sorted(set(files)):
        rows = read_csv(f)
        d = slate_date(rows)
        if not d:
            print(f'[salaries] {os.path.basename(f)}: no bout dates, skipped'); continue
        out[d] = rows
    return out


def main_gpp_ownership():
    """fighter -> actual ownership % in the largest-prize published contest, and the card's bout date."""
    try:
        cj = json.load(open(P('contests.json'), encoding='utf-8'))
        main = max(cj['contests'], key=lambda c: c.get('totalPrizes') or 0)
        d = json.load(open(P(main['file']), encoding='utf-8'))
    except Exception:
        return None, {}
    lb = d.get('leaderboard') or []
    if not lb:
        return None, {}
    cnt = {}
    for r in lb:
        for i in range(1, 7):
            f = r.get(f'F{i}')
            if f:
                cnt[f] = cnt.get(f, 0) + 1
    own = {f: round(100.0 * c / len(lb), 2) for f, c in cnt.items()}
    # the card's date comes from the slate the fight card belongs to: match by names below
    return main['name'], own


def main():
    stats = read_csv(P('ufc_fight_stats.csv'))
    by_date = {}
    for r in stats:
        try:
            by_date.setdefault(date.fromisoformat(r['event_date']), []).append(r)
        except Exception:
            pass

    own_rows = read_csv(P('ownership_history.csv')) if os.path.exists(P('ownership_history.csv')) else []
    own_by_date = {}
    for r in own_rows:
        try:
            own_by_date.setdefault(date.fromisoformat(r['date']), []).append(r)
        except Exception:
            pass

    main_name, main_own = main_gpp_ownership()
    salaries = load_salary_files()
    out = []
    filled = 0
    for d, srows in sorted(salaries.items()):
        # the fight-stats event within a day that shares the most names
        pool_names = set(norm(r['Name']) for r in srows)
        cands = [(k, by_date[k]) for k in by_date if abs((k - d).days) <= 1]
        ev_date, ev_rows, ev_name = None, [], ''
        best = -1
        for k, rows in cands:
            hits = len(pool_names & set(norm(r['fighter']) for r in rows))
            if hits > best:
                best, ev_date, ev_rows, ev_name = hits, k, rows, rows[0].get('event_name', '')
        stat_pool = {norm(r['fighter']): r['fighter'] for r in ev_rows}
        stat_by = {r['fighter']: r for r in ev_rows}
        # ownership rows for this card (the sheet may be a day off from the slate date)
        orows = []
        for k, rows in own_by_date.items():
            if abs((k - d).days) <= 1:
                orows += rows
        own_pool = {norm(r['fighter']): r for r in orows}
        # opponents from Game Info: the other fighter in the same game
        by_game = {}
        for r in srows:
            by_game.setdefault(r.get('Game Info', ''), []).append(r['Name'])
        for r in srows:
            name = r['Name']
            opp_list = [x for x in by_game.get(r.get('Game Info', ''), []) if x != name]
            opp = opp_list[0] if opp_list else ''
            sal = int(num(r.get('Salary')))
            # fight stats
            sname = find_name(name, stat_pool)
            st = stat_by.get(sname) if sname else None
            if st and not opp:
                opp = st.get('opponent', '')
            # ownership: the sheet first, then the Main GPP for a blank actual on this card
            orow = own_pool.get(norm(name))
            if orow is None:
                hits = [v for k, v in own_pool.items() if names_match(name, v['fighter'])]
                orow = hits[0] if len(hits) == 1 else None
            proj = orow['projected'].strip() if orow and orow.get('projected') else ''
            act = orow['actual'].strip() if orow and orow.get('actual') else ''
            if not act and main_own:
                # only when the published Main GPP is this card: its names must overlap the slate
                if len(set(norm(x) for x in main_own) & pool_names) >= max(6, len(pool_names) // 2):
                    mo = find_name(name, {norm(k): k for k in main_own})
                    if mo is not None:
                        act = str(main_own[mo])
                        if orow is not None and not (orow.get('actual') or '').strip():
                            orow['actual'] = act; filled += 1
            out.append({
                'date': (ev_date or d).isoformat(), 'event': ev_name or f'UFC {d.strftime("%-m/%-d/%y")}',
                'fighter': name, 'opponent': opp, 'salary': sal,
                'projected': proj, 'actual': act,
                'dk_score': dk_points(st) if st else '', 'result': st.get('result', '') if st else '',
                'method': st.get('method', '') if st else '', 'round': st.get('round', '') if st else '',
                'time': st.get('time', '') if st else '',
            })
        matched = sum(1 for o in out if o['date'] == (ev_date or d).isoformat() and o['result'])
        print(f'[card] {d}  {ev_name or "(no fight data yet)":44s} {len(srows):3d} fighters, {matched:3d} with results')

    cols = ['date', 'event', 'fighter', 'opponent', 'salary', 'projected', 'actual', 'dk_score', 'result', 'method', 'round', 'time']
    with open(P('fighter_history.csv'), 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(out)
    print(f'[done] fighter_history.csv: {len(out)} rows across {len(salaries)} cards')
    if filled:
        with open(P('ownership_history.csv'), 'w', encoding='utf-8', newline='') as f:
            w = csv.DictWriter(f, fieldnames=['date', 'event', 'fighter', 'projected', 'actual']); w.writeheader()
            for r in own_rows:
                w.writerow({k: r.get(k, '') for k in ['date', 'event', 'fighter', 'projected', 'actual']})
        print(f'[ownership] filled {filled} actual values from the Main GPP ({main_name})')


if __name__ == '__main__':
    main()
