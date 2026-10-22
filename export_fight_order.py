"""Write fight_order.json from the sim workbook's fight IDs.

Reads the 'DraftKings Fighter Pool' sheet (column A = Fighter, column B =
FightID) and writes fight_order.json beside it, so the website lists this
week's bouts in the same order as the sim. publish.bat runs this before it
stages the contest files.

Usage:  python export_fight_order.py ["Post Contest Sim.xlsm"]
"""
import json
import sys
from datetime import datetime
from pathlib import Path

try:
    import openpyxl
except ImportError:
    print("export_fight_order: openpyxl is not installed (pip install openpyxl); skipping.")
    sys.exit(0)

SHEET = "DraftKings Fighter Pool"


def main():
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "Post Contest Sim.xlsm")
    if not src.exists():
        print(f"export_fight_order: {src} not found; skipping.")
        return 0
    wb = openpyxl.load_workbook(src, read_only=True, data_only=True, keep_vba=False)
    if SHEET not in wb.sheetnames:
        print(f"export_fight_order: sheet '{SHEET}' not found in {src.name}; skipping.")
        return 0
    fights = {}
    for i, row in enumerate(wb[SHEET].iter_rows(values_only=True)):
        if i == 0:
            continue  # header row
        name = str(row[0]).strip() if row and row[0] is not None else ""
        fid = row[1] if row and len(row) > 1 else None
        if not name or fid is None:
            continue
        try:
            fid = int(float(fid))
        except (TypeError, ValueError):
            continue
        fights.setdefault(fid, [])
        if name not in fights[fid]:
            fights[fid].append(name)
    wb.close()
    if not fights:
        print("export_fight_order: no fighters with fight IDs found; skipping.")
        return 0
    out = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "source": src.name,
        "fights": [{"fight_id": fid, "fighters": fights[fid]} for fid in sorted(fights)],
    }
    dest = src.parent / "fight_order.json"
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"export_fight_order: wrote {dest.name} ({len(fights)} fights)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
