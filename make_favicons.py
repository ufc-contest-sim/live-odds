#!/usr/bin/env python3
"""Generate all the favicon / touch-icon sizes Google and browsers want from a
single square logo. Drop your logo in this folder as logo.png (ideally
512x512 or larger, square) and run:  py make_favicons.py

Outputs (next to this script): favicon.ico (16/32/48 in one file),
favicon-96x96.png, favicon-192x192.png, favicon-512x512.png,
apple-touch-icon.png (180x180). Then run publish.bat.
"""
from pathlib import Path
from PIL import Image


def find_logo():
    """Find the logo even if Windows saved it as logo.png.png, or as a jpg/webp.
    Prefers an exact logo.png, then any logo.* (newest first)."""
    here = Path(".")
    exact = here / "logo.png"
    if exact.exists():
        return exact
    cands = sorted(
        (f for f in here.glob("logo.*")
         if f.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")),
        key=lambda f: f.stat().st_mtime, reverse=True)
    return cands[0] if cands else None


def main():
    p = find_logo()
    if p is None:
        print("Put your logo here as 'logo.png' (square image) and re-run.")
        return
    if p.name != "logo.png":
        print(f"note: using '{p.name}' (Windows may have hidden the real extension)")
    img = Image.open(p).convert("RGBA")
    w, h = img.size
    if w != h:
        # pad to a square canvas so nothing gets squished
        s = max(w, h)
        canvas = Image.new("RGBA", (s, s), (255, 255, 255, 0))
        canvas.paste(img, ((s - w) // 2, (s - h) // 2), img)
        img = canvas
        print(f"note: logo was {w}x{h}, padded to square {s}x{s}")

    def resized(size):
        return img.resize((size, size), Image.LANCZOS)

    resized(96).save("favicon-96x96.png")
    resized(192).save("favicon-192x192.png")
    resized(512).save("favicon-512x512.png")
    resized(180).save("apple-touch-icon.png")
    # multi-size .ico for legacy/browser tabs
    img.save("favicon.ico", sizes=[(16, 16), (32, 32), (48, 48)])
    print("wrote: favicon.ico, favicon-96x96.png, favicon-192x192.png, "
          "favicon-512x512.png, apple-touch-icon.png")
    print("Next: run publish.bat")

if __name__ == "__main__":
    main()
