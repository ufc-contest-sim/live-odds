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

SRC = "logo.png"

def main():
    p = Path(SRC)
    if not p.exists():
        print(f"Put your logo here as '{SRC}' (square PNG) and re-run.")
        return
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
