# /// script
# requires-python = ">=3.11"
# dependencies = ["pillow"]
# ///
"""Vendor diagrams from icechunk-illustrated into the docs.

Copies the built pages and their assets into ``docs/assets/illustrated/`` and
patches them so they follow the docs' light/dark theme:

- every color literal in the CSS becomes ``var(--il-<hex>, <original>)``, and
  ``theme.css`` defines light values for those variables;
- the few colors the canvas code hard-codes pick their light value at load;
- the page reads ``?theme=light|dark`` into ``<html data-theme>``.

Usage (from icechunk-python/docs)::

    git -C ../icechunk-illustrated worktree add /tmp/illustrated origin/main
    (cd /tmp/illustrated && npm ci && npm run build)
    uv run tools/vendor_illustrated.py /tmp/illustrated/dist
"""

import re
import shutil
import sys
from pathlib import Path

from PIL import Image

PAGES = ["virtual/netcdf", "commits", "consistency"]
OUT = Path(__file__).resolve().parent.parent / "docs" / "assets" / "illustrated"

# Light values keyed by the dark color as 8-digit lowercase hex. White at any
# alpha becomes the docs' dark blue at the same alpha, so faint grid lines and
# panel tints stay faint on a white page.
LIGHT = {
    "0d1117ff": "#f5f5f5",  # the figure frame on the docs page
    "0d1117eb": "#ffffffeb",
    "161b22ff": "#f5f8fb",
    "1c2230ff": "#e4f1f8",
    "1b2530ff": "#e4f1f8",
    "e7edf3ff": "#1f2933",
    "8b97a6ff": "#5c6773",
    "6b7785ff": "#9aa5b1",
    "06231bff": "#ffffff",
    "ffd43bff": "#b98900",
    "ffd43b73": "#e0a80073",
    "4dabf7ff": "#1d467f",
    "9775faff": "#a653ff",
    "5ea0d1ff": "#2f6fa8",
    "4ec48cff": "#1f9d63",
    "e57a77ff": "#d64545",
    "d8a05aff": "#b7791f",
}
DARK_BACKGROUND = "#272a35"  # the figure frame on the slate docs page

# Colors the canvas code passes straight to the 2D context.
JS_COLORS = {
    '"#1b2530"': "#e4f1f8",
    '"rgba(15, 20, 25, 0.62)"': "rgba(255, 255, 255, 0.68)",
    '"rgba(255, 255, 255, 0.28)"': "rgba(29, 70, 127, 0.12)",
}

# The NetCDF diagram alternates its archive between a climate format and a
# bio format each round, to show that virtual chunks are not climate-only.
# The label code runs once per round (file 0 first), so file 0 flips it.
FORMAT_SWAP = """<script>
(() => {
  const formats = [
    { stem: "file", ext: ".nc", label: "NetCDF / GRIB" },
    { stem: "ds", ext: ".h5ad", label: "AnnData h5ad" },
  ];
  let round = -1;
  window.__ilFileName = (i) => {
    if (i === 0) {
      round += 1;
      const label = formats[round % formats.length].label;
      for (const el of document.querySelectorAll("[data-il-format]")) el.textContent = label;
    }
    const f = formats[round % formats.length];
    return `${f.stem}_${String(i).padStart(2, "0")}${f.ext}`;
  };
})();
</script>
"""
FILE_NAME_RE = re.compile(r'`file_\$\{String\((\w+)\)\.padStart\(2,"0"\)\}\.nc`')

COLOR_RE = re.compile(r"#[0-9a-fA-F]{3,8}\b|rgba?\([^)]*\)")
ASSET_RE = re.compile(r'(?:src|href)="((?:\.\./)+assets/[^"]+)"|from"\./([^"]+)"')


def to_hex8(color: str) -> str:
    if color.startswith("#"):
        h = color[1:].lower()
        if len(h) in (3, 4):
            h = "".join(c * 2 for c in h)
        return h if len(h) == 8 else h + "ff"
    parts = [p.strip() for p in color[color.index("(") + 1 : -1].split(",")]
    r, g, b = (int(p) for p in parts[:3])
    a = float(parts[3]) if len(parts) == 4 else 1.0
    return f"{r:02x}{g:02x}{b:02x}{round(a * 255):02x}"


def light_value(key: str) -> str:
    if key in LIGHT:
        return LIGHT[key]
    if key.startswith("ffffff"):
        return "#1d467f" + key[6:]
    raise SystemExit(f"no light value for #{key}: add it to LIGHT")


def must_replace(text: str, old: str, new: str, where: str) -> str:
    if old not in text:
        raise SystemExit(f"{where}: {old!r} not found; upstream changed?")
    return text.replace(old, new)


def theme_css(css: str) -> tuple[str, set[str]]:
    keys: set[str] = set()

    def repl(m: re.Match[str]) -> str:
        key = to_hex8(m.group(0))
        keys.add(key)
        return f"var(--il-{key}, {m.group(0)})"

    return COLOR_RE.sub(repl, css), keys


def main(dist: Path) -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    (OUT / "assets").mkdir(parents=True)
    (OUT / "images").mkdir()

    needed: set[str] = set()
    for page in PAGES:
        html = (dist / page / "index.html").read_text()
        needed |= {Path(m[0]).name for m in ASSET_RE.findall(html) if m[0]}
        depth = "../" * (page.count("/") + 1)
        bootstrap = (
            "<script>document.documentElement.dataset.theme="
            'new URLSearchParams(location.search).get("theme")||"dark"</script>\n'
            f'    <link rel="stylesheet" href="{depth}theme.css">\n'
        )
        html = html.replace("<head>\n", "<head>\n    " + bootstrap, 1)
        if page == "virtual/netcdf":
            for label in ("Archival (NetCDF / GRIB)", "Native NetCDF / GRIB reader"):
                html = must_replace(
                    html,
                    label,
                    label.replace(
                        "NetCDF / GRIB", "<span data-il-format>NetCDF / GRIB</span>"
                    ),
                    page,
                )
            html = html.replace("</head>", FORMAT_SWAP + "  </head>", 1)
        (OUT / page).mkdir(parents=True, exist_ok=True)
        (OUT / page / "index.html").write_text(html)

    pending = list(needed)
    while pending:
        name = pending.pop()
        text = (dist / "assets" / name).read_text()
        for m in ASSET_RE.findall(text):
            if m[1] and m[1] not in needed:
                needed.add(m[1])
                pending.append(m[1])

    keys: set[str] = set()
    unused_js_colors = set(JS_COLORS)
    for name in sorted(needed):
        text = (dist / "assets" / name).read_text()
        if name.endswith(".css"):
            text, found = theme_css(text)
            keys |= found
        else:
            for dark, light in JS_COLORS.items():
                if dark in text:
                    unused_js_colors.discard(dark)
                text = text.replace(
                    dark,
                    f'(document.documentElement.dataset.theme==="light"?"{light}":{dark})',
                )
            text = re.sub(r"\.\./images/(state[12])\.png", r"../images/\1.webp", text)
            if name.startswith("virtualNetcdf-"):
                text, n = FILE_NAME_RE.subn(r"window.__ilFileName(\1)", text)
                if n != 1:
                    raise SystemExit(
                        f"{name}: file-name template not found; upstream changed?"
                    )
        (OUT / "assets" / name).write_text(text)

    if unused_js_colors:
        raise SystemExit(
            f"canvas colors not found: {sorted(unused_js_colors)}; upstream changed?"
        )

    for name in ("state1", "state2"):
        img = Image.open(dist / "images" / f"{name}.png")
        img.thumbnail((720, 720))
        img.save(OUT / "images" / f"{name}.webp", quality=85)

    light = "\n".join(f"  --il-{k}: {light_value(k)};" for k in sorted(keys))
    (OUT / "theme.css").write_text(
        "/* Generated by tools/vendor_illustrated.py. */\n"
        f':root[data-theme="light"] {{\n{light}\n}}\n'
        f':root[data-theme="dark"] {{\n  --il-0d1117ff: {DARK_BACKGROUND};\n}}\n'
        "/* The back link to the illustrated index has no place in the docs. */\n"
        ".crumb {\n  display: none;\n}\n"
        "/* Embedded frames are sized to their content, so a cell size that\n"
        "   depends on the frame's height would grow without end. */\n"
        "@media (max-width: 760px) {\n"
        "  :root:root {\n    --cell: clamp(96px, 40vw, 132px);\n  }\n}\n"
        "/* The commits snapshot panel collapses beside a wide storage panel;\n"
        "   give it a floor so its label isn't clipped. */\n"
        "@media (min-width: 761px) {\n"
        "  .ck-stage.ck-stage {\n"
        "    grid-template-columns: minmax(200px, 1fr) minmax(0, 2.4fr);\n"
        "  }\n}\n"
    )
    print(f"vendored {len(PAGES)} pages, {len(needed)} assets, {len(keys)} colors")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
