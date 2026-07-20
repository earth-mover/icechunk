"""MkDocs hook that renders tracey requirement anchors as linkable chips.

Tracey requires its requirement anchors (e.g. ``ic[layout.root]``) to appear
as visible plain text in the spec markdown: on a line of their own at column
0, or at the start of a blockquote. Without this hook they show up as
literal ``ic[...]`` artifacts in the rendered site.

This hook rewrites each anchor line at build time into a small deep-linkable
``<a>`` chip (styled by ``stylesheets/tracey.css``), e.g.::

    ic[layout.root]  ->  <a class="tracey-req" id="ic-layout.root" ...>

When the anchor starts a blockquote, the whole blockquote is unwrapped so
the requirement text renders as regular paragraphs rather than a quote box.

Pages containing at least one anchor also get a show/hide toggle injected
below their H1 title (wired up by ``scripts/tracey.js``).

The markdown files on disk are untouched, so tracey keeps parsing them.
"""

import re

# An anchor line: optional blockquote prefix, then exactly `ic[req.id]`,
# tolerating a future `+N` version suffix.
ANCHOR_RE = re.compile(
    r"^(?P<quote>>\s?)?ic\[(?P<req>[a-z0-9][a-z0-9._-]*(?:\+\d+)?)\]\s*$"
)

FENCE_RE = re.compile(r"^\s*(```|~~~)")

QUOTE_PREFIX_RE = re.compile(r"^> ?")

TOGGLE_HTML = (
    '<label class="tracey-toggle">'
    '<input type="checkbox"> Show requirement anchors'
    "</label>"
)


def on_page_markdown(markdown, page, config, files):
    out = []
    fence = None
    unquoting = False
    found_anchor = False
    h1_index = None
    for line in markdown.splitlines():
        # Keep stripping the `>` prefix until the anchor-led blockquote ends.
        if unquoting:
            if line.startswith(">"):
                out.append(QUOTE_PREFIX_RE.sub("", line))
                continue
            unquoting = False
        fence_match = FENCE_RE.match(line)
        if fence_match:
            marker = fence_match.group(1)
            if fence is None:
                fence = marker
            elif fence == marker:
                fence = None
        elif fence is None:
            if h1_index is None and line.startswith("# "):
                h1_index = len(out)
            m = ANCHOR_RE.match(line)
            if m:
                req = m.group("req")
                line = f'<a class="tracey-req" id="ic-{req}" href="#ic-{req}">{req}</a>'
                unquoting = m.group("quote") is not None
                found_anchor = True
        out.append(line)
    if found_anchor:
        at = h1_index + 1 if h1_index is not None else 0
        out[at:at] = ["", TOGGLE_HTML, ""]
    return "\n".join(out)
