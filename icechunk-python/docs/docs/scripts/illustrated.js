// Icechunk Illustrated diagrams are vendored into assets/illustrated/ and
// embedded as <iframe data-illustrated="<page>" data-params="<query>">.
// They take ?theme=, and their canvas colors are read once at load, so a
// theme change reloads them. They reflow with width, so each frame is sized
// to its content.
(() => {
  const base = new URL("../assets/illustrated/", document.currentScript.src);

  const fit = (frame) => {
    const body = frame.contentDocument?.body;
    if (!body) return;
    const resize = () => {
      frame.style.height = `${body.scrollHeight}px`;
    };
    new ResizeObserver(resize).observe(body);
    resize();
  };

  const sync = () => {
    const scheme = document.body.getAttribute("data-md-color-scheme");
    const theme = scheme === "slate" ? "dark" : "light";
    for (const frame of document.querySelectorAll("iframe[data-illustrated]")) {
      frame.onload = () => fit(frame);
      const params = new URLSearchParams(frame.dataset.params);
      params.set("diagram", "");
      params.set("theme", theme);
      const src = new URL(`${frame.dataset.illustrated}/?${params}`, base).href;
      if (frame.src !== src) frame.src = src;
    }
  };

  document$.subscribe(sync);
  new MutationObserver(sync).observe(document.body, {
    attributes: true,
    attributeFilter: ["data-md-color-scheme"],
  });
})();
