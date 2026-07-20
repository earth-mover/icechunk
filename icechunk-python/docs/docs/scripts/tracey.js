/* Show/hide tracey requirement chips via the .tracey-toggle checkbox
   injected by hooks/tracey_anchors.py. Hidden by default; the choice
   persists in localStorage. Deep links to a chip (#ic-...) force the
   chips visible so the link target exists. Re-binds on every page
   render to support Material's instant navigation. */
(function () {
  var KEY = "tracey-anchors-visible";

  function init() {
    var input = document.querySelector(".tracey-toggle input");
    if (!input) return;
    var visible = localStorage.getItem(KEY) === "true";
    var deepLink = location.hash.indexOf("#ic-") === 0;
    if (deepLink) visible = true;
    document.body.classList.toggle("tracey-anchors-visible", visible);
    input.checked = visible;
    if (deepLink) {
      var el = document.getElementById(location.hash.slice(1));
      if (el) el.scrollIntoView();
    }
    input.addEventListener("change", function () {
      document.body.classList.toggle("tracey-anchors-visible", input.checked);
      localStorage.setItem(KEY, String(input.checked));
    });
  }

  if (window.document$) {
    window.document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
