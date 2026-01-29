(function () {
  function initDir(dir) {
    const row = dir.querySelector(":scope > .file-tree-row");
    if (!row || row.dataset.bound === "1") return;
    row.dataset.bound = "1";

    row.setAttribute("aria-expanded", "false");

    const toggle = () => {
      const open = dir.classList.toggle("is-open");
      row.setAttribute("aria-expanded", open ? "true" : "false");
    };

    row.addEventListener("click", (e) => {
      e.stopPropagation();
      toggle();
    });

    row.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        toggle();
      }
    });
  }

  function initTree(tree) {
    if (!tree) return;
    tree.querySelectorAll(":scope .file-tree-item.dir").forEach(initDir);
  }

  function initAllFileTrees() {
    document.querySelectorAll(".file-tree-root").forEach(initTree);
  }

  document.addEventListener("DOMContentLoaded", initAllFileTrees);

  // Expose for pages that inject file trees dynamically.
  window.initFileTrees = initAllFileTrees;
})();
