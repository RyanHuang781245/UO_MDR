(function () {
  "use strict";

  function submitPostForm(action, fields) {
    var form = document.createElement("form");
    form.method = "post";
    form.action = action;
    var entries = fields ? Object.keys(fields) : [];
    for (var i = 0; i < entries.length; i++) {
      var name = entries[i];
      var input = document.createElement("input");
      input.type = "hidden";
      input.name = name;
      input.value = fields[name];
      form.appendChild(input);
    }
    document.body.appendChild(form);
    form.submit();
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  window.UiUtils = Object.assign(window.UiUtils || {}, {
    submitPostForm: submitPostForm,
    escapeHtml: escapeHtml
  });
})();
