(function () {
  // The page is served two ways: on its own (inside a copy of the site's header and
  // footer), and embedded in a page on tuffshop.co.uk (/returns-form) where the site
  // supplies the header. Embedded, it reports its height so the frame fits it.
  var embedded = document.documentElement.classList.contains("rt-embed");
  var API = embedded ? (window.RT_API_BASE || "") : "";
  function fit() {
    if (!embedded || window.parent === window) return;
    // The form's own bottom edge: the theme gives the page a minimum height, so the
    // document's height overstates it and leaves a gap under the form.
    var rt = document.querySelector(".rt");
    var h = rt ? Math.ceil(rt.getBoundingClientRect().bottom + window.pageYOffset) + 10 : document.documentElement.scrollHeight;
    window.parent.postMessage({ tuffReturnsHeight: h }, "*");
  }
  function top() {
    if (embedded) window.parent.postMessage({ tuffReturnsScrollTop: true }, "*");
    else window.scrollTo({ top: 0, behavior: "smooth" });
  }

  var $ = function (id) { return document.getElementById(id); };
  var esc = function (s) { return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; }); };
  var show = function (id, on) { $(id).classList.toggle("rt-hidden", !on); fit(); };
  var msg = function (id, text, kind) {
    $(id).innerHTML = text ? '<div class="rt-msg ' + (kind === "info" ? "" : "rt-msg--err") + '">' + esc(text) + "</div>" : "";
    fit();
  };
  var found = null, query = null;

  function post(url, body) {
    return fetch(API + url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
      .then(function (r) { return r.json(); })
      .catch(function () { return { ok: false, message: "Something went wrong there. Please try again, or give us a call on 0113 288 7713." }; });
  }

  $("rt-find-form").addEventListener("submit", function (e) {
    e.preventDefault();
    var orderNumber = $("rt-order").value.trim(), postcode = $("rt-postcode").value.trim();
    if (!orderNumber || !postcode) return msg("rt-find-msg", "We need your order number and the postcode it was delivered to.");
    $("rt-find-btn").disabled = true; msg("rt-find-msg", "");
    post("/api/returns/lookup", { orderNumber: orderNumber, postcode: postcode }).then(function (j) {
      $("rt-find-btn").disabled = false;
      if (!j.ok) return msg("rt-find-msg", j.message || "We can't find that order.", j.code === "already" ? "info" : "err");
      found = j; query = { orderNumber: orderNumber, postcode: postcode };
      render();
    });
  });

  function options(list, placeholder) {
    return '<option value="">' + esc(placeholder) + "</option>" + list.map(function (x) { return "<option>" + esc(x) + "</option>"; }).join("");
  }

  function render() {
    $("rt-ref").textContent = found.orderRef;
    $("rt-sent").textContent = found.despatchedOn;
    $("rt-last").textContent = found.lastDay;
    $("rt-items").innerHTML = found.lines.map(function (l, i) {
      var qty = "";
      if (l.available > 0) {
        var o = "";
        for (var n = 0; n <= l.available; n++) o += '<option value="' + n + '">' + (n === 0 ? "Keeping it" : "Sending back " + n) + "</option>";
        qty = '<div class="rt-item-qty"><select class="rt-qty" aria-label="How many are coming back">' + o + "</select></div>";
      }
      return '<div class="rt-item" data-i="' + i + '">' +
        '<div class="rt-item-head"><div><div class="rt-item-name">' + esc(l.name) + '</div>' +
          '<div class="rt-item-sub">You ordered ' + l.qty + (l.available < l.qty ? " &middot; " + (l.qty - l.available) + " already on its way back" : "") + "</div></div>" +
          (qty || '<div class="rt-item-sub">Already on its way back</div>') + "</div>" +
        '<div class="rt-item-body">' +
          '<p class="rt-q">What would you like?</p>' +
          '<div class="rt-toggle">' +
            '<button type="button" data-out="exchange">Swap it<small>Free delivery on the new one</small></button>' +
            '<button type="button" data-out="refund">Refund<small>Money back once it\'s with us</small></button>' +
          "</div>" +
          '<div class="rt-choice rt-hidden" data-for="exchange"><p class="rt-q">What would you like instead?</p>' +
            '<select class="rt-exchange">' + options(found.exchangeChoices, "Choose one...") + "</select>" +
            '<div class="rt-other rt-hidden"><input type="text" class="input-text rt-exchange-for" maxlength="200" placeholder="e.g. the same trousers in 34R, or the black ones instead"></div></div>' +
          '<div class="rt-choice rt-hidden" data-for="refund"><p class="rt-q">Why is it coming back?</p>' +
            '<select class="rt-reason">' + options(found.refundReasons, "Choose a reason...") + "</select>" +
            '<div class="rt-note rt-hidden">Sorry about that. Give us a call on 0113 288 7713 before you send it back and we\'ll get it sorted quickly.</div></div>' +
        "</div></div>";
    }).join("");

    Array.prototype.forEach.call(document.querySelectorAll(".rt-item"), function (el) {
      var q = el.querySelector(".rt-qty");
      if (q) q.addEventListener("change", function () { el.classList.toggle("is-on", Number(q.value) > 0); fit(); });
      Array.prototype.forEach.call(el.querySelectorAll(".rt-toggle button"), function (b) {
        b.addEventListener("click", function () {
          el.dataset.out = b.dataset.out;
          Array.prototype.forEach.call(el.querySelectorAll(".rt-toggle button"), function (x) { x.classList.toggle("is-on", x === b); });
          Array.prototype.forEach.call(el.querySelectorAll(".rt-choice"), function (c) { c.classList.toggle("rt-hidden", c.dataset.for !== b.dataset.out); });
          fit();
        });
      });
      el.querySelector(".rt-exchange").addEventListener("change", function () {
        el.querySelector(".rt-other").classList.toggle("rt-hidden", this.value !== "Something else"); fit();
      });
      el.querySelector(".rt-reason").addEventListener("change", function () {
        el.querySelector(".rt-note").classList.toggle("rt-hidden", !/faulty|wrong item/i.test(this.value)); fit();
      });
    });

    $("rt-email-field").classList.toggle("rt-hidden", !found.needsEmail);
    $("rt-email-hint").innerHTML = found.needsEmail ? "" : "We'll email your returns reference to <b>" + esc(found.emailHint) + "</b>, the address on your order.";
    msg("rt-submit-msg", "");
    show("rt-find", false); show("rt-choose", true); show("rt-done", false);
    top();
  }

  $("rt-again").addEventListener("click", function (e) {
    e.preventDefault(); found = null;
    show("rt-choose", false); show("rt-find", true); msg("rt-find-msg", ""); top();
  });

  $("rt-submit").addEventListener("click", function () {
    var lines = [], items = document.querySelectorAll(".rt-item");
    for (var i = 0; i < items.length; i++) {
      var el = items[i], l = found.lines[Number(el.dataset.i)], q = el.querySelector(".rt-qty");
      var qty = q ? Number(q.value) : 0;
      if (!qty) continue;
      var out = el.dataset.out;
      if (!out) return msg("rt-submit-msg", 'Would you like to swap "' + l.name + '" or get a refund?');
      if (out === "exchange") {
        var choice = el.querySelector(".rt-exchange").value, other = el.querySelector(".rt-exchange-for").value.trim();
        if (!choice) return msg("rt-submit-msg", 'What would you like instead of "' + l.name + '"?');
        if (choice === "Something else" && !other) return msg("rt-submit-msg", 'Tell us what you\'d like instead of "' + l.name + '".');
        lines.push({ rowId: l.rowId, qty: qty, outcome: "exchange", exchangeChoice: choice, exchangeFor: other });
      } else {
        var reason = el.querySelector(".rt-reason").value;
        if (!reason) return msg("rt-submit-msg", 'Let us know why "' + l.name + '" is coming back.');
        lines.push({ rowId: l.rowId, qty: qty, outcome: "refund", reason: reason });
      }
    }
    if (!lines.length) return msg("rt-submit-msg", "Choose how many of each item you're sending back.");
    var email = $("rt-email").value.trim();
    if (found.needsEmail && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return msg("rt-submit-msg", "We need an email address to send your returns reference to.");
    $("rt-submit").disabled = true; msg("rt-submit-msg", "");
    post("/api/returns/submit", { orderNumber: query.orderNumber, postcode: query.postcode, email: email, lines: lines, comments: $("rt-comments").value.trim() }).then(function (j) {
      $("rt-submit").disabled = false;
      if (!j.ok) return msg("rt-submit-msg", j.message || "Something went wrong there. Please try again.");
      $("rt-done-ref").textContent = j.ref;
      Array.prototype.forEach.call(document.querySelectorAll(".rt-done-ref2"), function (x) { x.textContent = j.ref; });
      $("rt-done-addr").innerHTML = j.address.map(esc).join("<br>");
      $("rt-done-by").textContent = j.lastDay;
      $("rt-done-next").textContent = j.exchanging
        ? "Once it's here and we've checked it over, we'll send your replacement out with free standard delivery and let you know."
        : "Once it's here and we've checked it over, we'll sort your refund and let you know. Refunds normally go through within 14 days.";
      $("rt-done-contact").innerHTML = j.needsContact
        ? '<div class="rt-msg">As something\'s not right with your order, please give us a call on <b>0113 288 7713</b> before you post it and we\'ll get it sorted.</div>' : "";
      $("rt-done-mail").innerHTML = j.emailed
        ? "We've emailed all of this to <b>" + esc(j.emailHint) + "</b> too."
        : "We couldn't send the email, so please make a note of your reference. Our team has been told.";
      show("rt-choose", false); show("rt-done", true);
      top();
    });
  });

  // Mobile menu: the site's own needs scripts that only run on tuffshop.co.uk, so the
  // hamburger opens a plain list of the same top-level categories instead.
  var toggle = document.querySelector('[data-action="toggle-mobile-nav"]');
  if (toggle && !embedded) {
    var links = Array.prototype.map.call(document.querySelectorAll(".ox-megamenu-navigation > li > a.level-top"), function (a) {
      return '<a href="' + esc(a.href) + '">' + esc(a.textContent.trim()) + "</a>";
    }).join("");
    var nav = document.createElement("div");
    nav.className = "rt-mnav";
    nav.innerHTML = '<div class="rt-mnav-panel"><a href="#" class="rt-mnav-close">&times; Close</a>' + links +
      '<a href="https://tuffshop.co.uk/customer/account/">My account</a><a href="https://tuffshop.co.uk/checkout/cart/">My cart</a></div>';
    document.body.appendChild(nav);
    toggle.style.cursor = "pointer";
    toggle.addEventListener("click", function () { nav.classList.add("is-open"); });
    nav.addEventListener("click", function (e) { if (e.target === nav || e.target.classList.contains("rt-mnav-close")) { e.preventDefault(); nav.classList.remove("is-open"); } });
  }

  window.addEventListener("load", fit);
  window.addEventListener("resize", fit);
  fit();
})();
