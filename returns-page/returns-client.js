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
    $("rt-sent").textContent = found.orderedOn || found.despatchedOn;
    $("rt-last").textContent = found.lastDay;
    $("rt-items").innerHTML = found.lines.map(function (l, i) {
      // One obvious button per item. A quantity is only asked for when they bought
      // more than one — a "0 / 1 / 2" dropdown confused people (Dec, 29 Sep).
      var pick = l.available > 0
        ? '<div class="rt-item-pick"><button type="button" class="rt-pick">Return this item</button></div>'
        : '<div class="rt-item-sub">Already being returned</div>';
      var howMany = "";
      if (l.available > 1) {
        var o = "";
        for (var n = 1; n <= l.available; n++) o += '<option value="' + n + '">' + n + " of " + l.available + "</option>";
        howMany = '<p class="rt-q">How many are you sending back?</p><select class="rt-qty rt-howmany">' + o + "</select>";
      }
      return '<div class="rt-item" data-i="' + i + '">' +
        '<div class="rt-item-head"><div><div class="rt-item-name">' + esc(l.name) + '</div>' +
          '<div class="rt-item-sub">You ordered ' + l.qty + (l.available < l.qty ? " &middot; " + (l.qty - l.available) + " already being returned" : "") + "</div></div>" +
          pick + "</div>" +
        '<div class="rt-item-body">' + howMany +
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
      var pickBtn = el.querySelector(".rt-pick");
      if (pickBtn) pickBtn.addEventListener("click", function () {
        var on = !el.classList.contains("is-on");
        el.classList.toggle("is-on", on);
        photoNeed();
        pickBtn.innerHTML = on ? "&#10003; Returning this item<small>Tap to undo</small>" : "Return this item";
        fit();
      });
      Array.prototype.forEach.call(el.querySelectorAll(".rt-toggle button"), function (b) {
        b.addEventListener("click", function () {
          el.dataset.out = b.dataset.out;
          Array.prototype.forEach.call(el.querySelectorAll(".rt-toggle button"), function (x) { x.classList.toggle("is-on", x === b); });
          Array.prototype.forEach.call(el.querySelectorAll(".rt-choice"), function (c) { c.classList.toggle("rt-hidden", c.dataset.for !== b.dataset.out); });
          photoNeed(); fit();
        });
      });
      el.querySelector(".rt-exchange").addEventListener("change", function () {
        el.querySelector(".rt-other").classList.toggle("rt-hidden", this.value !== "Something else"); fit();
      });
      el.querySelector(".rt-reason").addEventListener("change", function () {
        el.querySelector(".rt-note").classList.toggle("rt-hidden", !/faulty|wrong item/i.test(this.value)); photoNeed(); fit();
      });
    });

    photos = []; drawThumbs(); photoNeed();
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
      if (!el.classList.contains("is-on")) continue;
      var qty = q ? Number(q.value) : 1;
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
    if (!lines.length) return msg("rt-submit-msg", 'Press "Return this item" on each item you are sending back.');
    if (photoNeed() && !photos.length) { $("rt-photos").scrollIntoView({ behavior: "smooth", block: "center" }); return msg("rt-submit-msg", "As something is faulty or damaged, please add a photo of the problem so we can see it."); }
    var email = $("rt-email").value.trim();
    if (found.needsEmail && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return msg("rt-submit-msg", "We need an email address to send your returns reference to.");
    $("rt-submit").disabled = true; msg("rt-submit-msg", "");
    post("/api/returns/submit", { orderNumber: query.orderNumber, postcode: query.postcode, email: email, lines: lines, comments: $("rt-comments").value.trim(), photos: photos.map(function (x) { return { contentType: x.contentType, base64: x.base64 }; }) }).then(function (j) {
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
      if (photos.length) $("rt-done-mail").innerHTML += " Thanks for the photo" + (photos.length === 1 ? "" : "s") + " &ndash; our team will take a look and be in touch if we need anything.";
      show("rt-choose", false); show("rt-done", true);
      top();
    });
  });

  // ---- Photos --------------------------------------------------------------------
  // Shrunk on the device to a 1600px JPEG before sending: a phone photo is 3-8 MB,
  // this makes it ~300 KB, so it uploads quickly on mobile data and fits in an email.
  var photos = [], MAX_PHOTOS = 6;
  function photoNeed() {
    var need = Array.prototype.some.call(document.querySelectorAll(".rt-item.is-on"), function (el) {
      return el.dataset.out === "refund" && /faulty|damaged/i.test(el.querySelector(".rt-reason").value);
    });
    $("rt-photos").classList.toggle("is-needed", need);
    $("rt-photos-title").innerHTML = need ? "Photos of the problem <em>(needed)</em>" : "Photos <em>(optional)</em>";
    $("rt-photos-help").textContent = need
      ? "Please add at least one photo showing the fault or damage, so we can see what's wrong."
      : "If anything's damaged or not right, a photo helps us sort it out quicker.";
    return need;
  }
  function drawThumbs() {
    $("rt-thumbs").innerHTML = photos.map(function (x, i) {
      return '<div class="rt-thumb" style="background-image:url(' + x.url + ')"><button type="button" data-i="' + i + '" aria-label="Remove photo">&times;</button></div>';
    }).join("");
    Array.prototype.forEach.call($("rt-thumbs").querySelectorAll("button"), function (b) {
      b.addEventListener("click", function () { photos.splice(Number(b.dataset.i), 1); drawThumbs(); });
    });
    fit();
  }
  function shrink(file) {
    return new Promise(function (resolve) {
      var img = new Image(), url = URL.createObjectURL(file);
      img.onload = function () {
        var s = Math.min(1, 1600 / Math.max(img.width, img.height));
        var c = document.createElement("canvas");
        c.width = Math.round(img.width * s); c.height = Math.round(img.height * s);
        c.getContext("2d").drawImage(img, 0, 0, c.width, c.height);
        URL.revokeObjectURL(url);
        var data = c.toDataURL("image/jpeg", 0.82);
        resolve({ contentType: "image/jpeg", base64: data.split(",")[1], url: data });
      };
      img.onerror = function () { URL.revokeObjectURL(url); resolve(null); };
      img.src = url;
    });
  }
  $("rt-photo-input").addEventListener("change", function () {
    var files = Array.prototype.slice.call(this.files || []);
    this.value = "";
    var room = MAX_PHOTOS - photos.length;
    if (files.length > room) msg("rt-submit-msg", "You can add up to " + MAX_PHOTOS + " photos.", "info");
    Promise.all(files.slice(0, Math.max(0, room)).map(shrink)).then(function (out) {
      var bad = out.filter(function (x) { return !x; }).length;
      photos = photos.concat(out.filter(Boolean));
      drawThumbs();
      if (bad) msg("rt-submit-msg", "One of those files isn't a photo we can open. Try a JPG or PNG.");
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
