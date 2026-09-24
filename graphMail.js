// Microsoft Graph mail for the Sales Hub — reads the sales inbox and sends from it.
//
// tuffshop.co.uk is Microsoft 365, so sending through Graph is sending as the real mailbox:
// covered by SPF/DKIM (smtp2go is in neither), lands in Sent Items, and a reply stays in the
// customer's thread. App-only (client credentials). The app registration "Tuffshop CS Helpdesk"
// carries NO roles in its token — its mailbox access is granted in Exchange (RBAC for
// Applications), which is what limits it to the sales mailbox rather than every mailbox.
//
// Env: MS_TENANT_ID, MS_CLIENT_ID, MS_CLIENT_SECRET (secret expires 23/03/2027),
//      SALES_MAILBOX (default sales@tuffshop.co.uk).

const GRAPH = "https://graph.microsoft.com/v1.0";

export const graphConfigured = () =>
  !!(process.env.MS_TENANT_ID && process.env.MS_CLIENT_ID && process.env.MS_CLIENT_SECRET);

export const salesMailbox = () => process.env.SALES_MAILBOX || "sales@tuffshop.co.uk";

let cached = { token: null, until: 0 };

async function token() {
  if (cached.token && Date.now() < cached.until) return cached.token;
  const r = await fetch(`https://login.microsoftonline.com/${process.env.MS_TENANT_ID}/oauth2/v2.0/token`, {
    method: "POST",
    body: new URLSearchParams({
      client_id: process.env.MS_CLIENT_ID,
      client_secret: process.env.MS_CLIENT_SECRET,
      scope: "https://graph.microsoft.com/.default",
      grant_type: "client_credentials",
    }),
  });
  const j = await r.json().catch(() => ({}));
  if (!j.access_token) throw new Error(`Graph sign-in failed: ${j.error || r.status}`);
  cached = { token: j.access_token, until: Date.now() + (Number(j.expires_in || 3600) - 300) * 1000 };
  return cached.token;
}

async function graph(method, path, body, { html = false } = {}) {
  for (let attempt = 0; ; attempt++) {
    const r = await fetch(`${GRAPH}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${await token()}`,
        "Content-Type": "application/json",
        // Bodies come back as text, so a customer's HTML never reaches the page as markup —
        // except when building a reply, which must keep the quoted thread as HTML.
        Prefer: `outlook.body-content-type="${html ? "html" : "text"}"`,
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (r.status === 429 && attempt < 3) {
      await new Promise((s) => setTimeout(s, (parseInt(r.headers.get("retry-after") || "2", 10) + 1) * 1000));
      continue;
    }
    if (r.status === 202 || r.status === 204) return null;
    const j = await r.json().catch(() => null);
    if (!r.ok) {
      const e = new Error(`Graph ${method} ${path.split("?")[0]} -> ${r.status}: ${(j && j.error && j.error.message) || ""}`);
      e.status = r.status;
      throw e;
    }
    return j;
  }
}

const mb = () => `/users/${encodeURIComponent(salesMailbox())}`;

// Newest first. Summary fields only — the body is fetched when someone opens one.
export async function listInbox({ top = 30, unreadOnly = false } = {}) {
  const q = new URLSearchParams({
    $top: String(Math.min(Number(top) || 30, 100)),
    $orderby: "receivedDateTime desc",
    $select: "id,subject,from,receivedDateTime,isRead,bodyPreview,conversationId,hasAttachments",
  });
  if (unreadOnly) q.set("$filter", "isRead eq false");
  const j = await graph("GET", `${mb()}/mailFolders/inbox/messages?${q}`);
  return (j.value || []).map((m) => ({
    id: m.id,
    subject: m.subject || "",
    fromName: (m.from && m.from.emailAddress && m.from.emailAddress.name) || "",
    fromAddress: (m.from && m.from.emailAddress && m.from.emailAddress.address) || "",
    receivedAt: m.receivedDateTime,
    isRead: !!m.isRead,
    preview: m.bodyPreview || "",
    hasAttachments: !!m.hasAttachments,
  }));
}

export async function getMessage(id) {
  const m = await graph("GET", `${mb()}/messages/${encodeURIComponent(id)}?$select=id,subject,from,receivedDateTime,body,conversationId`);
  return {
    id: m.id,
    subject: m.subject || "",
    fromName: (m.from && m.from.emailAddress && m.from.emailAddress.name) || "",
    fromAddress: (m.from && m.from.emailAddress && m.from.emailAddress.address) || "",
    receivedAt: m.receivedDateTime,
    text: (m.body && m.body.content) || "",
  };
}

// Reply inside the customer's thread: createReply sets the threading headers and quotes the
// original; our text goes ABOVE that quote, the way a person replying in Outlook would.
export async function replyToMessage(id, { html, to, replyTo, subject }) {
  const draft = await graph("POST", `${mb()}/messages/${encodeURIComponent(id)}/createReply`, {}, { html: true });
  const quoted = (draft.body && draft.body.content) || "";
  const content = /<body[^>]*>/i.test(quoted) ? quoted.replace(/<body[^>]*>/i, (tag) => `${tag}${html}<br>`) : `${html}<br>${quoted}`;
  const patch = { body: { contentType: "HTML", content } };
  if (subject) patch.subject = subject;
  if (to) patch.toRecipients = [{ emailAddress: { address: to } }];
  if (replyTo) patch.replyTo = [{ emailAddress: { address: replyTo } }];
  await graph("PATCH", `${mb()}/messages/${encodeURIComponent(draft.id)}`, patch);
  await graph("POST", `${mb()}/messages/${encodeURIComponent(draft.id)}/send`);
  return { via: "graph-reply", mailbox: salesMailbox() };
}

export async function sendNew({ to, subject, html, replyTo }) {
  await graph("POST", `${mb()}/sendMail`, {
    message: {
      subject,
      body: { contentType: "HTML", content: html },
      toRecipients: [{ emailAddress: { address: to } }],
      ...(replyTo ? { replyTo: [{ emailAddress: { address: replyTo } }] } : {}),
    },
    saveToSentItems: true,
  });
  return { via: "graph", mailbox: salesMailbox() };
}

export async function markRead(id) { return setRead(id, true); }

export async function setRead(id, isRead) {
  await graph("PATCH", `${mb()}/messages/${encodeURIComponent(id)}`, { isRead: !!isRead });
}
