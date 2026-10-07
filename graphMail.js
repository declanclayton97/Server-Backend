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

import fs from "fs";
import { AsyncLocalStorage } from "async_hooks";
import path from "path";
import { fileURLToPath } from "url";

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

// Which mailbox the calls in this request act on: the shared sales@ by default, or a person's
// OWN mailbox when the Sales Hub route has resolved one (and only that person's - see
// salesHubRoutes). AsyncLocalStorage carries it through every await of the request.
export const mailboxContext = new AsyncLocalStorage();
export const currentMailbox = () => (mailboxContext.getStore() && mailboxContext.getStore().box) || salesMailbox();
const mb = () => `/users/${encodeURIComponent(currentMailbox())}`;

// Unread count of a mailbox's inbox (for the badges in the folder pane).
export async function inboxUnread(address) {
  const j = await graph("GET", `/users/${encodeURIComponent(address)}/mailFolders/inbox?$select=unreadItemCount,totalItemCount`);
  return { unread: Number(j && j.unreadItemCount) || 0, total: Number(j && j.totalItemCount) || 0 };
}

const addr = (r) => ({ name: (r && r.emailAddress && r.emailAddress.name) || "", address: (r && r.emailAddress && r.emailAddress.address) || "" });
// "a@x.com; b@y.com" or an array -> clean addresses.
export function splitAddresses(v) {
  const list = Array.isArray(v) ? v : String(v || "").split(/[;,]/);
  return list.map((s) => String(s || "").trim()).map((s) => (/<([^>]+)>/.exec(s) || [null, s])[1].trim()).filter(Boolean);
}
const recipients = (list) => splitAddresses(list).map((address) => ({ emailAddress: { address } }));

// The folders the hub shows, by Outlook's well-known names.
export const FOLDERS = { inbox: "inbox", sent: "sentitems", drafts: "drafts", deleted: "deleteditems", junk: "junkemail", archive: "archive" };

const LIST_SELECT = "id,subject,from,toRecipients,receivedDateTime,sentDateTime,lastModifiedDateTime,isRead,isDraft,bodyPreview,conversationId,hasAttachments,parentFolderId";
const summary = (m, folder) => ({
  id: m.id,
  subject: m.subject || "",
  fromName: addr(m.from).name,
  fromAddress: addr(m.from).address,
  to: (m.toRecipients || []).map(addr),
  // Sent Items by when it went, Drafts by when it was last touched, the rest by arrival.
  receivedAt: (folder === "drafts" ? m.lastModifiedDateTime : folder === "sent" ? m.sentDateTime : m.receivedDateTime) || m.receivedDateTime || m.lastModifiedDateTime,
  isRead: !!m.isRead,
  isDraft: !!m.isDraft,
  preview: m.bodyPreview || "",
  hasAttachments: !!m.hasAttachments,
  conversationId: m.conversationId || null,
});

// A page of a folder, newest first, plus `next` (Graph's nextLink) for the page after it.
// `search` searches the WHOLE folder on the server (subject, body, people), Outlook-style —
// not just the emails already loaded. Graph will not sort a search, so results come back
// newest-first by its own ranking. `next` is only accepted if it points back at this mailbox.
export async function listInbox({ top = 50, unreadOnly = false, folder = "inbox", search = "", next = "" } = {}) {
  let j;
  if (next) {
    // Graph writes the mailbox in its nextLink unencoded (sales@…); accept either spelling.
    const bases = [`${GRAPH}${mb()}/`, `${GRAPH}/users/${currentMailbox()}/`].map((s) => s.toLowerCase());
    if (!bases.some((b) => String(next).toLowerCase().startsWith(b))) throw new Error("Bad page link");
    j = await graph("GET", String(next).slice(GRAPH.length));
  } else {
    const q = new URLSearchParams({ $top: String(Math.min(Number(top) || 50, 100)), $select: LIST_SELECT });
    const term = String(search || "").replace(/["\\]/g, " ").trim().slice(0, 100);
    if (term) q.set("$search", `"${term}"`);
    else {
      q.set("$orderby", `${folder === "drafts" ? "lastModifiedDateTime" : folder === "sent" ? "sentDateTime" : "receivedDateTime"} desc`);
      if (unreadOnly) q.set("$filter", "isRead eq false");
    }
    j = await graph("GET", `${mb()}/mailFolders/${FOLDERS[folder] || "inbox"}/messages?${q}`);
  }
  return { messages: (j.value || []).map((m) => summary(m, folder)), next: j["@odata.nextLink"] || null };
}

// Every email in one conversation, whatever folder it is in (the customer's in the Inbox,
// our replies in Sent Items), oldest first — Outlook's conversation view.
export async function listThread(conversationId) {
  const q = new URLSearchParams({ $filter: `conversationId eq '${String(conversationId).replace(/'/g, "''")}'`, $select: LIST_SELECT, $top: "50" });
  const j = await graph("GET", `${mb()}/messages?${q}`);
  return (j.value || [])
    .map((m) => ({ ...summary(m), receivedAt: m.isDraft ? m.lastModifiedDateTime : (m.receivedDateTime || m.sentDateTime) }))
    .sort((a, b) => new Date(a.receivedAt) - new Date(b.receivedAt));
}

export async function getMessage(id) {
  const sel = "$select=id,subject,from,toRecipients,ccRecipients,bccRecipients,receivedDateTime,sentDateTime,lastModifiedDateTime,isDraft,body,conversationId,parentFolderId";
  // Text for the order lookup, HTML for showing it the way Outlook does.
  const [m, h] = await Promise.all([
    graph("GET", `${mb()}/messages/${encodeURIComponent(id)}?${sel}`),
    graph("GET", `${mb()}/messages/${encodeURIComponent(id)}?$select=body`, undefined, { html: true }),
  ]);
  return {
    id: m.id,
    subject: m.subject || "",
    fromName: addr(m.from).name,
    fromAddress: addr(m.from).address,
    to: (m.toRecipients || []).map(addr),
    cc: (m.ccRecipients || []).map(addr),
    bcc: (m.bccRecipients || []).map(addr),
    receivedAt: m.receivedDateTime || (m.isDraft ? m.lastModifiedDateTime : null),
    isDraft: !!m.isDraft,
    conversationId: m.conversationId || null,
    sentAt: m.sentDateTime,
    // Inline images leave "[cid:image001.png@01DB…]" markers in the text body.
    text: ((m.body && m.body.content) || "").replace(/\[cid:[^\]]+\]/g, "").replace(/\n{3,}/g, "\n\n"),
    // Shown in a sandboxed frame (no scripts) with cid: images swapped for the real ones.
    html: (h.body && h.body.contentType === "html" && h.body.content) || "",
    attachments: await listAttachments(id).catch(() => []),
  };
}

// File attachments only (an attached EMAIL or calendar item has no bytes to show).
// contentBytes is left out of the list — a few photos would make it megabytes. The
// contentId is what the HTML body points at (src="cid:…") for an inline image.
export async function listAttachments(id) {
  const base = `${mb()}/messages/${encodeURIComponent(id)}/attachments?$select=id,name,contentType,size,isInline`;
  let j;
  try { j = await graph("GET", `${base},microsoft.graph.fileAttachment/contentId`); }
  catch { j = await graph("GET", base); }
  return (j.value || [])
    .filter((a) => !a["@odata.type"] || a["@odata.type"] === "#microsoft.graph.fileAttachment")
    .map((a) => ({ id: a.id, name: a.name || "attachment", contentType: a.contentType || "", size: a.size || 0, isInline: !!a.isInline, contentId: a.contentId || null }));
}

// The raw bytes of one attachment, plus what it says it is.
export async function getAttachment(id, attachmentId) {
  const base = `${GRAPH}${mb()}/messages/${encodeURIComponent(id)}/attachments/${encodeURIComponent(attachmentId)}`;
  const meta = await graph("GET", `${mb()}/messages/${encodeURIComponent(id)}/attachments/${encodeURIComponent(attachmentId)}?$select=name,contentType,size`);
  const r = await fetch(`${base}/$value`, { headers: { Authorization: `Bearer ${await token()}` } });
  if (!r.ok) { const e = new Error(`Graph attachment -> ${r.status}`); e.status = r.status; throw e; }
  return { name: meta.name || "attachment", contentType: meta.contentType || "", buf: Buffer.from(await r.arrayBuffer()) };
}

// Our signature's logo and icons, embedded the way Outlook's own signature does it: as
// inline attachments the body points at with cid:, so the customer sees them straight
// away instead of a "download pictures" bar or a hosted image a mail client blocks.
const ASSET_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "email-assets");
export function embedSignatureImages(html) {
  const inline = [];
  const seen = new Map();
  const out = String(html || "").replace(/(src=["'])https?:\/\/[^"']*\/email-assets\/(image\d{3}\.png)(["'])/gi, (all, a, file, b) => {
    let cid = seen.get(file);
    if (!cid) {
      let bytes;
      try { bytes = fs.readFileSync(path.join(ASSET_DIR, file)); } catch { return all; }   // unknown file: leave the link
      cid = `${file}@tuffshop`;
      seen.set(file, cid);
      inline.push({ name: file, contentType: "image/png", base64: bytes.toString("base64"), isInline: true, contentId: cid });
    }
    return `${a}cid:${cid}${b}`;
  });
  return { html: out, inline };
}

// Every outgoing email, the way Outlook builds it:
//   new      a fresh message
//   reply    createReply      — threaded, the original quoted below our text
//   replyAll createReplyAll   — same, to everyone on it
//   forward  createForward    — the original quoted AND its attachments carried over
// Our text goes ABOVE the quote. to/cc/bcc take an address, a "a; b" list, or an array;
// when given they replace what Graph filled in (the page shows and lets you edit them).
// Our text on top of Outlook's quoted original (inside its <body>, as Outlook lays it out).
export function onTopOf(quoted, body) {
  if (!quoted) return body;
  return /<body[^>]*>/i.test(quoted) ? quoted.replace(/<body[^>]*>/i, (tag) => `${tag}${body}<br>`) : `${body}<br>${quoted}`;
}

// Create the Outlook draft for a compose: a blank message, or createReply / createReplyAll /
// createForward (which fill in the recipients, subject and the quoted original). Returns the
// draft and the quoted original, which our text is laid on top of at every save and the send.
async function createDraft(mode, sourceId, subject) {
  if (mode === "new" || !sourceId) {
    const d = await graph("POST", `${mb()}/messages`, { subject: subject || "", body: { contentType: "HTML", content: "" } });
    return { draft: d, quoted: "" };
  }
  const action = { reply: "createReply", replyAll: "createReplyAll", forward: "createForward" }[mode];
  if (!action) throw new Error(`Unknown email mode "${mode}"`);
  const d = await graph("POST", `${mb()}/messages/${encodeURIComponent(sourceId)}/${action}`, {}, { html: true });
  return { draft: d, quoted: (d.body && d.body.content) || "" };
}

function headerPatch({ subject, to, cc, bcc, replyTo }) {
  const patch = {};
  if (subject) patch.subject = subject;
  if (to !== undefined && splitAddresses(to).length) patch.toRecipients = recipients(to);
  if (cc !== undefined) patch.ccRecipients = recipients(cc);
  if (bcc !== undefined) patch.bccRecipients = recipients(bcc);
  if (replyTo) patch.replyTo = recipients(replyTo);
  return patch;
}

// Save (create or update) the Outlook draft for a compose, as Outlook does while you type.
// The signature images stay as hosted links in a draft; they are embedded at send.
export async function saveDraft({ draftId, quoted = "", mode = "new", sourceId, to, cc, bcc, subject, html }) {
  let id = draftId;
  if (!id) {
    const c = await createDraft(mode, sourceId, subject);
    id = c.draft.id; quoted = c.quoted;
  }
  await graph("PATCH", `${mb()}/messages/${encodeURIComponent(id)}`, {
    body: { contentType: "HTML", content: onTopOf(quoted, String(html || "")) },
    ...headerPatch({ subject, to, cc, bcc }),
  });
  return { draftId: id, quoted };
}

// Discard: only ever a message that is still a draft.
export async function deleteDraft(id) {
  const m = await graph("GET", `${mb()}/messages/${encodeURIComponent(id)}?$select=isDraft`);
  if (!m || !m.isDraft) throw new Error("That is not a draft");
  await graph("DELETE", `${mb()}/messages/${encodeURIComponent(id)}`);
}

// Every outgoing email, the way Outlook builds it:
//   new      a fresh message
//   reply    createReply      — threaded, the original quoted below our text
//   replyAll createReplyAll   — same, to everyone on it
//   forward  createForward    — the original quoted AND its attachments carried over
// Our text goes ABOVE the quote. to/cc/bcc take an address, a "a; b" list, or an array;
// when given they replace what Graph filled in (the page shows and lets you edit them).
// With draftId it sends that saved draft (quoted = the original it was created with).
export async function composeAndSend({ mode = "new", sourceId, draftId, quoted = "", to, cc, bcc, subject, html, replyTo, attachments = [] }) {
  const { html: body, inline } = embedSignatureImages(html);
  let draft;
  if (draftId) draft = { id: draftId };
  else ({ draft, quoted } = await createDraft(mode, sourceId, subject));
  await graph("PATCH", `${mb()}/messages/${encodeURIComponent(draft.id)}`, {
    body: { contentType: "HTML", content: onTopOf(quoted, body) },
    ...headerPatch({ subject, to, cc, bcc, replyTo }),
  });
  // A reply quotes the original, but Graph does not carry its inline images (a forward
  // does) — copy them over so the quoted signature/logos still show, as in Outlook.
  const quotedImages = [];
  if ((mode === "reply" || mode === "replyAll") && sourceId) {
    try {
      for (const a of (await listAttachments(sourceId)).filter((x) => x.isInline && x.contentId).slice(0, 15)) {
        const f = await getAttachment(sourceId, a.id);
        quotedImages.push({ name: f.name, contentType: f.contentType, base64: f.buf.toString("base64"), isInline: true, contentId: a.contentId });
      }
    } catch (e) { console.error("[graph] quoted images not copied:", e.message); }   // the reply still goes
  }
  await addAttachments(draft.id, [...inline, ...quotedImages, ...attachments]);
  await graph("POST", `${mb()}/messages/${encodeURIComponent(draft.id)}/send`);
  return { via: mode === "new" ? "graph" : `graph-${mode}`, mailbox: currentMailbox() };
}

// Kept for the other senders (returns, call report).
export async function replyToMessage(id, { html, to, replyTo, subject, attachments = [] }) {
  const r = await composeAndSend({ mode: "reply", sourceId: id, to, subject, html, replyTo, attachments });
  return { ...r, via: "graph-reply" };
}
export async function sendNew({ to, subject, html, replyTo, attachments = [] }) {
  return composeAndSend({ mode: "new", to, subject, html, replyTo, attachments });
}

// Files onto a draft. Graph takes up to 3 MB in one call; anything bigger goes
// through an upload session in chunks (multiples of 320 KiB, as Graph requires).
const SIMPLE_MAX = 3 * 1024 * 1024;
const CHUNK = 320 * 1024 * 10;   // 3.125 MiB
export async function addAttachments(draftId, files = []) {
  for (const f of files) {
    const bytes = Buffer.from(String(f.base64 || ""), "base64");
    const name = String(f.name || "attachment").slice(0, 200);
    const contentType = String(f.contentType || "application/octet-stream");
    if (bytes.length <= SIMPLE_MAX) {
      await graph("POST", `${mb()}/messages/${encodeURIComponent(draftId)}/attachments`, {
        "@odata.type": "#microsoft.graph.fileAttachment", name, contentType, contentBytes: bytes.toString("base64"),
        ...(f.isInline ? { isInline: true, contentId: String(f.contentId || name) } : {}),
      });
      continue;
    }
    const s = await graph("POST", `${mb()}/messages/${encodeURIComponent(draftId)}/attachments/createUploadSession`, {
      AttachmentItem: { attachmentType: "file", name, size: bytes.length, contentType },
    });
    for (let at = 0; at < bytes.length; at += CHUNK) {
      const part = bytes.subarray(at, Math.min(at + CHUNK, bytes.length));
      // The upload URL carries its own authorisation — sending ours as well is refused.
      const r = await fetch(s.uploadUrl, {
        method: "PUT",
        headers: { "Content-Length": String(part.length), "Content-Range": `bytes ${at}-${at + part.length - 1}/${bytes.length}` },
        body: part,
      });
      if (!r.ok && r.status !== 200 && r.status !== 201 && r.status !== 202) {
        const e = new Error(`Attachment upload failed for ${name}: ${r.status}`); e.status = r.status; throw e;
      }
    }
  }
}

export async function markRead(id) { return setRead(id, true); }

// Outlook's Delete: MOVE to Deleted Items (recoverable), never a hard delete. Also used to
// restore (move back to the inbox). Graph gives the moved message a new id.
export async function moveMessage(id, folder) {
  const dest = FOLDERS[folder];
  if (!dest) throw new Error(`Unknown folder "${folder}"`);
  const m = await graph("POST", `${mb()}/messages/${encodeURIComponent(id)}/move`, { destinationId: dest });
  return { id: m && m.id };
}

export async function setRead(id, isRead) {
  await graph("PATCH", `${mb()}/messages/${encodeURIComponent(id)}`, { isRead: !!isRead });
}

// Generic Graph call for other modules (the picking sheet reads/colours a SharePoint
// workbook with the same app credentials). Same retry and error handling as the mail calls.
export async function graphRequest(method, path, body) { return graph(method, path, body); }
