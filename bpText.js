// bpText.js — text that Brightpearl's screens can show.
//
// Brightpearl STORES notes correctly (read back through the API, "—" is "—"), but its
// own order screen renders them as Latin-1, so every non-ASCII character comes out as
// debris: "—" shows as "â€”", "£" as "Â£" (Dec, 29 Sep: "RETURN REQUESTED ONLINE â WR…").
// Escaping the JSON or declaring UTF-8 makes no difference — proven on the test
// account — so notes are written in plain ASCII.
const MAP = {
  "—": "-", "–": "-", "‒": "-", "−": "-", "×": "x", "•": "*", "…": "...",
  "‘": "'", "’": "'", "‚": "'", "“": '"', "”": '"', "„": '"', "£": "GBP ",
  "€": "EUR ", " ": " ", "→": "->", "←": "<-", "✓": "", "✔": "",
};
export function bpSafeText(text) {
  return String(text == null ? "" : text)
    .replace(/[—–‒−×•…‘’‚“”„£€ →←✓✔]/g, (c) => MAP[c])
    .replace(/GBP (\d)/g, "GBP$1").replace(/GBP\s+/g, "GBP ").replace(/GBP(\d)/g, "GBP $1")
    .normalize("NFKD").replace(/[̀-ͯ]/g, "")      // é -> e
    .replace(/[^\x09\x0a\x0d\x20-\x7e]/g, "");               // anything else Brightpearl would garble
}
