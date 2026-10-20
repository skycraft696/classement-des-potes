const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const page = fs.readFileSync(path.join(__dirname, "public", "index.html"));
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(page);
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const clients = new Map(); // id -> { ws, presence, timer }

function broadcast() {
  for (const [id, c] of clients) {
    if (!c.ws || c.ws.readyState !== 1) continue;
    const code = c.presence.code;
    const peers = [];
    for (const [pid, x] of clients) {
      if (pid === id || (code && x.presence.code === code)) peers.push({ peer: pid, presence: x.presence });
    }
    c.ws.send(JSON.stringify({ peers }));
  }
}

wss.on("connection", (ws, req) => {
  const id = (new URL(req.url, "http://x").searchParams.get("id") || "").replace(/[^a-z0-9]/gi, "").slice(0, 16);
  if (!id) return ws.close();
  const old = clients.get(id);
  if (old) clearTimeout(old.timer);
  const c = { ws, presence: old ? old.presence : {}, timer: null };
  clients.set(id, c);
  ws.on("message", data => {
    try {
      const m = JSON.parse(data);
      if (m.p && typeof m.p === "object" && JSON.stringify(m.p).length < 8000) { c.presence = m.p; broadcast(); }
    } catch (e) {}
  });
  ws.on("close", () => {
    if (clients.get(id) !== c) return;
    c.timer = setTimeout(() => { clients.delete(id); broadcast(); }, 20000); // 20 s de grâce pour une reconnexion
  });
  broadcast();
});

server.listen(process.env.PORT || 3000, () => console.log("Serveur prêt"));
