const targets = [
  { id: "api",  url: "/api" },
  { id: "sql",  url: "/sql/ping" },
  { id: "blob", url: "/blob/ping" },
];

function setStatus(id, state, payload) {
  const dot = document.getElementById(`dot-${id}`);
  const out = document.getElementById(`out-${id}`);
  dot.classList.remove("dot-pending", "dot-ok", "dot-err");
  dot.classList.add(`dot-${state}`);
  out.textContent = typeof payload === "string"
    ? payload
    : JSON.stringify(payload, null, 2);
}

async function checkOne({ id, url }) {
  setStatus(id, "pending", "checking…");
  try {
    const res = await fetch(url, { headers: { "Accept": "application/json" } });
    const body = await res.json().catch(() => ({}));
    if (res.ok) {
      setStatus(id, "ok", body);
    } else {
      setStatus(id, "err", body || `HTTP ${res.status}`);
    }
  } catch (err) {
    setStatus(id, "err", `network error: ${err.message}`);
  }
}

async function refreshAll() {
  document.getElementById("envPill").textContent = "running checks…";
  await Promise.all(targets.map(checkOne));
  document.getElementById("envPill").textContent = "live";
}

document.getElementById("refreshBtn").addEventListener("click", refreshAll);

fetch("/api")
  .then((r) => r.ok ? r.json() : null)
  .then((info) => {
    if (info && info.version) {
      document.getElementById("buildTag").textContent = info.version;
    }
  })
  .catch(() => { /* ignore — surfaced via the API card */ });

refreshAll();
