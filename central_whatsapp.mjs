import express from "express";
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
  jidNormalizedUser
} from "@whiskeysockets/baileys";
import Pino from "pino";
import fs from "fs";

const PORT = Number(process.env.WHATSAPP_PORT || 3000);
const AUTH_DIR = process.env.BAILEYS_AUTH_DIR || "./auth_info_baileys";
const SEND_CONFIRM_TIMEOUT_MS = Number(process.env.SEND_CONFIRM_TIMEOUT_MS || 45000);

const app = express();
app.use(express.json({ limit: "2mb" }));

let sock = null;
let connected = false;
let needsQr = false;
let sessionInvalid = false;
let lastQr = null;
let lastConnectionUpdate = null;
let reconnectAttempt = 0;

let lastSendStatus = null;
let lastAckStatus = null;
let lastSendJid = null;
let lastSendMessageId = null;
let lastSendVisibleInStore = false;
let lastSendError = null;

const recentMessages = new Map();
const waiters = new Map();

function onlyDigits(v) {
  return String(v || "").replace(/\D+/g, "");
}

function normalizePhone(v) {
  let d = onlyDigits(v);
  if (!d) return "";
  if (!d.startsWith("55")) d = "55" + d;
  return d;
}

function jidForPhone(phone) {
  return `${normalizePhone(phone)}@s.whatsapp.net`;
}

function nowIso() {
  return new Date().toISOString();
}

function msgText(m) {
  const msg = m?.message || {};
  return (
    msg.conversation ||
    msg.extendedTextMessage?.text ||
    msg.imageMessage?.caption ||
    msg.videoMessage?.caption ||
    ""
  );
}

function rememberMessage(m, source = "event") {
  const jid = jidNormalizedUser(m?.key?.remoteJid || "");
  if (!jid) return;

  const row = {
    id: m?.key?.id || "",
    fromMe: !!m?.key?.fromMe,
    timestamp: Number(m?.messageTimestamp || Math.floor(Date.now() / 1000)),
    text: msgText(m).slice(0, 500),
    status: m?.status ?? m?.update?.status ?? null,
    source,
  };

  if (!recentMessages.has(jid)) recentMessages.set(jid, []);
  const arr = recentMessages.get(jid);
  arr.push(row);
  while (arr.length > 50) arr.shift();

  const id = row.id;
  if (id && waiters.has(id)) {
    const w = waiters.get(id);
    if (row.fromMe) {
      w.visible = true;
      w.visibleRow = row;
      const st = Number(row.status ?? 0);
      if (st >= 2) {
        w.ack = true;
        w.ackStatus = st;
        w.resolve({ ok: true, reason: "ack_status_ge_2_from_message", waiter: w });
      }
    }
  }
}

function markAck(update) {
  const id = update?.key?.id;
  if (!id) return;

  const status = Number(update?.update?.status ?? update?.status ?? 0);
  if (waiters.has(id)) {
    const w = waiters.get(id);
    w.ackStatus = status;
    if (status >= 2) {
      w.ack = true;
      w.resolve({ ok: true, reason: "ack_status_ge_2", waiter: w });
    }
  }
}

function waitForConfirm(messageId) {
  return new Promise((resolve) => {
    const waiter = {
      visible: false,
      ack: false,
      ackStatus: null,
      visibleRow: null,
      resolve,
    };

    waiters.set(messageId, waiter);

    setTimeout(() => {
      if (waiters.has(messageId)) {
        waiters.delete(messageId);
        resolve({ ok: false, reason: "timeout", waiter });
      }
    }, SEND_CONFIRM_TIMEOUT_MS);
  });
}

async function startSock() {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: true,
    logger: Pino({ level: process.env.BAILEYS_LOG_LEVEL || "silent" }),
    browser: ["Mand Digital SDR", "Chrome", "1.0.0"],
    syncFullHistory: false,
    markOnlineOnConnect: false,
    generateHighQualityLinkPreview: false,
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", (u) => {
    lastConnectionUpdate = { ...u, qr: u.qr ? "[QR]" : undefined, at: nowIso() };

    if (u.qr) {
      lastQr = u.qr;
      needsQr = true;
      connected = false;
      console.log("[QR_AVAILABLE]");
    }

    if (u.connection === "open") {
      connected = true;
      needsQr = false;
      sessionInvalid = false;
      reconnectAttempt = 0;
      console.log("[WA_CONNECTED]", sock?.user?.id || "");
    }

    if (u.connection === "close") {
      connected = false;
      const code = u?.lastDisconnect?.error?.output?.statusCode;
      console.log("[WA_CLOSED]", code);

      if (code === DisconnectReason.loggedOut || code === 401) {
        sessionInvalid = true;
        needsQr = true;
        console.log("[SESSION_INVALID]");
        return;
      }

      reconnectAttempt += 1;
      setTimeout(startSock, Math.min(30000, 2000 * reconnectAttempt));
    }
  });

  sock.ev.on("messages.upsert", ({ messages }) => {
    for (const m of messages || []) rememberMessage(m, "messages.upsert");
  });

  sock.ev.on("messages.update", (updates) => {
    for (const u of updates || []) markAck(u);
  });

  sock.ev.on("message-receipt.update", (updates) => {
    for (const u of updates || []) markAck(u);
  });
}


app.get("/qr", (req, res) => {
  res.json({
    qr_available: !!lastQr,
    qr: lastQr,
    needs_qr: needsQr,
    session_invalid: sessionInvalid,
    connected,
  });
});

app.get("/status", (req, res) => {
  res.json({
    status: connected ? "online" : "offline",
    connected,
    session_invalid: sessionInvalid,
    needs_qr: needsQr,
    qr_available: !!lastQr,
    mode: connected ? "connected" : "disconnected",
    reconnect_attempt: reconnectAttempt,
    stable: connected && !sessionInvalid && !needsQr,
    auth_folder: AUTH_DIR,
    user_jid: sock?.user?.id || null,
    user_phone: onlyDigits(sock?.user?.id || "").slice(0, 15) || null,
    last_connection_update: lastConnectionUpdate,
    last_send_status: lastSendStatus,
    last_ack_status: lastAckStatus,
    last_send_jid: lastSendJid,
    last_send_visible_in_store: lastSendVisibleInStore,
    last_send_message_id: lastSendMessageId,
    last_send_error: lastSendError,
  });
});

app.get("/debug/chat/:phone", (req, res) => {
  const jid = jidForPhone(req.params.phone);
  res.json({
    phone: normalizePhone(req.params.phone),
    jid,
    messages: recentMessages.get(jid) || [],
  });
});

app.post("/send", async (req, res) => {
  try {
    if (!sock || !connected) {
      lastSendStatus = "offline";
      return res.status(503).json({ status: "offline", error: "whatsapp_not_connected" });
    }

    const phone = normalizePhone(req.body.number || req.body.phone || req.body.to);
    const text = String(req.body.text || req.body.message || "").trim();

    if (!phone || !text) {
      return res.status(400).json({ status: "invalid_payload", error: "number/phone and text/message required" });
    }

    let jid = jidForPhone(phone);
    lastSendJid = jid;
    lastSendStatus = "sending";
    lastSendVisibleInStore = false;
    lastAckStatus = null;
    lastSendError = null;

    const waCheck = await sock.onWhatsApp(jid).catch(() => null);
    if (!Array.isArray(waCheck) || !waCheck[0] || waCheck[0].exists === false) {
      lastSendStatus = "not_on_whatsapp";
      return res.status(200).json({ status: "not_on_whatsapp", input_phone: phone, jid });
    }

    const resolvedJid = jidNormalizedUser(waCheck[0].jid || jid);
    if (resolvedJid && resolvedJid !== jid) {
      console.log("[JID_RESOLVED]", phone, jid, "->", resolvedJid);
      jid = resolvedJid;
      lastSendJid = jid;
    }

    console.log("[ENVIO_ATTEMPT]", jid, text.slice(0, 80));

    const sent = await sock.sendMessage(jid, { text });
    const messageId = sent?.key?.id || "";
    lastSendMessageId = messageId;

    if (!messageId || jidNormalizedUser(sent?.key?.remoteJid || "") !== jid) {
      lastSendStatus = "send_key_invalid";
      lastSendError = { sent_key: sent?.key || null };
      return res.status(200).json({
        status: "send_key_invalid",
        message_id: messageId,
        jid,
        visible_in_store: false,
      });
    }

    const confirm = await waitForConfirm(messageId);
    const w = confirm.waiter || {};
    lastSendVisibleInStore = !!w.visible;
    lastAckStatus = w.ackStatus ?? null;

    if (!confirm.ok || !w.ack || Number(w.ackStatus ?? 0) < 2) {
      lastSendStatus = "pending_unconfirmed";
      console.log("[ENVIO_PENDING_UNCONFIRMED]", jid, messageId, "visible=", !!w.visible, "ack_status=", w.ackStatus ?? null);
      return res.status(200).json({
        status: "pending_unconfirmed",
        message_id: messageId,
        jid,
        visible_in_store: !!w.visible,
        ack_status: w.ackStatus ?? null,
        reason: confirm.reason,
      });
    }

    lastSendStatus = "sent";
    console.log("[ENVIO_CONFIRMED_REAL]", jid, messageId, "visible=", !!w.visible, "ack_status=", w.ackStatus);

    return res.json({
      status: "sent",
      message_id: messageId,
      jid,
      visible_in_store: !!w.visible,
      ack_status: w.ackStatus,
    });
  } catch (e) {
    lastSendStatus = "error";
    lastSendError = String(e?.stack || e?.message || e);
    console.error("[ENVIO_ERROR]", lastSendError);
    return res.status(500).json({ status: "error", error: String(e?.message || e) });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log("[WA_GATEWAY_SAFE_LISTEN]", PORT);
  startSock().catch((e) => console.error("[WA_START_ERROR]", e));
});
