import http from 'http'
import https from 'https'
import fs from 'fs'
import path from 'path'
import { createRequire } from 'module'
import express from 'express'
import makeWASocket, { DisconnectReason, fetchLatestBaileysVersion, useMultiFileAuthState } from '@whiskeysockets/baileys'
import P from 'pino'

const require = createRequire(import.meta.url)
const qrcodeTerminal = require('qrcode-terminal')
const QRCodeVendor = require('qrcode-terminal/vendor/QRCode')

const app = express()
app.use(express.json())

global.sock = global.sock || null
global.connected = global.connected || false
global.session_invalid = global.session_invalid || false
const WA_STATE = {
    connected: false,
    needs_qr: false,
    qr_available: false,
    reconnect_attempts: 0,
    status: 'offline',
}
let sock = global.sock
let backlogProcessedOnce = false
let backlogInProgress = false
let lastBacklogRunAt = 0
let isConnected = global.connected
let isStarting = false
let startPromise = null
let reconnectTimer = null
let connectTimeoutTimer = null
let currentQR = null
let reconnectAttempt = 0
let connectionMode = 'booting'
let needsQr = false
let outboundQueue = Promise.resolve()
let dailyLimit = 50
let dailyCountDate = ''
let dailySentCount = 0
let authStateRef = null
let saveCredsRef = null
let reauthInProgress = false
let forcedReauthAttempted = false
const backlogChats = new Map()
const backlogMessages = new Map()
const seenInboundKeys = new Map()
const inflightInboundKeys = new Map()
const recentOutboundTexts = new Map()
const watchedOutboundChats = new Map()
const RECONNECT_BACKOFF_MS = [2000, 5000, 10000, 30000, 60000]
const MAX_RECONNECT = 5
const BACKLOG_SWEEP_INTERVAL_MS = 30000
const BACKLOG_MIN_GAP_MS = 30000
const BACKLOG_WATCH_TTL_MS = 24 * 60 * 60 * 1000
const VPS_URL = String(process.env.VPS_URL || 'http://127.0.0.1:8001').replace(/\/+$/, '')
const WA_PORT = Number(process.env.PORT || 3000)
const WA_INSTANCE = String(process.env.WA_INSTANCE || (WA_PORT === 3001 ? 'WA2' : 'WA1')).trim() || 'WA1'
const HISTORY_FILE = path.join('logs', 'whatsapp_message_history.json')
const BACKLOG_STATE_FILE = path.join('logs', `whatsapp_backlog_state.${WA_INSTANCE.toLowerCase()}.json`)
const AUTH_INFO_DIR = path.resolve(String(process.env.WA_AUTH_DIR || path.join(process.cwd(), 'auth_info_baileys')))
const PROCESS_LOCK_FILE = path.join(process.cwd(), `baileys.${WA_INSTANCE.toLowerCase()}.lock`)
let backlogSweepTimer = null
let backlogStateFlushTimer = null
let processLockFd = null
let server = null

function clearReconnectTimer() {
    if (reconnectTimer) {
        clearTimeout(reconnectTimer)
        reconnectTimer = null
    }
}

function clearConnectTimeoutTimer() {
    if (connectTimeoutTimer) {
        clearTimeout(connectTimeoutTimer)
        connectTimeoutTimer = null
    }
}

function scheduleBacklogStateSave() {
    if (backlogStateFlushTimer) {
        clearTimeout(backlogStateFlushTimer)
    }
    backlogStateFlushTimer = setTimeout(() => {
        backlogStateFlushTimer = null
        persistBacklogState()
    }, 250)
}

function persistBacklogState() {
    try {
        fs.mkdirSync(path.dirname(BACKLOG_STATE_FILE), { recursive: true })
        const payload = {
            savedAt: new Date().toISOString(),
            backlogChats: Array.from(backlogChats.entries()).map(([jid, chat]) => [jid, chat]),
            backlogMessages: Array.from(backlogMessages.entries()).map(([jid, messages]) => [
                jid,
                Array.isArray(messages) ? messages.slice(-50) : [],
            ]),
            watchedOutboundChats: Array.from(watchedOutboundChats.entries()).map(([jid, entry]) => [jid, entry]),
        }
        fs.writeFileSync(BACKLOG_STATE_FILE, JSON.stringify(payload))
    } catch (_error) {
    }
}

function loadPersistedBacklogState() {
    try {
        if (!fs.existsSync(BACKLOG_STATE_FILE)) {
            return
        }
        const raw = fs.readFileSync(BACKLOG_STATE_FILE, 'utf-8')
        const parsed = JSON.parse(raw)
        for (const [jid, chat] of Array.isArray(parsed?.backlogChats) ? parsed.backlogChats : []) {
            if (!jid || shouldIgnoreJid(jid)) {
                continue
            }
            backlogChats.set(jid, chat || {})
        }
        for (const [jid, messages] of Array.isArray(parsed?.backlogMessages) ? parsed.backlogMessages : []) {
            if (!jid || shouldIgnoreJid(jid)) {
                continue
            }
            backlogMessages.set(jid, Array.isArray(messages) ? messages.slice(-50) : [])
        }
        for (const [jid, entry] of Array.isArray(parsed?.watchedOutboundChats) ? parsed.watchedOutboundChats : []) {
            if (!jid || shouldIgnoreJid(jid)) {
                continue
            }
            watchedOutboundChats.set(jid, entry || {})
        }
        cleanupWatchedOutboundChats()
    } catch (_error) {
    }
}

function markWhatsAppDisconnected(mode = 'offline') {
    isConnected = false
    global.connected = false
    connectionMode = mode
    WA_STATE.connected = false
    WA_STATE.status = mode === 'booting' ? 'offline' : mode
}

function markAwaitingQr(reason = '') {
    markWhatsAppDisconnected('awaiting_qr')
    global.session_invalid = true
    needsQr = true
    WA_STATE.needs_qr = true
    WA_STATE.qr_available = false
    WA_STATE.status = 'offline'
    clearReconnectTimer()
    clearConnectTimeoutTimer()
    reconnectAttempt = 0
    WA_STATE.reconnect_attempts = 0
    console.log('[WA_AGUARDANDO_QR]', reason || 'session_invalid')
}

function isPidRunning(pid) {
    const parsedPid = Number.parseInt(String(pid || '').trim(), 10)
    if (!Number.isInteger(parsedPid) || parsedPid <= 0) {
        return false
    }
    try {
        process.kill(parsedPid, 0)
        return true
    } catch (_error) {
        return false
    }
}

function acquireProcessLock() {
    try {
        processLockFd = fs.openSync(PROCESS_LOCK_FILE, 'wx')
        fs.writeFileSync(processLockFd, String(process.pid))
        return true
    } catch (error) {
        if (error?.code === 'EEXIST') {
            try {
                const existingPid = fs.readFileSync(PROCESS_LOCK_FILE, 'utf-8')
                if (!isPidRunning(existingPid)) {
                    console.log('[BAILEYS_LOCK_STALE_REMOVIDO]', String(existingPid || '').trim())
                    fs.unlinkSync(PROCESS_LOCK_FILE)
                    processLockFd = fs.openSync(PROCESS_LOCK_FILE, 'wx')
                    fs.writeFileSync(processLockFd, String(process.pid))
                    return true
                }
            } catch (_lockError) {
            }
            console.log('[BAILEYS_JA_ATIVO_LOCK]')
            return false
        }
        throw error
    }
}

function releaseProcessLock() {
    if (processLockFd !== null) {
        try {
            fs.closeSync(processLockFd)
        } catch (_error) {
        }
        processLockFd = null
    }
    try {
        if (fs.existsSync(PROCESS_LOCK_FILE)) {
            fs.unlinkSync(PROCESS_LOCK_FILE)
        }
    } catch (_error) {
    }
}

function startHttpServer() {
    if (server) {
        return server
    }
    server = app.listen(WA_PORT, () => {
        console.log('[WA_SERVER_READY]')
        console.log(`[WA_INSTANCE] ${WA_INSTANCE}`)
        console.log(`API BAILEYS ON: http://127.0.0.1:${WA_PORT}`)
    })

    server.on('error', (error) => {
        if (error?.code === 'EADDRINUSE') {
            console.log('[BAILEYS_PORTA_EM_USO]')
            return
        }
        console.log('[WA_FATAL]', error)
    })

    return server
}

function initBaileys() {
    return start().catch((error) => {
        console.log('[WA_FATAL]', error)
        WA_STATE.status = 'offline'
        return null
    })
}

function currentDateKey() {
    return new Date().toISOString().slice(0, 10)
}

function resetDailyCountersIfNeeded() {
    const today = currentDateKey()
    if (dailyCountDate !== today) {
        dailyCountDate = today
        dailySentCount = 0
    }
}

function randomBetween(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min
}

function entryDelayMs() {
    return randomBetween(500, 1500)
}

function stealthQueueDelayMs() {
    return 0
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

function humanDelayForText(text) {
    const content = String(text || '').trim()
    const length = content.length
    const base = 7000
    const perChar = Math.min(85 * length, 12000)
    const jitter = Math.floor(Math.random() * 3000)
    return Math.max(7000, Math.min(22000, base + perChar + jitter))
}

function postJson(hostname, port, endpoint, data, timeout = 40000) {
    return new Promise((resolve) => {
        const payload = JSON.stringify(data)
        const options = {
            hostname,
            port,
            path: endpoint,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload),
            },
            timeout,
        }

        const req = http.request(options, (res) => {
            const chunks = []
            res.on('data', (chunk) => chunks.push(chunk))
            res.on('end', () => {
                let parsed = null
                try {
                    parsed = JSON.parse(Buffer.concat(chunks).toString('utf-8') || '{}')
                } catch (_parseError) {
                }
                resolve({
                    statusCode: Number(res.statusCode || 0),
                    body: parsed,
                })
            })
        })

        req.on('timeout', () => {
            req.destroy(new Error('timeout'))
        })

        req.on('error', (e) => {
            console.log('[ERRO_HTTP]', e.message)
            resolve(false)
        })

        req.write(payload)
        req.end()
    })
}

function postJsonToUrl(baseUrl, endpoint, data, timeout = 40000) {
    return new Promise((resolve) => {
        const payload = JSON.stringify(data || {})
        const target = new URL(String(endpoint || ''), `${String(baseUrl || '').replace(/\/+$/, '')}/`)
        const client = target.protocol === 'https:' ? https : http
        const options = {
            hostname: target.hostname,
            port: Number(target.port || (target.protocol === 'https:' ? 443 : 80)),
            path: `${target.pathname}${target.search}`,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload),
            },
            timeout,
        }

        const req = client.request(options, (res) => {
            const chunks = []
            res.on('data', (chunk) => chunks.push(chunk))
            res.on('end', () => {
                let parsed = null
                try {
                    parsed = JSON.parse(Buffer.concat(chunks).toString('utf-8') || '{}')
                } catch (_parseError) {
                }
                resolve({
                    statusCode: Number(res.statusCode || 0),
                    body: parsed,
                })
            })
        })

        req.on('timeout', () => {
            req.destroy(new Error('timeout'))
        })

        req.on('error', (e) => {
            console.log('[ERRO_HTTP]', e.message)
            resolve(false)
        })

        req.write(payload)
        req.end()
    })
}

async function postToPython(data, endpoint = '/inbound') {
    const result = await postJson('127.0.0.1', 5000, endpoint, data, 40000)
    console.log('[PYTHON_STATUS]', result.statusCode)
    const confirmed = result?.body?.confirmed !== false
    const ok = result?.body?.ok === true
    if (result.statusCode === 200 && ok && confirmed) {
        return true
    }
    console.log('[ERRO_ENVIO_PYTHON]', endpoint, `status=${result.statusCode || 0}`, `ok=${ok ? 1 : 0}`, `confirmed=${confirmed ? 1 : 0}`)
    return false
}

async function postToVps(data) {
    const phone = data.phone || data.remoteJid || ''
    const msgId = data.messageId || data.id || ''
    for (const delayMs of [1000, 2000, 3000]) {
        console.log('[INBOUND_VPS_ENVIADO]', phone, `msg_id=${msgId}`)
        const result = await postJsonToUrl(VPS_URL, '/inbound', data, 40000)
        const ok = result?.body?.ok === true
        if (result?.statusCode === 200 && ok) {
            console.log('[INBOUND_VPS_CONFIRMADO]', phone, `msg_id=${msgId}`)
            return true
        }
        console.log('[INBOUND_VPS_FALHOU]', `status=${result?.statusCode || 0}`, phone, `msg_id=${msgId}`)
        await sleep(delayMs)
    }
    return false
}

async function postToPythonWithDelay(data, source) {
    const delayMs = entryDelayMs()
    console.log('[DELAY_ENTRADA]', source, `delay=${delayMs}ms`)
    await sleep(delayMs)
    const ok = await postToVps(data)
    return ok
}

function syncOutboundToPython(data) {
    postToPython(data, '/history/outbound')
}

function closeExistingSocket() {
    if (backlogSweepTimer) {
        clearInterval(backlogSweepTimer)
        backlogSweepTimer = null
    }
    if (!global.sock) {
        return
    }
    try {
        global.sock.ws.close()
    } catch (_error) {
    }
    try {
        global.sock.end()
    } catch (_error) {
    }
    try {
        global.sock.ev.removeAllListeners()
    } catch (_error) {
    }
}

function ensureBacklogSweep(currentSock = sock) {
    if (backlogSweepTimer) {
        clearInterval(backlogSweepTimer)
    }
    backlogSweepTimer = setInterval(() => {
        if (!global.connected || global.session_invalid || !currentSock || currentSock !== sock) {
            return
        }
        processBacklog(currentSock, { force: true, reason: 'interval' })
    }, BACKLOG_SWEEP_INTERVAL_MS)
}

async function forceReauthMode() {
    if (reauthInProgress) {
        return
    }
    reauthInProgress = true
    try {
        console.log('[WA_FORCE_REAUTH]')
        currentQR = null
        needsQr = true
        WA_STATE.needs_qr = true
        WA_STATE.qr_available = false
        WA_STATE.connected = false
        WA_STATE.reconnect_attempts = 0
        WA_STATE.status = 'offline'
        reconnectAttempt = 0
        authStateRef = null
        saveCredsRef = null
        closeExistingSocket()
        forcedReauthAttempted = false
        console.log('[WA_AUTH_PRESERVADA]')
    } catch (error) {
        console.log('[WA_FATAL]', error)
    } finally {
        reauthInProgress = false
    }
}

function queueStealthSend(task) {
    const previousQueue = outboundQueue.catch(() => null)
    const nextQueue = previousQueue
        .then(async () => {
            const delayMs = stealthQueueDelayMs()
            if (delayMs > 0) {
                console.log('[STEALTH_DELAY]', `delay=${delayMs}ms`)
                await sleep(delayMs)
            }
            return task()
        })
        .catch((error) => {
            console.log('[ERRO_FILA_ENVIO]', error?.message || String(error))
            throw error
        })
    outboundQueue = nextQueue.catch(() => null)
    return nextQueue
}

function renderQrSvg(qrText) {
    const qrCode = new QRCodeVendor(-1, qrcodeTerminal.error)
    qrCode.addData(String(qrText || ''))
    qrCode.make()
    const moduleCount = qrCode.getModuleCount()
    const quietZone = 4
    const size = moduleCount + quietZone * 2
    const parts = []
    for (let row = 0; row < moduleCount; row += 1) {
        for (let col = 0; col < moduleCount; col += 1) {
            if (qrCode.modules?.[row]?.[col]) {
                parts.push(`<rect x="${col + quietZone}" y="${row + quietZone}" width="1" height="1"/>`)
            }
        }
    }
    return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${size} ${size}" shape-rendering="crispEdges">
<rect width="${size}" height="${size}" fill="#ffffff"/>
<g fill="#000000">
${parts.join('')}
</g>
</svg>`
}

function renderQrAscii(qrText) {
    return new Promise((resolve) => {
        qrcodeTerminal.generate(String(qrText || ''), { small: true }, (output) => {
            resolve(output)
        })
    })
}

function buildInboundKey(msg, fallbackJid = '') {
    const remoteJid = msg?.key?.remoteJid || fallbackJid || ''
    const msgId = msg?.key?.id || ''
    if (msgId) {
        return `${remoteJid}:${msgId}`
    }
    const text = extractText(msg)
    const timestamp = Number(msg?.messageTimestamp || 0)
    return `${remoteJid}:${timestamp}:${text}`
}

function shouldBlockDuplicateOutbound(jid, text, withinMs = 2 * 60 * 1000) {
    const key = `${String(jid || '').trim()}::${String(text || '').trim()}`
    if (!key || key.endsWith('::')) {
        return false
    }
    const now = Date.now()
    for (const [entryKey, createdAt] of recentOutboundTexts.entries()) {
        if (now - createdAt > withinMs) {
            recentOutboundTexts.delete(entryKey)
        }
    }
    if (recentOutboundTexts.has(key)) {
        return true
    }
    recentOutboundTexts.set(key, now)
    return false
}

function claimInboundKey(msg, fallbackJid = '') {
    const key = buildInboundKey(msg, fallbackJid)
    if (!key) {
        return ''
    }
    const now = Date.now()
    for (const [entryKey, createdAt] of seenInboundKeys.entries()) {
        if (now - createdAt > 24 * 60 * 60 * 1000) {
            seenInboundKeys.delete(entryKey)
        }
    }
    for (const [entryKey, createdAt] of inflightInboundKeys.entries()) {
        if (now - createdAt > 10 * 60 * 1000) {
            inflightInboundKeys.delete(entryKey)
        }
    }
    if (seenInboundKeys.has(key) || inflightInboundKeys.has(key)) {
        return ''
    }
    inflightInboundKeys.set(key, now)
    return key
}

function confirmInboundKey(key) {
    const normalized = String(key || '').trim()
    if (!normalized) {
        return
    }
    inflightInboundKeys.delete(normalized)
    seenInboundKeys.set(normalized, Date.now())
}

function releaseInboundKey(key) {
    const normalized = String(key || '').trim()
    if (!normalized) {
        return
    }
    inflightInboundKeys.delete(normalized)
}

function cleanupWatchedOutboundChats() {
    const now = Date.now()
    let changed = false
    for (const [jid, entry] of watchedOutboundChats.entries()) {
        const lastSentAt = Number(entry?.lastSentAt || 0)
        if (!lastSentAt || now - lastSentAt > BACKLOG_WATCH_TTL_MS) {
            watchedOutboundChats.delete(jid)
            changed = true
        }
    }
    if (changed) {
        scheduleBacklogStateSave()
    }
}

function watchOutboundChat(jid) {
    const chatJid = String(jid || '').trim()
    if (!chatJid || shouldIgnoreJid(chatJid)) {
        return
    }
    cleanupWatchedOutboundChats()
    const previous = watchedOutboundChats.get(chatJid) || {}
    watchedOutboundChats.set(chatJid, {
        ...previous,
        jid: chatJid,
        lastSentAt: Date.now(),
    })
    console.log('[BACKLOG_WATCH_REGISTRADO]', chatJid)
    scheduleBacklogStateSave()
}

function getWatchedOutboundEntry(jid) {
    cleanupWatchedOutboundChats()
    return watchedOutboundChats.get(String(jid || '').trim()) || null
}

function loadHistoryBacklogChats(maxAgeHours = 24) {
    try {
        if (!fs.existsSync(HISTORY_FILE)) {
            return []
        }
        const raw = fs.readFileSync(HISTORY_FILE, 'utf-8')
        const parsed = JSON.parse(raw)
        if (!parsed || typeof parsed !== 'object') {
            return []
        }
        const now = Date.now()
        const cutoffMs = Math.max(1, Number(maxAgeHours || 24)) * 60 * 60 * 1000
        const chats = []
        for (const [phone, items] of Object.entries(parsed)) {
            const normalizedPhone = normalizePhoneDigits(phone)
            if (!normalizedPhone || !Array.isArray(items)) {
                continue
            }
            let lastOutMs = 0
            for (const item of items) {
                if (String(item?.direction || '').trim().toLowerCase() !== 'out') {
                    continue
                }
                const createdAt = Date.parse(String(item?.created_at || ''))
                if (!Number.isFinite(createdAt)) {
                    continue
                }
                lastOutMs = Math.max(lastOutMs, createdAt)
            }
            if (!lastOutMs || now - lastOutMs > cutoffMs) {
                continue
            }
            chats.push({
                id: `55${normalizedPhone}@s.whatsapp.net`,
                jid: `55${normalizedPhone}@s.whatsapp.net`,
                remoteJid: `55${normalizedPhone}@s.whatsapp.net`,
                historySeed: true,
                lastSentAt: lastOutMs,
                unreadCount: 0,
            })
        }
        return chats
    } catch (_error) {
        return []
    }
}

function shouldIgnoreJid(remoteJid) {
    const jid = String(remoteJid || '')
    return (
        !jid ||
        jid.includes('@g.us') ||
        jid.includes('status@broadcast') ||
        jid.endsWith('@broadcast') ||
        jid.endsWith('@newsletter')
    )
}

function extractText(msg) {
    const audio = msg?.message?.audioMessage
    const document = msg?.message?.documentMessage
    const sticker = msg?.message?.stickerMessage
    const contact =
        msg?.message?.contactMessage ||
        msg?.message?.contactsArrayMessage
    const location =
        msg?.message?.locationMessage ||
        msg?.message?.liveLocationMessage
    const contactText = extractContactText(contact)
    return (
        msg?.message?.conversation ||
        msg?.message?.extendedTextMessage?.text ||
        msg?.message?.imageMessage?.caption ||
        msg?.message?.videoMessage?.caption ||
        (audio ? '[AUDIO]' : '') ||
        (document ? '[DOCUMENTO]' : '') ||
        (sticker ? '[STICKER]' : '') ||
        contactText ||
        (location ? '[LOCALIZACAO]' : '') ||
        ''
    )
}

function extractContactText(contact) {
    if (!contact) {
        return ''
    }
    const contacts = Array.isArray(contact?.contacts) ? contact.contacts : [contact]
    const parts = []
    for (const item of contacts) {
        const displayName = String(item?.displayName || item?.name || '').trim()
        const vcard = String(item?.vcard || '').replace(/\u200e/g, '')
        const phones = [...vcard.matchAll(/TEL[^:\n\r]*:([^\n\r]+)/gi)]
            .map((match) => String(match[1] || '').trim())
            .filter(Boolean)
        const emails = [...vcard.matchAll(/EMAIL[^:\n\r]*:([^\n\r]+)/gi)]
            .map((match) => String(match[1] || '').trim())
            .filter(Boolean)
        const names = [...vcard.matchAll(/FN[^:\n\r]*:([^\n\r]+)/gi)]
            .map((match) => String(match[1] || '').trim())
            .filter(Boolean)
        const name = displayName || names[0] || ''
        const fields = [name, ...phones, ...emails].filter(Boolean)
        if (fields.length > 0) {
            parts.push(fields.join(' '))
        }
    }
    return parts.length > 0 ? `[CONTATO] ${parts.join(' | ')}` : '[CONTATO]'
}

function extractMessageType(msg) {
    const content = msg?.message || {}
    if (content.conversation) {
        return 'conversation'
    }
    if (content.extendedTextMessage) {
        return 'extendedTextMessage'
    }
    if (content.imageMessage) {
        return 'imageMessage'
    }
    if (content.videoMessage) {
        return 'videoMessage'
    }
    if (content.audioMessage) {
        return 'audioMessage'
    }
    if (content.documentMessage) {
        return 'documentMessage'
    }
    if (content.stickerMessage) {
        return 'stickerMessage'
    }
    if (content.contactMessage || content.contactsArrayMessage) {
        return 'contactMessage'
    }
    if (content.locationMessage || content.liveLocationMessage) {
        return 'locationMessage'
    }
    const firstKey = Object.keys(content)[0] || ''
    return firstKey || 'unknown'
}

function normalizePhoneDigits(value) {
    const digits = String(value || '').replace(/\D+/g, '')
    if (!digits) {
        return ''
    }
    if (digits.startsWith('55') && digits.length > 11) {
        return digits.slice(2)
    }
    return digits
}

function resolvePhoneFromLid(remoteJid) {
    const jid = String(remoteJid || '').trim()
    if (!jid || !jid.includes('@lid')) {
        return ''
    }
    const userPart = jid.split('@')[0] || ''
    const lidUser = userPart.split(':')[0] || ''
    if (!lidUser) {
        return ''
    }
    const reverseFile = path.join('auth_info_baileys', `lid-mapping-${lidUser}_reverse.json`)
    try {
        if (fs.existsSync(reverseFile)) {
            const raw = fs.readFileSync(reverseFile, 'utf-8')
            const mapped = JSON.parse(raw)
            return normalizePhoneDigits(mapped)
        }
    } catch (_error) {
    }
    return ''
}

function buildInboundPayload(msg, source = 'realtime') {
    const remoteJid = msg?.key?.remoteJid || ''
    const phone = normalizePhoneDigits(
        remoteJid.includes('@lid')
            ? resolvePhoneFromLid(remoteJid)
            : remoteJid.replace('@s.whatsapp.net', ''),
    )
    return {
        id: msg?.key?.id || '',
        phone,
        remoteJid,
        message: extractText(msg),
        source,
    }
}

async function collectChatMessages(chat) {
    const jid = chat?.id || chat?.jid || chat?.remoteJid || ''
    if (!jid || shouldIgnoreJid(jid)) {
        return []
    }
    if (sock && typeof sock.fetchMessagesFromWA === 'function') {
        try {
            const fetched = await sock.fetchMessagesFromWA(jid, 30)
            if (Array.isArray(fetched) && fetched.length > 0) {
                console.log('[BACKLOG_FETCH_OK]', jid, `count=${fetched.length}`)
                return fetched
            }
            console.log('[BACKLOG_FETCH_VAZIO]', jid)
        } catch (_error) {
            console.log('[BACKLOG_FETCH_ERRO]', jid)
        }
    }
    const cached = Array.isArray(backlogMessages.get(jid)) ? backlogMessages.get(jid) : []
    console.log('[BACKLOG_CACHE]', jid, `count=${cached.length}`)
    return cached
}

function isMessageOlderThan24h(msg) {
    const timestamp = Number(msg?.messageTimestamp || 0)
    if (!timestamp) {
        return false
    }
    const millis = timestamp > 9999999999 ? timestamp : timestamp * 1000
    return Date.now() - millis > 24 * 60 * 60 * 1000
}

async function processBacklog(currentSock = sock, options = {}) {
    const { force = false, reason = 'default', recoverAll = false } = options || {}
    if (!currentSock || currentSock !== sock) {
        return
    }
    if (backlogInProgress) {
        return
    }
    if (!force && backlogProcessedOnce) {
        return
    }
    if (Date.now() - lastBacklogRunAt < BACKLOG_MIN_GAP_MS) {
        return
    }

    backlogInProgress = true
    lastBacklogRunAt = Date.now()
    console.log('[BACKLOG_INICIADO]', `reason=${reason}`)

    try {
        const socketChats = typeof currentSock.chats?.all === 'function' ? await currentSock.chats.all() : []
        const cachedChats = Array.from(backlogChats.values())
        const historyChats = loadHistoryBacklogChats(24)
        const sourceChats = cachedChats.length > 0 ? cachedChats : socketChats
        const candidateChats = new Map()
        for (const chat of Array.isArray(sourceChats) ? sourceChats : []) {
            const jid = chat?.id || chat?.jid || chat?.remoteJid || ''
            if (!jid || shouldIgnoreJid(jid)) {
                continue
            }
            candidateChats.set(jid, chat)
        }
        if (recoverAll || candidateChats.size === 0) {
            for (const chat of historyChats) {
                const jid = chat?.id || chat?.jid || chat?.remoteJid || ''
                if (!jid || shouldIgnoreJid(jid)) {
                    continue
                }
                const previous = candidateChats.get(jid) || {}
                candidateChats.set(jid, { ...chat, ...previous })
            }
        }
        for (const [jid, entry] of watchedOutboundChats.entries()) {
            if (!jid || shouldIgnoreJid(jid)) {
                continue
            }
            const previous = candidateChats.get(jid) || {}
            candidateChats.set(jid, {
                ...previous,
                id: previous?.id || jid,
                jid,
                remoteJid: previous?.remoteJid || jid,
                watchedOutbound: true,
                lastSentAt: Number(entry?.lastSentAt || 0),
            })
        }
        const validChats = Array.from(candidateChats.values()).filter((chat) => {
            const jid = chat?.id || chat?.jid || chat?.remoteJid || ''
            const unreadCount = Number(chat?.unreadCount || 0)
            return recoverAll || unreadCount > 0 || Boolean(getWatchedOutboundEntry(jid))
        })
        console.log(
            '[BACKLOG_RESUMO]',
            `socket=${Array.isArray(socketChats) ? socketChats.length : 0}`,
            `cache=${cachedChats.length}`,
            `history=${historyChats.length}`,
            `watch=${watchedOutboundChats.size}`,
            `recover=${recoverAll ? 1 : 0}`,
            `valid=${validChats.length}`,
        )

        for (const chat of validChats) {
            const jid = chat?.id || chat?.jid || chat?.remoteJid || ''
            const unreadCount = Number(chat?.unreadCount || 0)
            const watchEntry = getWatchedOutboundEntry(jid)
            console.log('[BACKLOG_CHAT]', jid, `unread=${unreadCount}`, `watch=${watchEntry ? 1 : 0}`)
            const messages = await collectChatMessages(chat)
            const ordered = Array.isArray(messages) ? [...messages] : []
            const inboundMessages = ordered.filter((msg) => {
                if (!msg || !msg.message || msg.key?.fromMe) {
                    return false
                }
                if ((msg.key?.remoteJid || jid) !== jid) {
                    return false
                }
                if (isMessageOlderThan24h(msg)) {
                    return false
                }
                return true
            })
            let selectedMessages = []
            if (unreadCount > 0) {
                const unreadLimit = Math.max(1, unreadCount)
                selectedMessages = inboundMessages.slice(-unreadLimit)
                console.log('[BACKLOG_UNREAD]', jid, `selected=${selectedMessages.length}`, `limit=${unreadLimit}`)
            } else if (recoverAll) {
                selectedMessages = inboundMessages.slice(-3)
                console.log('[BACKLOG_RECOVER]', jid, `selected=${selectedMessages.length}`)
            } else if (watchEntry) {
                const lastSentSeconds = Math.floor(Number(watchEntry.lastSentAt || 0) / 1000)
                selectedMessages = inboundMessages
                    .filter((msg) => Number(msg?.messageTimestamp || 0) >= Math.max(0, lastSentSeconds - 10))
                    .slice(-3)
                console.log('[BACKLOG_WATCH]', jid, `selected=${selectedMessages.length}`, `since=${lastSentSeconds}`)
            }

            for (const msg of selectedMessages) {
                const inboundKey = claimInboundKey(msg, jid)
                if (!inboundKey) {
                    continue
                }
                const text = extractText(msg) || '[MENSAGEM_SEM_TEXTO]'
                const msgType = extractMessageType(msg)

                const payload = buildInboundPayload(
                    {
                        ...msg,
                        key: {
                            ...(msg.key || {}),
                            remoteJid: msg.key?.remoteJid || jid,
                        },
                    },
                    'backlog',
                )
                if (!payload.message) {
                    payload.message = '[MENSAGEM_SEM_TEXTO]'
                }
                console.log('[MSG_RAW]', jid, '|', msgType, '|', JSON.stringify(msg.message || {}))
                console.log('[BACKLOG_MSG_ID]', jid, msg?.key?.id || '')
                console.log('[BACKLOG_MSG_TYPE]', jid, msgType)
                console.log('[BACKLOG_MSG]', jid, text)
                console.log('[INBOUND_RECEBIDO]', jid, `msg_id=${msg?.key?.id || ''}`)
                console.log('[INBOUND_VPS_ENVIADO]', jid, `msg_id=${msg?.key?.id || ''}`)
                const ok = await postToPythonWithDelay(payload, 'backlog')
                if (!ok) {
                    console.log('[INBOUND_VPS_FALHOU]', jid, `msg_id=${msg?.key?.id || ''}`)
                    console.log('[NAO_CONFIRMADO_PYTHON]', jid, `msg_id=${msg?.key?.id || ''}`)
                    releaseInboundKey(inboundKey)
                    continue
                }
                console.log('[INBOUND_CONFIRMADO]', jid, `msg_id=${msg?.key?.id || ''}`)
                confirmInboundKey(inboundKey)
                try {
                    if (unreadCount > 0 && msg?.key?.id && typeof currentSock.readMessages === 'function') {
                        await currentSock.readMessages([{
                            remoteJid: msg.key.remoteJid || jid,
                            id: msg.key.id,
                            fromMe: false,
                        }])
                        console.log('[MARCADO_COMO_LIDO]', jid, `msg_id=${msg?.key?.id || ''}`)
                    }
                } catch (_readError) {
                }
                if (unreadCount > 0) {
                    const previous = backlogChats.get(jid) || chat || {}
                    const nextUnread = Math.max(0, Number(previous?.unreadCount || unreadCount) - 1)
                    backlogChats.set(jid, { ...previous, unreadCount: nextUnread })
                    scheduleBacklogStateSave()
                }
            }
            if (unreadCount > 0 && selectedMessages.length === 0) {
                console.log('[ALERTA_MSG_PERDIDA]', jid, `unread=${unreadCount}`)
            }
        }
        backlogProcessedOnce = true
        console.log('[BACKLOG_PROCESSADO]')
    } catch (e) {
        console.log('[ERRO_BACKLOG]', e.message)
    } finally {
        backlogInProgress = false
    }
}

async function processIncomingBatch(messages, source = 'realtime') {
    try {
        const batch = Array.isArray(messages) ? messages : []
        for (const msg of batch) {
            if (!msg || !msg.message) {
                continue
            }

            const remoteJid = msg.key?.remoteJid || ''
            if (shouldIgnoreJid(remoteJid)) {
                continue
            }
            const currentMessages = backlogMessages.get(remoteJid) || []
            currentMessages.push(msg)
            backlogMessages.set(remoteJid, currentMessages.slice(-50))
            if (!msg.key?.fromMe) {
                const previousChat = backlogChats.get(remoteJid) || {
                    id: remoteJid,
                    jid: remoteJid,
                    remoteJid,
                    unreadCount: 0,
                }
                const nextUnread = Math.max(1, Number(previousChat?.unreadCount || 0) + 1)
                backlogChats.set(remoteJid, { ...previousChat, unreadCount: nextUnread })
            }
            scheduleBacklogStateSave()

            const text = extractText(msg) || '[MENSAGEM_SEM_TEXTO]'
            const msgType = extractMessageType(msg)
            const payload = buildInboundPayload(msg, source)
            if (!payload.message) {
                payload.message = '[MENSAGEM_SEM_TEXTO]'
            }
            if (msg.key?.fromMe) {
                console.log('[MSG_RAW]', remoteJid, '|', msgType, '|', JSON.stringify(msg.message || {}))
                console.log('[OUTBOUND_MANUAL_DETECTADO]', payload.phone, text)
                syncOutboundToPython(payload)
                continue
            }
            const inboundKey = claimInboundKey(msg, remoteJid)
            if (!inboundKey) {
                continue
            }
            console.log('[MSG_RAW]', remoteJid, '|', msgType, '|', JSON.stringify(msg.message || {}))
            console.log('[MSG_CAPTURADA_INBOUND]', payload.phone, text)
            console.log('[INBOUND_RECEBIDO]', payload.phone, `msg_id=${msg?.key?.id || ''}`)
            console.log('[INBOUND_VPS_ENVIADO]', payload.phone, `msg_id=${msg?.key?.id || ''}`)
            const ok = await postToPythonWithDelay(payload, source)
            if (!ok) {
                console.log('[INBOUND_VPS_FALHOU]', payload.phone, `msg_id=${msg?.key?.id || ''}`)
                console.log('[NAO_CONFIRMADO_PYTHON]', payload.phone, `msg_id=${msg?.key?.id || ''}`)
                releaseInboundKey(inboundKey)
                continue
            }
            confirmInboundKey(inboundKey)
            console.log('[INBOUND_CONFIRMADO]', payload.phone, `msg_id=${msg?.key?.id || ''}`)
            try {
                if (msg?.key?.id && typeof sock?.readMessages === 'function') {
                    await sock.readMessages([{
                        remoteJid: msg.key.remoteJid || remoteJid,
                        id: msg.key.id,
                        fromMe: false,
                    }])
                    console.log('[MARCADO_COMO_LIDO]', payload.phone, `msg_id=${msg?.key?.id || ''}`)
                }
            } catch (_readError) {
            }
            const previousChat = backlogChats.get(remoteJid) || { id: remoteJid, jid: remoteJid, remoteJid, unreadCount: 1 }
            const nextUnread = Math.max(0, Number(previousChat?.unreadCount || 1) - 1)
            backlogChats.set(remoteJid, { ...previousChat, unreadCount: nextUnread })
            scheduleBacklogStateSave()
        }
    } catch (e) {
        console.log('[ERRO_INBOUND]', e.message)
    }
}

async function start() {
    if (startPromise) {
        console.log('[START_BLOQUEADO_DUPLICADO]')
        return startPromise
    }
    if (isStarting) {
        console.log('[START_BLOQUEADO_DUPLICADO]')
        return
    }
    startPromise = (async () => {
        console.log('[WA_STARTING]')
        isStarting = true
        loadPersistedBacklogState()
        markWhatsAppDisconnected('booting')
        WA_STATE.status = 'offline'
        clearConnectTimeoutTimer()
        clearReconnectTimer()
        closeExistingSocket()
        const { state, saveCreds } = await useMultiFileAuthState(AUTH_INFO_DIR)
        authStateRef = state
        saveCredsRef = saveCreds
        const { version } = await fetchLatestBaileysVersion()

        sock = makeWASocket({
            version,
            auth: state,
            logger: P({ level: 'silent' }),
            browser: ['Ubuntu', 'Chrome', '22.04'],
            syncFullHistory: true,
            printQRInTerminal: false,
        })
        global.sock = sock
        connectTimeoutTimer = setTimeout(() => {
            if (!global.connected && !currentQR) {
                console.log('[CONEXAO_TIMEOUT]')
                connectionMode = 'offline'
                WA_STATE.status = 'offline'
                console.log('[WA_OFFLINE]')
            }
        }, 15000)

        sock.ev.on('creds.update', saveCreds)

        sock.ev.on('connection.update', async ({ connection, lastDisconnect }) => {
            const update = { connection, lastDisconnect }
            console.log('[DEBUG_CONNECTION_UPDATE]', JSON.stringify(update, null, 2))
            console.log('[DEBUG_DISCONNECT]', lastDisconnect || null)
            const statusCode = lastDisconnect?.error?.output?.statusCode
            console.log('[STATUS_CODE]', statusCode)
            if (connection === 'connecting') {
                connectionMode = 'connecting'
                WA_STATE.status = 'offline'
            }
            if (connection === 'open') {
                markWhatsAppDisconnected('connected')
                isConnected = true
                global.connected = true
                WA_STATE.connected = true
                global.session_invalid = false
                currentQR = null
                needsQr = false
                WA_STATE.needs_qr = false
                WA_STATE.qr_available = false
                forcedReauthAttempted = false
                isStarting = false
                reconnectAttempt = 0
                WA_STATE.reconnect_attempts = 0
                connectionMode = 'connected'
                WA_STATE.status = 'online'
                backlogProcessedOnce = false
                backlogInProgress = false
                lastBacklogRunAt = 0
                clearConnectTimeoutTimer()
                clearReconnectTimer()
                console.log('[WA_CONNECTED]')
                console.log('[BAILEYS_CONECTADO]')
                console.log('[WA_STABLE]')
                ensureBacklogSweep(sock)
                setTimeout(() => {
                    processBacklog(sock, { force: true, reason: 'open' })
                }, 4000)
                setTimeout(() => {
                    processBacklog(sock, { force: true, reason: 'open-sync', recoverAll: true })
                }, 35000)
            }

            if (connection === 'close') {
                markWhatsAppDisconnected('offline')
                isStarting = false
                clearConnectTimeoutTimer()
                console.log('[WA_DISCONNECTED]')
                if (statusCode === 405 || statusCode === 401 || statusCode === 406 || statusCode === DisconnectReason.loggedOut) {
                    console.log('[SESSAO_INVALIDADA_QR_NECESSARIO]')
                    console.log('[AGUARDANDO_NOVA_AUTENTICACAO]')
                    console.log('Sessao invalida. Necessario escanear QR manualmente para restaurar conexao.')
                    await forceReauthMode()
                    markAwaitingQr(`status_${statusCode}`)
                    return
                }
                if (statusCode === 403) {
                    connectionMode = 'offline'
                    console.log('[WA_BLOQUEADO_SEM_SESSAO]')
                    console.log('[WA_OFFLINE]')
                    return
                }
                const transientCodes = new Set([408, 428, 429, 500, 502, 503, 504, 515])
                const shouldReconnect = transientCodes.has(Number(statusCode)) || typeof statusCode === 'undefined'
                const nextBackoff = RECONNECT_BACKOFF_MS[Math.min(reconnectAttempt, RECONNECT_BACKOFF_MS.length - 1)]
                console.log('[BAILEYS_DESCONECTADO]', shouldReconnect)
                if (!shouldReconnect) {
                    connectionMode = 'offline'
                    console.log('[WA_OFFLINE]')
                    return
                }
                if (reconnectAttempt >= MAX_RECONNECT) {
                    console.log('[RECONNECT_MAXIMO_ATINGIDO]')
                    markAwaitingQr('max_reconnect')
                    return
                }
                if (!reconnectTimer) {
                    connectionMode = 'reconnecting'
                    WA_STATE.status = 'offline'
                    console.log('[WA_RECONNECT]', `tentativa=${reconnectAttempt + 1}`, `delay=${nextBackoff}ms`)
                    reconnectTimer = setTimeout(() => {
                        reconnectTimer = null
                        reconnectAttempt += 1
                        WA_STATE.reconnect_attempts = reconnectAttempt
                        console.log('[RECONNECT_FORCADO]')
                        start().catch((error) => {
                            console.log('[WA_OFFLINE]', error?.message || String(error))
                        })
                    }, nextBackoff)
                }
            }
        })

        sock.ev.on('connection.update', (update) => {
            const { qr } = update || {}
            if (qr) {
                if (currentQR === qr) {
                    return
                }
                currentQR = qr
                global.session_invalid = true
                needsQr = true
                WA_STATE.needs_qr = true
                WA_STATE.qr_available = true
                WA_STATE.status = 'offline'
                connectionMode = 'awaiting_qr'
                clearReconnectTimer()
                console.log('[WA_AGUARDANDO_QR]', 'qr_received')
                console.log('[QR_READY]')
                renderQrAscii(qr)
                    .then((asciiQR) => {
                        console.log('[QR_CODE_ASCII_INICIO]')
                        console.log(asciiQR)
                        console.log('[QR_CODE_ASCII_FIM]')
                    })
                    .catch((error) => {
                        console.log('[ERRO_QR_ASCII]', error?.message || String(error))
                    })
            }
        })

        sock.ev.on('messages.upsert', ({ messages }) => {
            processIncomingBatch(messages, 'realtime')
        })

        const trackChatsAndSweep = (chatItems, reason) => {
            const items = Array.isArray(chatItems) ? chatItems : []
            let hasUnread = false
            for (const chat of items) {
                const jid = chat?.id || chat?.jid || chat?.remoteJid || ''
                if (!jid) {
                    continue
                }
                const previous = backlogChats.get(jid) || {}
                const merged = { ...previous, ...chat }
                backlogChats.set(jid, merged)
                if (Number(merged?.unreadCount || 0) > 0 && !shouldIgnoreJid(jid)) {
                    hasUnread = true
                }
            }
            if (items.length > 0) {
                scheduleBacklogStateSave()
            }
            if (hasUnread) {
                setTimeout(() => {
                    processBacklog(sock, { force: true, reason })
                }, 1500)
            }
        }

        sock.ev.on('messaging.history-set', ({ chats, messages }) => {
            const chatItems = Array.isArray(chats) ? chats : []
            const messageItems = Array.isArray(messages) ? messages : []
            trackChatsAndSweep(chatItems, 'history-set')
            for (const msg of messageItems) {
                const jid = msg?.key?.remoteJid || ''
                if (!jid) {
                    continue
                }
                const current = backlogMessages.get(jid) || []
                current.push(msg)
                backlogMessages.set(jid, current.slice(-50))
            }
            if (chatItems.length > 0 || messageItems.length > 0) {
                scheduleBacklogStateSave()
            }
            setTimeout(() => {
                processBacklog(sock, { force: true, reason: 'history-set' })
            }, 2000)
        })

        sock.ev.on('chats.upsert', (chats) => {
            trackChatsAndSweep(chats, 'chats-upsert')
        })

        sock.ev.on('chats.update', (chats) => {
            trackChatsAndSweep(chats, 'chats-update')
        })
    })()

    try {
        await startPromise
    } finally {
        startPromise = null
    }
}

app.get('/', (_req, res) => {
    res.json({ ok: true })
})

app.get('/status', (_req, res) => {
    res.json({
        status: global.connected === true ? 'online' : 'offline',
        connected: WA_STATE.connected === true,
        session_invalid: global.session_invalid === true,
        needs_qr: WA_STATE.needs_qr === true || Boolean(currentQR),
        mode: connectionMode,
        reconnect_attempt: WA_STATE.reconnect_attempts,
        stable: global.connected === true && global.session_invalid !== true,
        backlog_processed: backlogProcessedOnce,
        qr_available: WA_STATE.qr_available === true,
        daily_limit: dailyLimit,
        sent_today: dailySentCount,
    })
})

app.get('/qr', (_req, res) => {
    if (!currentQR) {
        return res.json({ status: 'no_qr', needs_qr: needsQr === true })
    }

    return res.json({
        status: 'ok',
        qr: currentQR,
    })
})

app.get('/qr-image', (_req, res) => {
    if (!currentQR) {
        return res.status(404).send('QR pendente')
    }

    const svg = renderQrSvg(currentQR)
    return res.type('image/svg+xml').send(svg)
})

app.post('/backlog-recover', async (_req, res) => {
    try {
        if (!sock || !global.connected || global.session_invalid) {
            return res.status(503).json({ status: 'offline' })
        }
        setTimeout(() => {
            processBacklog(sock, { force: true, reason: 'manual-recover', recoverAll: true })
        }, 50)
        return res.json({ status: 'scheduled' })
    } catch (e) {
        return res.status(500).json({ status: 'failed', error: e.message })
    }
})

app.post('/validate', async (req, res) => {
    try {
        if (global.session_invalid) {
            throw new Error('SESSAO_INVALIDA_QR_NECESSARIO')
        }
        if (!sock || !global.connected || global.session_invalid || !sock.user) {
            return res.status(503).json({ status: 'offline', exists: false })
        }

        const { number } = req.body || {}
        if (!/^55\d{10,11}$/.test(String(number || '').trim())) {
            return res.status(400).json({ status: 'invalid_jid', exists: false })
        }

        const check = await sock.onWhatsApp(number)
        const isWhatsApp = Array.isArray(check) && check.some((item) => item && item.exists === true)
        return res.json({ status: isWhatsApp ? 'valid' : 'invalid', exists: isWhatsApp })
    } catch (e) {
        console.log('[VALIDACAO_FALHOU]', req.body?.number || '', e.message)
        return res.status(500).json({ status: 'failed', exists: false, error: e.message })
    }
})

app.post('/send', async (req, res) => {
    try {
        resetDailyCountersIfNeeded()
        console.log('[WARMUP_ATIVO]', `limite=${dailyLimit}`, `enviados=${dailySentCount}`)
        if (global.session_invalid || needsQr || WA_STATE.needs_qr) {
            console.log('[WA_BLOQUEADO_SEM_SESSAO]')
            throw new Error('SESSAO_INVALIDA_QR_NECESSARIO')
        }
        if (!sock || !global.connected || global.session_invalid) {
            console.log('[ENVIO_ABORTADO_SEM_CONEXAO]')
            return res.status(503).json({ status: 'offline' })
        }
        if (!sock.user) {
            console.log('[ERRO_SESSAO_INVALIDA]')
            return res.status(503).json({ status: 'invalid_session' })
        }

        const { number, text } = req.body || {}
        if (!number || !text) {
            return res.status(400).json({ status: 'invalid_payload' })
        }
        if (!/^55\d{10,11}$/.test(String(number || '').trim())) {
            console.log('[ENVIO_FALHOU]', number, 'jid_invalido')
            return res.status(400).json({ status: 'invalid_jid' })
        }

        const check = await sock.onWhatsApp(number)
        const isWhatsApp = Array.isArray(check) && check.some((item) => item && item.exists === true)
        if (!isWhatsApp) {
            console.log('SEM WHATSAPP REAL:', number, JSON.stringify(check || []))
            return res.json({ status: 'invalid' })
        }

        const jid = `${number}@s.whatsapp.net`
        console.log('[ENVIO_TENTANDO]', jid)
        if (!global.connected || global.session_invalid) {
            console.log('[ENVIO_ABORTADO_SEM_CONEXAO]')
            throw new Error('WHATSAPP_DESCONECTADO')
        }
        if (shouldBlockDuplicateOutbound(jid, text)) {
            console.log('[DUPLICIDADE_BLOQUEADA_TEXTO]', jid)
            return res.json({ status: 'duplicate_blocked' })
        }
        if (dailySentCount >= dailyLimit) {
            console.log('[ENVIO_BLOQUEADO_LIMITE]', `limite=${dailyLimit}`, `enviados=${dailySentCount}`)
            return res.status(429).json({ status: 'warmup_limit' })
        }
        const result = await queueStealthSend(async () => {
            if (!global.connected || global.session_invalid || !sock) {
                console.log('[ENVIO_ABORTADO_SEM_CONEXAO]')
                throw new Error('WHATSAPP_DESCONECTADO')
            }
            const delayMs = humanDelayForText(text)
            try {
                await sock.sendPresenceUpdate('composing', jid)
                await sleep(delayMs)
                await sock.sendPresenceUpdate('paused', jid)
            } catch (_presenceError) {
            }
            const sent = await sock.sendMessage(jid, { text })
            if (sent?.key?.id) {
                resetDailyCountersIfNeeded()
                dailySentCount += 1
                watchOutboundChat(jid)
            }
            return { sent, delayMs }
        })
        if (!result?.sent || !result.sent.key || !result.sent.key.id) {
            throw new Error('ENVIO_FALHOU')
        }
        console.log('[ENVIO_OK_REAL]', jid, `delay=${result.delayMs}ms`)
        return res.json({ status: 'sent', delay_ms: result.delayMs, message_id: result.sent.key.id })
    } catch (e) {
        console.log('[ENVIO_FALHOU]', req.body?.number || '', e.message)
        return res.status(500).json({ status: 'failed', error: e.message })
    }
})

if (!acquireProcessLock()) {
    process.exit(0)
}

process.on('uncaughtException', (err) => {
    console.error('[WA_FATAL]', err)
})

process.on('unhandledRejection', (err) => {
    console.error('[WA_PROMISE_ERROR]', err)
})

process.on('exit', () => {
    releaseProcessLock()
})

process.on('SIGINT', () => {
    releaseProcessLock()
    process.exit(0)
})

process.on('SIGTERM', () => {
    releaseProcessLock()
    process.exit(0)
})

startHttpServer()
initBaileys()
