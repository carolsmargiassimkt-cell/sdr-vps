const baileys = require('@whiskeysockets/baileys')
const makeWASocket = baileys.default
const { DisconnectReason, fetchLatestBaileysVersion, useMultiFileAuthState } = baileys
const express = require('express')
const fs = require('fs')
const path = require('path')
let sock = null
let ready = false
let bootFailed = false
let starting = false
let reconnectTimer = null
let lastCrashPrevented = ''
let heartbeatTimer = null
let httpServer = null
let bootStartedAt = 0
let currentConnectionState = 'idle'
let channelState = 'OK'
let blockedUntil = 0
let reconnectBlockedUntil = 0
let reconnectBlockTimer = null
let lastErrorCode = 0
let retryCount = 0
let sendFailureCount = 0
let recoveryStage = 0
const recentSendCache = new Map()
const incomingInbox = []
const incomingSeen = new Set()
const unreadChats = new Map()
const pendingOutgoing = new Map()
const outboundQueue = []
let outboundActive = false
const MAX_INCOMING_ITEMS = Math.max(1000, Number(process.env.BAILEYS_INBOX_MAX_ITEMS || 1000))
const DEFAULT_REST_HOURS = Math.max(12, Number(process.env.WHATSAPP_REST_HOURS || 24))
const START_WARMUP_MINUTES = Math.max(0, Number(process.env.BAILEYS_START_WARMUP_MINUTES || 0))
const RECONNECT_DELAY_MS = Math.max(3000, Number(process.env.BAILEYS_RECONNECT_DELAY_MS || 5000))
const ERROR_405_RECONNECT_DELAY_MS = Math.max(10000, Number(process.env.BAILEYS_405_RECONNECT_DELAY_MS || 10000))
const ERROR_405_BLOCK_MS = Math.max(60000, Number(process.env.BAILEYS_405_BLOCK_MS || 60000))
const ALLOW_PAIRING = String(process.env.BAILEYS_ALLOW_PAIRING || '').trim().toLowerCase() === '1'
const BLOCKED_MIN_MS = Math.max(30, Number(process.env.BAILEYS_BLOCKED_MIN_MIN || 30)) * 60 * 1000
const BLOCKED_MAX_MS = Math.max(BLOCKED_MIN_MS, Number(process.env.BAILEYS_BLOCKED_MAX_MIN || 60) * 60 * 1000)
const RECOVERY_BACKOFFS_MS = [2000, 5000, 10000]
const channelStateFile = path.resolve('./logs/whatsapp_channel_state.json')
const defaultAuthDir = path.resolve(__dirname, '../../auth_info_baileys')
const authDir = path.resolve(process.env.BAILEYS_AUTH_DIR || defaultAuthDir)
const credsFile = path.join(authDir, 'creds.json')
const runtimeDir = path.resolve('./runtime')
const singletonLockFile = path.join(runtimeDir, 'baileys.singleton.lock')

const nowIso = () => new Date().toISOString()
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)))
const SEND_TIMEOUT_MS = Math.max(5000, Number(process.env.BAILEYS_SEND_TIMEOUT_MS || 10000))
const SEND_RETRY_DELAY_MS = Math.max(1000, Number(process.env.BAILEYS_SEND_RETRY_DELAY_MS || 3000))
const SEND_DEDUPE_WINDOW_MS = Math.max(5000, Number(process.env.BAILEYS_SEND_DEDUPE_WINDOW_MS || 30000))
const SEND_OPERATION_TIMEOUT_MS = Math.max(10000, Number(process.env.BAILEYS_SEND_OPERATION_TIMEOUT_MS || 10000))
const HEARTBEAT_INTERVAL_MS = Math.max(5000, Number(process.env.BAILEYS_HEARTBEAT_INTERVAL_MS || 10000))
const BOOT_GRACE_MS = Math.max(30000, Number(process.env.BAILEYS_BOOT_GRACE_MS || 90000))
const VERSION_FETCH_TIMEOUT_MS = Math.max(500, Number(process.env.BAILEYS_VERSION_FETCH_TIMEOUT_MS || 2000))
const INBOX_WEBHOOK_URL = String(process.env.WHATSAPP_INBOX_WEBHOOK_URL || 'http://127.0.0.1:8011/webhook').trim()
const INBOX_WEBHOOK_TIMEOUT_MS = Math.max(1000, Number(process.env.WHATSAPP_INBOX_WEBHOOK_TIMEOUT_MS || 5000))
const API_HOST = String(process.env.BAILEYS_API_HOST || '127.0.0.1').trim() || '127.0.0.1'
const API_PORT = Math.max(1, Number(process.env.BAILEYS_API_PORT || 3000))
const normalizePhone = (raw) => String(raw || '').replace(/\D+/g, '')
const normalizeOutgoingText = (raw) =>
    Buffer.from(String(raw ?? '').normalize('NFKC'), 'utf8').toString('utf8').trim()
const recentSendKey = (jid, message) => `${String(jid || '').trim()}|${String(message || '').trim()}`
const isPidAlive = (pid) => {
    try {
        process.kill(Number(pid), 0)
        return true
    } catch {
        return false
    }
}
const extractIncomingText = (message) =>
    String(
        message?.message?.conversation ||
        message?.message?.extendedTextMessage?.text ||
        message?.message?.imageMessage?.caption ||
        message?.message?.videoMessage?.caption ||
        message?.message?.documentMessage?.caption ||
        message?.message?.buttonsResponseMessage?.selectedDisplayText ||
        message?.message?.listResponseMessage?.title ||
        ''
    ).trim()

const extractChatText = (chat) => {
    const directText = String(chat?.conversation || chat?.lastMessage || '').trim()
    if (directText) return directText
    for (const candidate of [chat?.lastMessage, chat?.messages?.[0], chat?.messages?.[chat?.messages?.length - 1]]) {
        if (!candidate) continue
        const wrapped = candidate?.message ? candidate : { message: candidate }
        const extracted = extractIncomingText(wrapped)
        if (extracted) return extracted
    }
    return ''
}

const extractChatMessageId = (chat) => {
    const candidates = [
        chat?.lastMessageKey?.id,
        chat?.messages?.[0]?.key?.id,
        chat?.messages?.[chat?.messages?.length - 1]?.key?.id
    ]
    for (const candidate of candidates) {
        const value = String(candidate || '').trim()
        if (value) return value
    }
    return ''
}

const captureIncomingMessages = (messages) => {
    const rows = Array.isArray(messages) ? messages : []
    for (const message of rows) {
        if (!message || message.key?.fromMe) continue
        const remoteJid = String(message.key?.remoteJid || '')
        const messageId = String(message.key?.id || '').trim()
        if (!remoteJid || remoteJid.endsWith('@g.us') || remoteJid.endsWith('@broadcast')) continue
        const phone = normalizePhone(remoteJid.split('@')[0] || '')
        if (!phone) continue
        const text = extractIncomingText(message)
        if (!text) continue
        const timestampValue = Number(message.messageTimestamp || 0)
        const timestamp = timestampValue > 0 ? new Date(timestampValue * 1000).toISOString() : nowIso()
        pushIncomingItem({
            id: messageId,
            message_id: messageId,
            phone,
            contact_name: String(message.pushName || '').trim(),
            message: text,
            timestamp
        })
        void triggerIncomingWebhook({
            id: messageId,
            message_id: messageId,
            phone,
            contact_name: String(message.pushName || '').trim(),
            message: text,
            timestamp
        })
    }
}

const pushIncomingItem = (item) => {
    const messageId = String(item?.message_id || item?.id || '').trim()
    const phone = normalizePhone(item?.phone)
    const message = String(item?.message || '').trim()
    const timestamp = String(item?.timestamp || nowIso()).trim()
    if (!phone || !message) return
    const dedupeKey = messageId ? `id|${messageId}` : `${phone}|${timestamp}|${message}`
    if (incomingSeen.has(dedupeKey)) return
    incomingSeen.add(dedupeKey)
    incomingInbox.unshift({
        id: messageId,
        message_id: messageId,
        phone,
        contact_name: String(item?.contact_name || '').trim(),
        message,
        timestamp
    })
    console.log(`[BAILEYS_INBOX_CAPTURED] phone=${phone} ts=${timestamp}`)
    while (incomingInbox.length > MAX_INCOMING_ITEMS) {
        const removed = incomingInbox.pop()
        if (!removed) continue
        const removedKey = String(removed?.message_id || removed?.id || '').trim()
            ? `id|${String(removed?.message_id || removed?.id || '').trim()}`
            : `${removed.phone}|${removed.timestamp}|${removed.message}`
        incomingSeen.delete(removedKey)
    }
}

const triggerIncomingWebhook = async (item) => {
    if (!INBOX_WEBHOOK_URL) return
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), INBOX_WEBHOOK_TIMEOUT_MS)
    try {
        const response = await fetch(INBOX_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json; charset=utf-8' },
            body: JSON.stringify(item || {}),
            signal: controller.signal
        })
        if (!response.ok) {
            console.warn(`[WEBHOOK_FAIL] status=${response.status}`)
            return
        }
        console.log(`[WEBHOOK_OK] phone=${String(item?.phone || '').trim()}`)
    } catch (err) {
        console.warn('[WEBHOOK_FAIL]', String(err?.message || err || 'webhook_fail'))
    } finally {
        clearTimeout(timeout)
    }
}

const syncUnreadChat = (chat) => {
    const id = String(chat?.id || '')
    if (!id || id.endsWith('@g.us') || id.endsWith('@broadcast')) return
    const unreadCount = Number(chat?.unreadCount || 0)
    if (unreadCount <= 0) {
        unreadChats.delete(id)
        return
    }
    const phone = normalizePhone(id.split('@')[0] || '')
    if (!phone) return
    const timestampValue = Number(chat?.conversationTimestamp || chat?.timestamp || 0)
    const timestamp = timestampValue > 0 ? new Date(timestampValue * 1000).toISOString() : nowIso()
    const lastMessage = extractChatText(chat)
    const messageId = extractChatMessageId(chat)
    unreadChats.set(id, {
        id,
        message_id: messageId,
        phone,
        contact_name: String(chat?.name || chat?.formattedName || '').trim(),
        unread_count: unreadCount,
        timestamp,
        message: lastMessage
    })
}

const buildInboxItems = (limit) => {
    const merged = [...incomingInbox]
    const seen = new Set(
        merged.map((item) =>
            String(item?.message_id || item?.id || '').trim()
                ? `id|${String(item?.message_id || item?.id || '').trim()}`
                : `${item.phone}|${item.timestamp}|${item.message}`
        )
    )
    for (const chat of unreadChats.values()) {
        if (!String(chat?.message || '').trim()) continue
        const dedupeKey = String(chat?.message_id || chat?.id || '').trim()
            ? `id|${String(chat?.message_id || chat?.id || '').trim()}`
            : `${chat.phone}|${chat.timestamp}|${chat.message}`
        if (seen.has(dedupeKey)) continue
        merged.push({
            id: String(chat?.message_id || chat?.id || '').trim(),
            message_id: String(chat?.message_id || chat?.id || '').trim(),
            phone: chat.phone,
            contact_name: chat.contact_name,
            message: chat.message,
            timestamp: chat.timestamp,
            unread_count: chat.unread_count
        })
    }
    merged.sort((a, b) => String(b.timestamp || '').localeCompare(String(a.timestamp || '')))
    return merged.slice(0, limit)
}

const clearDeliveredInboxItems = (items) => {
    const rows = Array.isArray(items) ? items : []
    if (!rows.length) return
    const deliveredIds = new Set(
        rows
            .map((item) => String(item?.message_id || item?.id || '').trim())
            .filter(Boolean)
    )
    if (deliveredIds.size > 0) {
        for (let index = incomingInbox.length - 1; index >= 0; index -= 1) {
            const item = incomingInbox[index]
            const messageId = String(item?.message_id || item?.id || '').trim()
            if (!messageId || !deliveredIds.has(messageId)) continue
            incomingInbox.splice(index, 1)
            incomingSeen.delete(`id|${messageId}`)
        }
    }
}

const outgoingKey = (jid, messageId) => `${String(jid || '').trim()}|${String(messageId || '').trim()}`

const resolveOutgoing = (jid, messageId, payload) => {
    const key = outgoingKey(jid, messageId)
    const pending = pendingOutgoing.get(key)
    if (!pending) return
    clearTimeout(pending.timeout)
    pendingOutgoing.delete(key)
    try {
        pending.resolve(payload)
    } catch {}
}

const rejectOutgoing = (jid, messageId, errorMessage) => {
    const key = outgoingKey(jid, messageId)
    const pending = pendingOutgoing.get(key)
    if (!pending) return
    clearTimeout(pending.timeout)
    pendingOutgoing.delete(key)
    try {
        pending.reject(new Error(String(errorMessage || 'SEND_UNCONFIRMED')))
    } catch {}
}

const rejectOutgoingForJid = (jid, errorMessage) => {
    const prefix = `${String(jid || '').trim()}|`
    for (const key of Array.from(pendingOutgoing.keys())) {
        if (!String(key || '').startsWith(prefix)) continue
        const pending = pendingOutgoing.get(key)
        if (!pending) continue
        clearTimeout(pending.timeout)
        pendingOutgoing.delete(key)
        try {
            pending.reject(new Error(String(errorMessage || 'SEND_UNCONFIRMED')))
        } catch {}
    }
}

const waitForOutgoingConfirmation = (jid, messageId, timeoutMs = 15000) => {
    const key = outgoingKey(jid, messageId)
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            pendingOutgoing.delete(key)
            reject(new Error('SEND_CONFIRMATION_TIMEOUT'))
        }, Math.max(1000, Number(timeoutMs) || 15000))
        pendingOutgoing.set(key, { resolve, reject, timeout })
    })
}

const withTimeout = async (promise, timeoutMs, timeoutLabel) => {
    let timer = null
    try {
        return await Promise.race([
            promise,
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error(String(timeoutLabel || 'TIMEOUT'))), Math.max(1000, Number(timeoutMs) || 10000))
            })
        ])
    } finally {
        if (timer) clearTimeout(timer)
    }
}

const enqueueSerializedSend = (taskFactory) =>
    new Promise((resolve, reject) => {
        outboundQueue.push({ taskFactory, resolve, reject, queuedAt: Date.now() })
        if (!outboundActive) {
            void drainOutboundQueue()
        }
    })

const drainOutboundQueue = async () => {
    if (outboundActive) return
    outboundActive = true
    try {
        while (outboundQueue.length > 0) {
            const item = outboundQueue.shift()
            if (!item) continue
            try {
                const result = await item.taskFactory()
                item.resolve(result)
            } catch (err) {
                item.reject(err)
            }
        }
    } finally {
        outboundActive = false
    }
}

const socketLooksOpen = () => {
    const wsReadyState = Number(sock?.ws?.readyState)
    const isWsOpen = Number.isFinite(wsReadyState) ? wsReadyState === 1 : false
    return Boolean(sock && !bootFailed && (ready || isWsOpen))
}

const isChannelBlocked = () => channelState === 'BLOQUEADO' && Date.now() < blockedUntil

const markChannelState = (state, reason = '') => {
    channelState = state
    writeChannelState({
        channel_state: state.toLowerCase(),
        last_error: String(reason || '').trim() || ''
    })
    console.log(`[CHANNEL_STATE] state=${state} reason=${reason}`)
    if (state === 'OK') {
        recoveryStage = 0
    }
}

const enterBlockedState = (reason) => {
    if (channelState === 'BLOQUEADO' && Date.now() < blockedUntil) return
    markChannelState('BLOQUEADO', reason)
    const delayMs = BLOCKED_MIN_MS + Math.floor(Math.random() * (BLOCKED_MAX_MS - BLOCKED_MIN_MS + 1))
    blockedUntil = Date.now() + delayMs
    console.warn(`[FASE_1_RESFRIAMENTO_ATIVO] motivo=${reason} delay=${Math.round(delayMs / 60000)}m`)
    clearReconnectTimer()
    closeSocket(reason)
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null
        channelState = 'RECUPERANDO'
        console.log('[FASE_2_TESTE_CANAL]')
        start()
    }, delayMs)
}

const handleChannelRecovery = () => {
    if (channelState === 'RECUPERANDO') {
        recoveryStage += 1
        console.log('[FASE_3_REAQUECIMENTO]', `stage=${recoveryStage}`)
        if (recoveryStage >= 4) {
            markChannelState('OK', 'recover_success')
            console.log('[FASE_4_EXECUCAO_NORMAL]')
        }
    }
}

const isWithinBootGrace = () => {
    if (!bootStartedAt) return false
    return (Date.now() - Number(bootStartedAt || 0)) < BOOT_GRACE_MS
}

const buildStatusPayload = () => {
    const state = readChannelState()
    const sessionValid = hasPersistedSession()
    const socketOpen = socketLooksOpen()
    const offline = !sessionValid || bootFailed || state.mode === 'offline'
    const degraded = !offline && (!ready || !socketOpen || state.mode === 'reconnecting' || state.mode === 'booting' || state.mode === 'degraded')
    return {
        connected: Boolean(!offline && ready && socketOpen && sessionValid),
        status: offline ? 'offline' : (degraded ? 'degraded' : 'online'),
        socket_open: socketOpen,
        session_valid: sessionValid,
        ...state,
        last_crash_prevented: lastCrashPrevented
    }
}

const sendWhatsAppMessage = async (jid, payloadMessage) => {
    try {
        const sent = await withTimeout(
            sock.sendMessage(jid, { text: payloadMessage }),
            SEND_TIMEOUT_MS,
            'BAILEYS_SEND_TIMEOUT'
        )
        const messageId = String(sent?.key?.id || '').trim()
        if (!messageId) {
            throw new Error('missing_message_id')
        }
        const confirmation = await waitForOutgoingConfirmation(jid, messageId, SEND_TIMEOUT_MS)
        return {
            messageId,
            confirmed: true,
            status: Number(confirmation?.status || 0)
        }
    } catch (err) {
        const detail = String(err?.message || err || 'SEND_FAIL').trim() || 'SEND_FAIL'
        rejectOutgoingForJid(jid, detail)
        if (detail === 'BAILEYS_SEND_TIMEOUT' || detail === 'SEND_CONFIRMATION_TIMEOUT') {
            console.error('[BAILEYS_TIMEOUT_SEND]', `jid=${jid}`)
        } else {
            console.error('[BAILEYS_SEND_ERRO]', `jid=${jid}`, `motivo=${detail}`)
        }
        console.error('[BAILEYS_SEND_FAIL_FINAL]', `jid=${jid}`, `motivo=${detail}`)
        throw err
    }
}

const getRecentSendResult = (jid, payloadMessage) => {
    const key = recentSendKey(jid, payloadMessage)
    const cached = recentSendCache.get(key)
    if (!cached) return null
    if ((Date.now() - Number(cached.ts || 0)) > SEND_DEDUPE_WINDOW_MS) {
        recentSendCache.delete(key)
        return null
    }
    return cached.result || null
}

const cacheRecentSendResult = (jid, payloadMessage, result) => {
    const key = recentSendKey(jid, payloadMessage)
    recentSendCache.set(key, {
        ts: Date.now(),
        result
    })
}

const writeChannelState = (patch) => {
    const current = readChannelState()
    const next = {
        connected: false,
        mode: 'booting',
        rest_until: '',
        rest_hours: DEFAULT_REST_HOURS,
        updated_at: nowIso(),
        last_error: '',
        last_disconnect_code: '',
        qr_blocked: false,
        warmup_until: '',
        ...current,
        ...patch,
        updated_at: nowIso()
    }
    fs.mkdirSync(path.dirname(channelStateFile), { recursive: true })
    const tempFile = `${channelStateFile}.${process.pid}.tmp`
    fs.writeFileSync(tempFile, JSON.stringify(next, null, 2))
    fs.renameSync(tempFile, channelStateFile)
    return next
}

const readChannelState = () => {
    try {
        return JSON.parse(fs.readFileSync(channelStateFile, 'utf-8'))
    } catch {
        return {}
    }
}

const setRestMode = (reason, code = '') => {
    const restUntil = new Date(Date.now() + DEFAULT_REST_HOURS * 60 * 60 * 1000).toISOString()
    return writeChannelState({
        connected: false,
        mode: 'resting',
        rest_until: restUntil,
        rest_hours: DEFAULT_REST_HOURS,
        last_error: String(reason || 'channel_rest'),
        last_disconnect_code: code ? String(code) : '',
        qr_blocked: String(reason || '').toLowerCase().includes('qr')
    })
}

const ensureBootState = () => {
    writeChannelState({
        connected: false,
        mode: 'booting',
        rest_hours: DEFAULT_REST_HOURS,
        warmup_until: '',
        qr_blocked: false
    })
}

const ensureOfflineState = (reason, code = '') => {
    writeChannelState({
        connected: false,
        mode: 'offline',
        rest_until: '',
        rest_hours: DEFAULT_REST_HOURS,
        last_error: String(reason || 'offline'),
        last_disconnect_code: code ? String(code) : '',
        qr_blocked: false,
        warmup_until: ''
    })
}

const setAwaitingPairState = () => {
    writeChannelState({
        connected: false,
        mode: 'awaiting_pair',
        rest_until: '',
        last_error: '',
        last_disconnect_code: '',
        qr_blocked: false,
        warmup_until: ''
    })
}

const safeExitWithInvalidSession = (reason, code = '') => {
    ready = false
    writeChannelState({
        connected: false,
        mode: 'offline',
        rest_until: '',
        rest_hours: DEFAULT_REST_HOURS,
        last_error: String(reason || 'sessao_invalida'),
        last_disconnect_code: code ? String(code) : '',
        qr_blocked: String(reason || '').toLowerCase().includes('qr'),
        warmup_until: ''
    })
    console.error('[BAILEYS_SESSAO_INVALIDA]', String(reason || 'sessao_invalida'), code ? `code=${code}` : '')
    bootFailed = true
    try {
        if (sock?.end) sock.end(new Error(String(reason || 'sessao_invalida')))
    } catch {}
}

const acquireSingletonLock = () => {
    fs.mkdirSync(runtimeDir, { recursive: true })
    try {
        if (fs.existsSync(singletonLockFile)) {
            const payload = JSON.parse(fs.readFileSync(singletonLockFile, 'utf-8'))
            const existingPid = Number(payload?.pid || 0)
            if (existingPid > 0 && existingPid !== process.pid && isPidAlive(existingPid)) {
                console.error('[BAILEYS_DUPLICADO_BLOQUEADO]', `pid_ativo=${existingPid}`)
                return false
            }
        }
    } catch {}
    fs.writeFileSync(singletonLockFile, JSON.stringify({ pid: process.pid, started_at: nowIso() }, null, 2))
    return true
}

const releaseSingletonLock = () => {
    try {
        if (!fs.existsSync(singletonLockFile)) return
        const payload = JSON.parse(fs.readFileSync(singletonLockFile, 'utf-8'))
        if (Number(payload?.pid || 0) === process.pid) {
            fs.unlinkSync(singletonLockFile)
        }
    } catch {}
}

const closeSocket = (reason = 'socket_close') => {
    try {
        if (sock?.ws?.close) sock.ws.close()
    } catch {}
    try {
        if (sock?.end) sock.end(new Error(String(reason || 'socket_close')))
    } catch {}
    sock = null
}

const requestSocketRestart = (reason, options = {}) => {
    const normalizedReason = String(reason || 'restart').trim() || 'restart'
    const delayMs = Math.max(0, Number(options.delayMs || RECONNECT_DELAY_MS))
    const code = Number(options.code || 0)
    if (bootFailed) {
        ensureOfflineState(normalizedReason)
        return
    }
    if (reconnectTimer) return
    ready = false
    currentConnectionState = 'reconnecting'
    console.warn('[BAILEYS_RECONNECT]', `motivo=${normalizedReason}`)
    writeChannelState({
        connected: false,
        mode: 'reconnecting',
        last_error: normalizedReason,
        last_disconnect_code: code > 0 ? String(code) : '',
        qr_blocked: false
    })
    closeSocket(normalizedReason)
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null
        start()
    }, delayMs)
}

const startHeartbeat = () => {
    if (heartbeatTimer) return
    heartbeatTimer = setInterval(() => {
        if (bootFailed) return
        if (isWithinBootGrace()) return
        if (['connecting', 'open', 'reconnecting'].includes(String(currentConnectionState || '').trim().toLowerCase())) return
        if (!socketLooksOpen()) {
            console.error('[BAILEYS_HEARTBEAT_FAIL]', `ready=${ready}`, `mode=${readChannelState().mode || ''}`)
            requestSocketRestart('heartbeat_fail')
        }
    }, HEARTBEAT_INTERVAL_MS)
}

const stopHeartbeat = () => {
    if (!heartbeatTimer) return
    clearInterval(heartbeatTimer)
    heartbeatTimer = null
}

const hasPersistedSession = () => {
    try {
        return fs.existsSync(authDir) && fs.existsSync(credsFile) && fs.statSync(credsFile).size > 0
    } catch {
        return false
    }
}

const clearReconnectTimer = () => {
    if (!reconnectTimer) return
    clearTimeout(reconnectTimer)
    reconnectTimer = null
}

const clearReconnectBlockTimer = () => {
    if (!reconnectBlockTimer) return
    clearTimeout(reconnectBlockTimer)
    reconnectBlockTimer = null
}

const getDisconnectCode = (lastDisconnect) => {
    const outputCode = Number(lastDisconnect?.error?.output?.statusCode || 0)
    if (outputCode > 0) return outputCode
    const payloadCode = Number(lastDisconnect?.error?.data?.statusCode || 0)
    if (payloadCode > 0) return payloadCode
    return 0
}

const scheduleReconnect = (reason, options = {}) => {
    const code = Number(options.code || 0)
    const delayMs = Math.max(0, Number(options.delayMs || RECONNECT_DELAY_MS))

    if (code === 405) {
        retryCount += 1
        lastErrorCode = 405
        if (retryCount > 3) {
            reconnectBlockedUntil = Date.now() + ERROR_405_BLOCK_MS
            clearReconnectTimer()
            clearReconnectBlockTimer()
            console.warn('[CONEXAO_BLOQUEADA_TEMPORARIAMENTE]', `code=405`, `retryCount=${retryCount}`, `aguarde_ms=${ERROR_405_BLOCK_MS}`)
            writeChannelState({
                connected: false,
                mode: 'degraded',
                last_error: 'connection_failure_405_blocked',
                last_disconnect_code: '405',
                qr_blocked: false
            })
            reconnectBlockTimer = setTimeout(() => {
                reconnectBlockTimer = null
                reconnectBlockedUntil = 0
                retryCount = 0
                lastErrorCode = 0
                console.log('[CONEXAO_BLOQUEADA_LIBERADA]', 'code=405')
                start()
            }, ERROR_405_BLOCK_MS)
            return
        }
    } else {
        retryCount = 0
        lastErrorCode = code
    }

    requestSocketRestart(reason || 'connection_closed', { delayMs, code })
}

const start = async () => {
    if (bootFailed || starting) return
    if (isChannelBlocked()) {
        console.warn('[CANAL_BLOQUEADO_AGUARDANDO]', `until=${new Date(blockedUntil).toISOString()}`)
        return
    }
    if (reconnectBlockedUntil && Date.now() < reconnectBlockedUntil) {
        console.warn('[CONEXAO_BLOQUEADA_TEMPORARIAMENTE]', `until=${new Date(reconnectBlockedUntil).toISOString()}`)
        return
    }
    if (sock && (socketLooksOpen() || ['connecting', 'open', 'logging in'].includes(String(currentConnectionState || '').trim().toLowerCase()))) {
        console.log('[SOCKET_REUTILIZADO]', `state=${currentConnectionState}`)
        return
    }
    starting = true
    bootStartedAt = Date.now()
    currentConnectionState = 'connecting'
    clearReconnectTimer()
    ensureBootState()
    console.log('[BAILEYS_START]')

    try {
        fs.mkdirSync(authDir, { recursive: true })
        if (!hasPersistedSession()) {
            safeExitWithInvalidSession('sessao_inexistente_ou_invalida')
            return
        }

        const { state, saveCreds } = await useMultiFileAuthState(authDir)
        let version = null
        try {
            const latest = await withTimeout(
                fetchLatestBaileysVersion(),
                VERSION_FETCH_TIMEOUT_MS,
                'BAILEYS_VERSION_FETCH_TIMEOUT'
            )
            version = latest?.version || null
        } catch (err) {
            console.warn('[BAILEYS_VERSION_FALLBACK]', String(err?.message || err || 'version_fetch_failed'))
        }

        if (sock?.ev?.removeAllListeners) {
            sock.ev.removeAllListeners('connection.update')
            sock.ev.removeAllListeners('creds.update')
        }

        const socketConfig = {
            auth: state,
            printQRInTerminal: false,
            passive: false,
            keepAliveIntervalMs: 30000,
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: 60000
        }
        if (Array.isArray(version) && version.length > 0) {
            socketConfig.version = version
        }
        sock = makeWASocket(socketConfig)

        sock.ev.on('creds.update', saveCreds)

        sock.ev.on('messages.upsert', (payload) => {
            try {
                captureIncomingMessages(payload?.messages)
            } catch (err) {
                console.warn('[BAILEYS_INBOX_CAPTURE_FAIL]', String(err?.message || err || 'inbox_capture_fail'))
            }
        })

        sock.ev.on('messaging-history.set', (payload) => {
            try {
                captureIncomingMessages(payload?.messages)
                const chats = Array.isArray(payload?.chats) ? payload.chats : []
                for (const chat of chats) syncUnreadChat(chat)
            } catch (err) {
                console.warn('[BAILEYS_HISTORY_CAPTURE_FAIL]', String(err?.message || err || 'history_capture_fail'))
            }
        })

        sock.ev.on('chats.upsert', (payload) => {
            try {
                const chats = Array.isArray(payload) ? payload : []
                for (const chat of chats) syncUnreadChat(chat)
            } catch (err) {
                console.warn('[BAILEYS_CHATS_UPSERT_FAIL]', String(err?.message || err || 'chats_upsert_fail'))
            }
        })

        sock.ev.on('chats.update', (payload) => {
            try {
                const chats = Array.isArray(payload) ? payload : []
                for (const chat of chats) syncUnreadChat(chat)
            } catch (err) {
                console.warn('[BAILEYS_CHATS_UPDATE_FAIL]', String(err?.message || err || 'chats_update_fail'))
            }
        })

        sock.ev.on('messages.update', (payload) => {
            try {
                const updates = Array.isArray(payload) ? payload : []
                for (const item of updates) {
                    const key = item?.key || {}
                    if (!key?.fromMe) continue
                    const remoteJid = String(key?.remoteJid || '').trim()
                    const messageId = String(key?.id || '').trim()
                    if (!remoteJid || !messageId) continue
                    const statusValue = Number(item?.update?.status || 0)
                    if (statusValue > 0) {
                        resolveOutgoing(remoteJid, messageId, { confirmed: true, status: statusValue })
                    }
                }
            } catch (err) {
                console.warn('[BAILEYS_MESSAGES_UPDATE_FAIL]', String(err?.message || err || 'messages_update_fail'))
            }
        })

        sock.ev.on('connection.update', (update) => {
            const { connection, qr, lastDisconnect } = update
            if (connection) {
                currentConnectionState = String(connection || '').trim().toLowerCase()
            }

            if (qr) {
                if (ALLOW_PAIRING && !hasPersistedSession()) {
                    setAwaitingPairState()
                    console.log('[BAILEYS_QR_READY]')
                    return
                }
                console.warn('[BAILEYS_QR_IGNORADO] sessao_existente')
                return
            }

            if (connection === 'open') {
                const warmupUntil = new Date(Date.now() + START_WARMUP_MINUTES * 60 * 1000).toISOString()
                console.log('[BAILEYS_CONECTADO]')
                console.log('[BAILEYS_RUNNING]')
                console.log('[BAILEYS_RECOVERY_OK]')
                ready = true
                currentConnectionState = 'open'
                retryCount = 0
                lastErrorCode = 0
                reconnectBlockedUntil = 0
                clearReconnectBlockTimer()
                clearReconnectTimer()
                startHeartbeat()
                writeChannelState({
                    connected: true,
                    mode: 'connected',
                    rest_until: '',
                    last_error: '',
                    last_disconnect_code: '',
                    qr_blocked: false,
                    warmup_until: warmupUntil
                })
                return
            }

            if (connection === 'close') {
                ready = false
                currentConnectionState = 'close'
                stopHeartbeat()
                const code = getDisconnectCode(lastDisconnect)
                console.warn('[BAILEYS_DESCONECTADO]', code ? `code=${code}` : 'sem_code')
                const invalidCodes = new Set([
                    Number(DisconnectReason.loggedOut || 401),
                    Number(DisconnectReason.badSession || 500),
                    401
                ])
                if (invalidCodes.has(code)) {
                    safeExitWithInvalidSession('connection_closed', code)
                    return
                }
                if (code === 405) {
                    console.warn('[BAILEYS_CONNECTION_FAILURE_405]', 'aguardando 10000ms antes de reconectar')
                    scheduleReconnect(`connection_failure_${code}`, {
                        code,
                        delayMs: ERROR_405_RECONNECT_DELAY_MS
                    })
                    return
                }
                console.warn('[BAILEYS_RECONECTANDO]', code ? `code=${code}` : 'sem_code')
                scheduleReconnect(`connection_closed_${code || 'sem_code'}`, { code })
            }
        })
    } catch (err) {
        console.error('[BAILEYS_CRASH]', String(err?.message || err || 'start_fail'))
        safeExitWithInvalidSession(err?.message || 'falha_ao_iniciar_sessao')
    } finally {
        starting = false
    }
}

process.on('unhandledRejection', (reason) => {
    lastCrashPrevented = String(reason?.message || reason || 'unhandled_rejection')
    console.error('[BAILEYS_CRASH_INTERCEPTADO]', `type=unhandledRejection`, `motivo=${lastCrashPrevented}`)
})

process.on('uncaughtException', (err) => {
    lastCrashPrevented = String(err?.message || err || 'uncaught_exception')
    console.error('[BAILEYS_CRASH_INTERCEPTADO]', `type=uncaughtException`, `motivo=${lastCrashPrevented}`)
})

process.on('exit', releaseSingletonLock)
process.on('SIGINT', () => {
    releaseSingletonLock()
    process.exit(0)
})
process.on('SIGTERM', () => {
    releaseSingletonLock()
    process.exit(0)
})

// ================= API =================

const app = express()
app.use(express.json())

app.get('/status', (req, res) => {
    res.json(buildStatusPayload())
})

app.get('/inbox', (req, res) => {
    const limit = Math.max(1, Math.min(Number(req.query.limit || 20), 100))
    const items = buildInboxItems(limit)
    clearDeliveredInboxItems(items)
    res.json({ items })
})

const handleSendRequest = async (req, res) => {
    const state = readChannelState()

    if (state.mode === 'resting') {
        return res.status(409).json({
            error: 'CHANNEL_RESTING',
            rest_until: state.rest_until || '',
            last_error: state.last_error || ''
        })
    }

    if (!ready) {
        return res.status(503).json({ error: 'NOT_READY' })
    }

    try {
        const { number, message, text, presence, typingDelayMs } = req.body
        const payloadMessage = normalizeOutgoingText(message ?? text ?? '')
        if (!number || !payloadMessage) {
            return res.status(400).json({ error: 'INVALID_PAYLOAD' })
        }
        const jid = `${String(number).trim()}@s.whatsapp.net`
        const requestedPresence = String(presence || '').trim().toLowerCase()
        const typingDelay = Math.max(0, Number(typingDelayMs || 0))
        const recentResult = getRecentSendResult(jid, payloadMessage)
        if (recentResult) {
            return res.json({
                ...recentResult,
                deduped: true
            })
        }

        try {
            const result = await enqueueSerializedSend(async () =>
                withTimeout(
                    (async () => {
                        if (requestedPresence === 'composing') {
                            try {
                                await withTimeout(sock.sendPresenceUpdate('composing', jid), 3000, 'PRESENCE_TIMEOUT')
                                if (typingDelay > 0) {
                                    await sleep(Math.min(typingDelay, 3000))
                                }
                                await withTimeout(sock.sendPresenceUpdate('paused', jid), 3000, 'PRESENCE_TIMEOUT')
                            } catch (presenceErr) {
                                console.warn(
                                    '[BAILEYS_PRESENCE_FAIL]',
                                    String(presenceErr?.message || presenceErr || 'presence_fail')
                                )
                            }
                        }
                        return sendWhatsAppMessage(jid, payloadMessage)
                    })(),
                    SEND_OPERATION_TIMEOUT_MS,
                    'BAILEYS_SEND_OPERATION_TIMEOUT'
                )
            )
            cacheRecentSendResult(jid, payloadMessage, result)
            return res.json(result)
        } catch (confirmErr) {
            const detail = String(confirmErr?.message || 'SEND_FAIL').trim() || 'SEND_FAIL'
            rejectOutgoingForJid(jid, detail)
            const statusCode = detail.includes('TIMEOUT') ? 504 : 502
            return res.status(statusCode).json({ error: statusCode === 504 ? 'SEND_TIMEOUT' : 'SEND_FAIL', detail })
        }
    } catch (err) {
        const errorMessage = String(err?.message || err || 'SEND_FAIL').trim() || 'SEND_FAIL'
        console.error('[BAILEYS_SEND_ERRO]', `motivo=${errorMessage}`)
        return res.status(502).json({ error: 'SEND_FAIL', detail: errorMessage })
    }
}

app.post('/send', handleSendRequest)
app.post('/message/send', handleSendRequest)

app.post('/webhook', (req, res) => {
    const payload = req.body || {}
    const fromMe = Boolean(payload.fromMe || payload.from_me || payload.self)
    if (fromMe) {
        console.log('[WEBHOOK_IGNORADO] mensagem_de_self')
        return res.status(204).json({ ignored: true })
    }
    const rawPhone = String(payload.number || payload.remoteJid || payload.sender || payload.from || '').trim()
    const strippedPhone = rawPhone.replace(/@.+$/, '')
    const phone = normalizePhone(strippedPhone)
    if (!phone) {
        console.warn('[WEBHOOK_INVALIDO] telefone_nao_identificado raw=%s', rawPhone)
        return res.status(400).json({ error: 'invalid_phone' })
    }
    const text = String(payload.text || payload.message || payload.body || '').trim()
    if (!text) {
        console.warn('[WEBHOOK_INVALIDO] texto_vazio telefone=%s', phone)
        return res.status(400).json({ error: 'empty_text' })
    }
    const messageId = String(payload.messageId || payload.id || `evo_${Date.now()}`).trim()
    const item = {
        id: messageId,
        message_id: messageId,
        phone,
        contact_name: String(payload.contact_name || payload.senderName || '').trim(),
        message: text,
        timestamp: String(payload.timestamp || nowIso()).trim(),
    }
    pushIncomingItem(item)
    void triggerIncomingWebhook(item)
    console.log('[WEBHOOK_EVOLUTION] phone=%s id=%s', phone, messageId)
    return res.json({ status: 'ok', received: true })
})

if (!acquireSingletonLock()) {
    process.exit(0)
}

httpServer = app.listen(API_PORT, API_HOST, () => {
    console.log(`API ${API_HOST}:${API_PORT}`)
    startHeartbeat()
    start()
})

httpServer.on('error', (err) => {
    console.error('[BAILEYS_CRASH]', String(err?.message || err || 'http_server_error'))
})
