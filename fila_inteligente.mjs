
// ===== CONFIG =====
const LIMITE_DIA = 100
const INICIO = 9   // 09:00
const FIM = 18     // 18:00

let enviadosHoje = 0
let fila = []
let enviando = false

// ===== RESET DIARIO =====
setInterval(() => {
    const agora = new Date()
    if (agora.getHours() === 0 && agora.getMinutes() === 0) {
        enviadosHoje = 0
        console.log('?? RESET DIARIO')
    }
}, 60000)

// ===== DELAY INTELIGENTE =====
function calcularDelay() {
    const horasAtivas = FIM - INICIO
    const segundos = horasAtivas * 3600

    const base = segundos / LIMITE_DIA

    // randomiza ?30%
    const variacao = base * 0.3
    return Math.floor((base + (Math.random() * variacao * 2 - variacao)) * 1000)
}

// ===== PROCESSADOR =====
async function processarFila() {
    if (enviando || fila.length === 0 || !sock) return

    const agora = new Date()
    const hora = agora.getHours()

    if (hora < INICIO || hora >= FIM) {
        return setTimeout(processarFila, 60000)
    }

    if (enviadosHoje >= LIMITE_DIA) {
        console.log('?? LIMITE DIARIO ATINGIDO')
        return
    }

    enviando = true
    const job = fila.shift()

    try {
        const jid = job.number + '@s.whatsapp.net'
        await sock.sendMessage(jid, { text: job.text })

        enviadosHoje++
        console.log(?? / -> )

    } catch (e) {
        console.log('? ERRO:', e.message)
    }

    enviando = false

    const delay = calcularDelay()
    setTimeout(processarFila, delay)
}

// ===== API =====
app.post('/send', async (req, res) => {
    const { number, text } = req.body

    if (!number || !text) {
        return res.status(400).json({ error: 'number/text missing' })
    }

    fila.push({ number, text })

    console.log('?? FILA:', number)

    processarFila()

    res.json({ status: 'queued' })
})

