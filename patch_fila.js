async function processarFila() {
    if (enviando || fila.length === 0 || !sock) return

    enviando = true
    const job = fila.shift()

    try {
        const jid = job.number + '@s.whatsapp.net'

        console.log('ENVIANDO...', job.number)

        const msg = await sock.sendMessage(jid, { text: job.text })

        // aguarda sync real
        await new Promise(r => setTimeout(r, 4000))

        enviadosHoje++
        console.log('ENVIADO CONFIRMADO', enviadosHoje, '/', LIMITE_DIA, '->', job.number)

        if (job.resolve) job.resolve({ status: 'sent' })

    } catch (e) {
        console.log('ERRO ENVIO:', e.message)
        if (job.reject) job.reject(e)
    }

    enviando = false
    setTimeout(processarFila, calcularDelay())
}
