        await conectarSePreciso()

        const jid = job.number + '@s.whatsapp.net'

        const msg = await sock.sendMessage(jid, { text: job.text })

        console.log('ENVIANDO...', job.number)

        // espera confirmação real
        await new Promise(resolve => setTimeout(resolve, 5000))

        enviadosHoje++
        console.log('ENVIADO CONFIRMADO', enviadosHoje, '/', LIMITE_DIA, '->', job.number)

        // agora sim pode fechar
        if (sock) {
            sock.ws.close()
            sock = null
        }
