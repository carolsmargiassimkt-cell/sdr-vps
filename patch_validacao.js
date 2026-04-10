        const jid = job.number + '@s.whatsapp.net'

        const exists = await sock.onWhatsApp(job.number)

        if (!exists || exists.length === 0) {
            console.log('NUMERO SEM WHATSAPP:', job.number)
            if (job.reject) job.reject(new Error('sem whatsapp'))
            return
        }

        console.log('VALIDO:', job.number)

        await sock.sendMessage(jid, { text: job.text })
