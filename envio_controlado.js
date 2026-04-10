        const jid = job.number + '@s.whatsapp.net'

        const check = await sock.onWhatsApp(job.number)

        if (!check || check.length === 0) {
            console.log('SEM WHATSAPP:', job.number)

            if (job.resolve) job.resolve({
                status: 'invalid',
                number: job.number
            })

            return
        }

        console.log('VALIDO:', job.number)

        await sock.sendMessage(jid, { text: job.text })

        if (job.resolve) job.resolve({
            status: 'sent',
            number: job.number
        })
