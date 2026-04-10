        sock.ev.on('connection.update', (update) => {
            const { connection, lastDisconnect, qr } = update

            if (qr) {
                console.log('ESCANEIE O QR')
                qrcode.generate(qr, { small: true })
            }

            if (connection === 'open') {
                console.log('CONECTADO ESTAVEL')
            }

            if (connection === 'close') {
                const code = lastDisconnect?.error?.output?.statusCode
                console.log('FECHOU CODE:', code)
            }
        })
