app.post('/send', async (req, res) => {
    const { number, text } = req.body

    if (!number || !text) {
        return res.status(400).json({ error: 'number/text missing' })
    }

    return new Promise((resolve) => {
        fila.push({
            number,
            text,
            resolve: (data) => {
                res.json(data)
                resolve()
            },
            reject: (err) => {
                res.status(500).json({ error: err.message })
                resolve()
            }
        })

        processarFila()
    })
})
