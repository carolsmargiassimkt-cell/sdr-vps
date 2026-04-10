import fs from 'fs'

const LOCK_FILE = './baileys.lock'

function createLock() {
    if (fs.existsSync(LOCK_FILE)) {
        console.log('?? J? EXISTE OUTRO BOT RODANDO')
        process.exit(1)
    }

    fs.writeFileSync(LOCK_FILE, process.pid.toString())
}

function removeLock() {
    if (fs.existsSync(LOCK_FILE)) {
        fs.unlinkSync(LOCK_FILE)
    }
}

process.on('exit', removeLock)
process.on('SIGINT', () => { removeLock(); process.exit() })
process.on('SIGTERM', () => { removeLock(); process.exit() })

createLock()
