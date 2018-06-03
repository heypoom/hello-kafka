const {HighLevelProducer, KeyedMessage, Client} = require('kafka-node')
const msgpack = require('msgpack')
const faker = require('faker')

const client = new Client()

const producer = new HighLevelProducer(client)

function send(topic, payload) {
  const payloads = [{topic, messages: msgpack.pack(payload)}]

  producer.send(payloads, (err, data) => {
    // console.log('[+] Data Sent:', data)
  })
}

function addTicket(buyer, seat) {
  send('queuing.ticket.add', {buyer, seat})
}

const seats = new Map()

producer.on('ready', () => {
  console.log('[+] Producer is now ready.')

  producer.send(
    [{topic: 'internal.status', messages: `READY:${Date.now()}`}],
    console.log,
  )

  for (let i = 1; i <= 30; i++) {
    const timer = setInterval(() => {
      const buyer = faker.name.findName()
      const seat = faker.helpers.replaceSymbols('?##')

      if (!seats.has(seat)) {
        addTicket(buyer, seat)
        seats.set(seat, buyer)

        console.log(`[> ${seats.size}] ${buyer} bought seat ${seat}`)

        if (seats.size >= 2600) {
          console.log('[!] All seats are now full!')
          clearInterval(timer)

          return
        }
      }
    }, 0)
  }

  setInterval(() => send('queuing.ticket.list'), 2000)
})

producer.on('error', console.error)

function shutdown(code) {
  console.log('[!] Shutdown:', code)

  process.exit()
}

const shutdownEvents = ['SIGINT', 'SIGQUIT', 'SIGTERM', 'SIGHUP', 'SIGSTP']

shutdownEvents.forEach(event => process.on(event, shutdown))
