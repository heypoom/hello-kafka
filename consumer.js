const {Consumer, Client} = require('kafka-node')
const msgpack = require('msgpack')

const client = new Client()

// Load all topics
function loadTopics() {
  return new Promise((resolve, reject) => {
    client.once('connect', () => {
      client.loadMetadataForTopics([], (err, results) => {
        if (err) return reject(err)

        resolve(Object.keys(results[1].metadata))
      })
    })
  })
}

// Retrieves a list of topics that matches the pattern
async function Topics(pattern) {
  const topics = await loadTopics()

  if (!(pattern instanceof RegExp)) {
    pattern = new RegExp(pattern)
  }

  return topics.filter(x => pattern.test(x))
}

const asTopic = topic => ({topic, offset: 0})

const seats = new Map()

function printSeat(seats) {
  if (seats.size === 0) return

  const tickets = [...seats]
    .map(x => ({seat: x[0], buyer: x[1]}))
    .sort((a, b) => a.seat.localeCompare(b.seat))

  console.log()

  console.log('[+] Seats:')
  tickets.forEach(t => console.log(`${t.seat} ${t.buyer}`))

  console.log()
}

async function main() {
  const topics = await Topics('queuing.ticket.*')
  console.log('[>] Subscribing to:', topics)

  const consumer = new Consumer(client, topics.map(asTopic), {
    autoCommit: true,
    encoding: 'buffer',
  })

  consumer.on('message', msg => {
    if (msg.topic.startsWith('queuing.ticket.add')) {
      const payload = msgpack.unpack(msg.value)

      const {seat, buyer} = payload
      const prevBuyer = seats.get(seat)

      if (prevBuyer) {
        console.error('[!!] Seat', seat, 'had been taken by', prevBuyer)

        return
      }

      seats.set(seat, buyer)
      // console.log('[+] Booked seat', seat, 'for', buyer)

      return
    }

    if (msg.topic.startsWith('queuing.ticket.list')) {
      printSeat(seats)

      console.log('[+] There are', seats.size, 'seats as of now.')

      if (seats.size >= 2600) {
        console.log('[!] All seats are now full!')
      }

      return
    }

    console.log('Incoming Message:', msg)
  })

  consumer.on('error', err => {
    console.error('Error:', err)
  })

  consumer.on('offsetOutOfRange', err => {
    console.error('offsetOutOfRange:', err)
  })
}

main()
