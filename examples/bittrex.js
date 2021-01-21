const { streamNormalized, normalizeTrades, normalizeBookChanges } = require('../dist')

async function run() {
  const messages = streamNormalized(
    {
      exchange: 'bittrex',
      symbols: ['ETH-USD']
    },
    normalizeBookChanges
  )

  for await (const message of messages) {
    console.log(message)
  }
}

run()
