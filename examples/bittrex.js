const { streamNormalized, normalizeTrades, normalizeBookChanges } = require('../dist')

async function bittrex() {
  const messages = streamNormalized(
    {
      exchange: 'bittrex',
      symbols: ['ETH-USD', 'ETH-BTC']
    },
    // normalizeTrades,
    normalizeBookChanges,
  )

  for await (const message of messages) {
    // console.log('New message: ', message)
  }
}

bittrex()
