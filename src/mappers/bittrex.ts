import { inflateRawSync } from 'zlib'
import { BookChange, Exchange } from '../types'
import { Mapper } from './mapper'

export class BittrexOrderChangeMapper implements Mapper<'bittrex', BookChange> {
  constructor(protected readonly exchange: Exchange) {}

  canHandle(message: BittrexMessageType) {
    return Array.isArray(message.M) && message.M.length > 0
  }

  getFilters(symbols?: string[]) {
    return [
      {
        channel: 'depthSnapshot',
        symbols
      } as const,
      {
        channel: 'orderBook',
        symbols
      } as const
    ]
  }

  *map(message: BittrexMessageType, localTimestamp: Date): IterableIterator<BookChange> {
    const data: BittrexOrderChange = decodeMessage(message)
    const bookChange: BookChange = {
      type: 'book_change',
      symbol: data.marketSymbol,
      exchange: this.exchange,
      isSnapshot: false,
      bids: data.bidDeltas.map(mapBookLevel),
      asks: data.askDeltas.map(mapBookLevel),
      timestamp: localTimestamp,
      localTimestamp
    }
    yield bookChange
  }
}

function mapBookLevel(level: BittrexBookLevel) {
  const amount = Number(level.quantity)
  const price = Number(level.rate)
  return { price, amount }
}

type BittrexMessageType = { M: { A: string[] }[] }

type BittrexBookLevel = { quantity: number; rate: number }

type BittrexOrderChange = {
  marketSymbol: string
  depth: number
  sequence: number
  bidDeltas: BittrexBookLevel[]
  askDeltas: BittrexBookLevel[]
}
function decodeMessage(message: BittrexMessageType) {
  return JSON.parse(inflateRawSync(Buffer.from(message.M.flatMap((m) => m.A[0])[0], 'base64')).toString())
}
