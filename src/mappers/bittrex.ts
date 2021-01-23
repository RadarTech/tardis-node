import { inflateRawSync } from 'zlib'
import { BookChange, Exchange } from '../types'
import { Mapper } from './mapper'

export class BittrexOrderChangeMapper implements Mapper<'bittrex', BookChange> {
  constructor(protected readonly exchange: Exchange) {}

  canHandle(message: BittrexMessageType) {
    return (Array.isArray(message.M) && message.M.length > 0) || message.stream === 'depthSnapshot'
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
    // the response between message from snapshot service and message from signalr are different
    let bookChange: BookChange
    if (message.data) {
      const data = message.data
      bookChange = {
        type: 'book_change',
        symbol: message.symbol!,
        exchange: this.exchange,
        isSnapshot: true,
        bids: data.bid.map(mapBookLevel),
        asks: data.ask.map(mapBookLevel),
        timestamp: message.timestamp!,
        localTimestamp
      }
    } else {
      const data: BittrexOrderChange = decodeMessage(message)
      bookChange = {
        type: 'book_change',
        symbol: data.marketSymbol,
        exchange: this.exchange,
        isSnapshot: false,
        bids: data.bidDeltas.map(mapBookLevel),
        asks: data.askDeltas.map(mapBookLevel),
        timestamp: localTimestamp,
        localTimestamp
      }
    }
    yield bookChange
  }
}

function mapBookLevel(level: BittrexBookLevel) {
  const amount = Number(level.quantity)
  const price = Number(level.rate)
  return { price, amount }
}

function decodeMessage(message: BittrexMessageType) {
  return JSON.parse(inflateRawSync(Buffer.from(message.M.flatMap((m) => m.A[0])[0], 'base64')).toString())
}

type BittrexMessageType = {
  M: { A: string[] }[]
  stream?: string
  data?: BittrexDataSnapshotType
  symbol?: string
  timestamp?: Date
}

type BittrexDataSnapshotType = {
  ask: BittrexBookLevel[]
  bid: BittrexBookLevel[]
}
type BittrexBookLevel = {
  quantity: number
  rate: number
}

type BittrexOrderChange = {
  marketSymbol: string
  depth: number
  sequence: number
  bidDeltas: BittrexBookLevel[]
  askDeltas: BittrexBookLevel[]
}
