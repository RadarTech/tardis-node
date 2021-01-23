import { inflateRawSync } from 'zlib'
import { BookChange, Exchange, Trade } from '../types'
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
      const data: BittrexOrderChange = fetchAndParseMessage(message)
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

export class BittrexTradesMapper implements Mapper<'bittrex', Trade> {
  constructor(private readonly _exchange: Exchange) {}

  canHandle(message: any) {
    console.log(message)
    return false
  }

  getFilters(symbols?: string[]) {
    return [
      {
        channel: 'trade',
        symbols
      } as const
    ]
  }

  *map(message: any, localTimestamp: Date) {
    const data = message.data
    console.log(data)
    const trade: Trade = {
      type: 'trade',
      symbol: data.s,
      exchange: this._exchange,
      id: String(data.t),
      price: Number(data.p),
      amount: Number(data.q),
      side: data.m ? 'sell' : 'buy',
      timestamp: new Date(data.T),
      localTimestamp: localTimestamp
    }

    yield trade
  }
}

function mapBookLevel(level: BittrexBookLevel) {
  const amount = Number(level.quantity)
  const price = Number(level.rate)
  return { price, amount }
}

function fetchAndParseMessage(message: BittrexMessageType) {
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
