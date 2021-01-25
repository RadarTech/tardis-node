import { inflateRawSync } from 'zlib'
import { BookChange, Exchange, Trade } from '../types'
import { Mapper } from './mapper'

export class BittrexOrderChangeMapper implements Mapper<'bittrex', BookChange> {
  constructor(protected readonly exchange: Exchange) {}

  canHandle(message: BittrexMessageType) {
    return isValid(message, 'orderBook') || message.stream === 'depthSnapshot'
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
        bids: data.bid.map(this.mapBookLevel),
        asks: data.ask.map(this.mapBookLevel),
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
        bids: data.bidDeltas.map(this.mapBookLevel),
        asks: data.askDeltas.map(this.mapBookLevel),
        timestamp: localTimestamp,
        localTimestamp
      }
    }
    yield bookChange
  }

  private mapBookLevel(level: BittrexBookLevel) {
    const amount = Number(level.quantity)
    const price = Number(level.rate)
    return { price, amount }
  }
}

export class BittrexTradesMapper implements Mapper<'bittrex', Trade> {
  constructor(private readonly exchange: Exchange) {}

  canHandle(message: BittrexMessageType) {
    return isValid(message, 'trade')
  }

  getFilters(symbols?: string[]) {
    return [
      {
        channel: 'trade',
        symbols
      } as const
    ]
  }

  *map(message: any, localTimestamp: Date): IterableIterator<Trade> {
    const bittrexTradeResponse: BittrexTradeResponse = fetchAndParseMessage(message)
    for (const bittrexTrade of bittrexTradeResponse.deltas) {
      const trade: Trade = {
        type: 'trade',
        symbol: bittrexTradeResponse.marketSymbol,
        exchange: this.exchange,
        id: bittrexTrade.id,
        price: Number(bittrexTrade.rate),
        amount: Number(bittrexTrade.quantity),
        side: bittrexTrade.takerSide === 'BUY' ? 'buy' : 'sell',
        timestamp: new Date(bittrexTrade.executedAt),
        localTimestamp: localTimestamp
      }
      yield trade
    }
  }
}

function fetchAndParseMessage(message: BittrexMessageType) {
  const raw = inflateRawSync(Buffer.from(message.M.flatMap((m) => m.A[0])[0], 'base64')).toString()
  return JSON.parse(raw)
}

function isValid(message: BittrexMessageType, type: string): boolean {
  return (
    Array.isArray(message.M) &&
    message.M.length > 0 &&
    message.M[0].M === type &&
    Array.isArray(message.M[0].A) &&
    message.M[0].A.length > 0
  )
}

type BittrexTradeResponse = {
  deltas: BittrexTrade[]
  sequence: number
  marketSymbol: string
}

type BittrexTrade = {
  id: string
  executedAt: string
  quantity: string
  rate: string
  takerSide: string
}
type BittrexMessageType = {
  G: string
  M: { A: string[]; M: string }[]
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
