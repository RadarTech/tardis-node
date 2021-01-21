import { BookChange, Exchange } from '../types'
import { Mapper } from './mapper'

export class BittrexBookChangeMapper implements Mapper<'bittrex', BookChange> {
  constructor(protected readonly exchange: Exchange) {}

  protected mapBookLevel(level: BittrexBookLevel) {
    const amount = Number(level.quantity) // quantity
    const price = Number(level.rate) // rate
    return { price, amount }
  }

  protected lowerCaseSymbols(symbols?: string[]) {
    if (symbols !== undefined) {
      return symbols.map((s) => s.toLowerCase())
    }
    return
  }

  protected getSymbolFromMessage(message: any) {
    return message.stream.split('@')[0].toUpperCase()
  }

  canHandle(message: any) {
    if (message.stream === undefined) {
      return false
    }

    return message.stream.includes('@orderBook')
  }

  getFilters(symbols?: string[]) {
    symbols = this.lowerCaseSymbols(symbols)

    return [
      {
        channel: 'orderBook',
        symbols
      } as const
    ]
  }

  *map(message: BittrexOrderBook, localTimestamp: Date): IterableIterator<BookChange> {
    const symbol = this.getSymbolFromMessage(message)
    const data = message.data

    const bookChange: BookChange = {
      type: 'book_change',
      symbol,
      exchange: this.exchange,
      isSnapshot: true,
      bids: data.bid.map(this.mapBookLevel),
      asks: data.ask.map(this.mapBookLevel),
      timestamp: localTimestamp,
      localTimestamp
    }

    yield bookChange
  }
}

type BittrexOrderBook = {
  channel: 'orderbook'
  stream: string
  data: { bid: BittrexBookLevel[]; ask: BittrexBookLevel[] }
}

type BittrexBookLevel = { quantity: number; rate: number }
