import { inflateRawSync } from 'zlib'
import { BookChange, Exchange } from '../types'
import { Mapper } from './mapper'

export class BittrexOrderChangeMapper implements Mapper<'bittrex', BookChange> {
  constructor(protected readonly exchange: Exchange) {}

  canHandle(message: any) {
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

  *map(message: any, localTimestamp: Date): IterableIterator<BookChange> {
    const data: BittrexOrderChange = decodeMessage(message)
    const bookChange: BookChange = {
      type: 'book_change',
      symbol: 'ETH',
      exchange: this.exchange,
      isSnapshot: false,
      bids: data.bidDeltas.map(mapBookLevel),
      asks: data.askDeltas.map(mapBookLevel),
      timestamp: localTimestamp,
      localTimestamp
    }
    console.log(bookChange)
    yield bookChange
  }
}

function mapBookLevel(level: BittrexBookLevel) {
  const amount = Number(level.quantity)
  const price = Number(level.rate)
  return { price, amount }
}

type BittrexBookLevel = { quantity: number; rate: number }

type BittrexOrderChange = {
  marketSymbol: string // 'ETH-USD'
  depth: number
  sequence: number
  bidDeltas: BittrexBookLevel[]
  askDeltas: BittrexBookLevel[]
}
//TODO: (@jpgonzalezra) refactor this method
function decodeMessage(message: any) {
  return message.M.map((books: { A: string[] }) =>
    books.A.map((book) => JSON.parse(inflateRawSync(Buffer.from(book, 'base64')).toString()))
  )[0][0]
}
