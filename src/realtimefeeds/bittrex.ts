import got from 'got'
import { Writable } from 'stream'
import { batch } from '../handy'
import { Filter } from '../types'
import { MultiConnectionRealTimeFeedBase, PoolingClientBase } from './realtimefeed'

abstract class BittrexRealTimeFeedBase extends MultiConnectionRealTimeFeedBase {
  protected abstract httpURL: string
  protected abstract suffixes: { [key: string]: number }

  protected *_getRealTimeFeeds(exchange: string, filters: Filter<string>[], _?: number, _onError?: (error: Error) => void) {
    const orderBookFilters = filters.filter((f) => f.channel === 'orderBook')
    if (orderBookFilters.length > 0) {
      const instruments = orderBookFilters.flatMap((s) => s.symbols!)
      yield new BittrexOrderBookClient(exchange, this.httpURL, instruments, this.suffixes)
    }
  }
}

class BittrexOrderBookClient extends PoolingClientBase {
  protected readonly _context: { [key: string]: number }

  constructor(
    exchange: string,
    private readonly _httpURL: string,
    private readonly _instruments: string[],
    context: { [key: string]: number }
  ) {
    super(exchange, context.poolingIntervalSeconds || 30)
    this._context = context
  }

  protected async poolDataToStream(outputStream: Writable) {
    for (const instruments of batch(this._instruments, 10)) {
      await Promise.all(
        instruments.map(async (instrument) => {
          if (outputStream.destroyed) {
            return
          }

          const orderBookResponse = (await got
            .get(`${this._httpURL}/markets/${instrument.toUpperCase()}/orderbook?depth=${this._context.depth}`, { timeout: 2000 })
            .json()) as any

          const orderBookMessage = {
            stream: `${instrument.toLocaleLowerCase()}@orderBook`,
            generated: true,
            data: orderBookResponse
          }

          if (outputStream.writable) {
            outputStream.write(orderBookMessage)
          }
        })
      )
    }
  }
}

export class BittrexRealTimeFeed extends BittrexRealTimeFeedBase {
  protected httpURL = 'https://api.bittrex.com/v3'

  protected suffixes = {
    depth: 500,
    poolingIntervalSeconds: 30
  }
}