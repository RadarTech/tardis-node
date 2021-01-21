import got from 'got'
import { Writable } from 'stream'
import { batch } from '../handy'
import { Filter } from '../types'
import { MultiConnectionRealTimeFeedBase, PoolingClientBase, RealTimeFeedBase } from './realtimefeed'

abstract class BittrexRealTimeFeedBase extends MultiConnectionRealTimeFeedBase {
  protected abstract wssURL: string
  protected abstract httpURL: string
  protected abstract suffixes: { [key: string]: number }

  protected *_getRealTimeFeeds(exchange: string, filters: Filter<string>[], timeoutIntervalMS?: number, onError?: (error: Error) => void) {
    const wsFilters = filters.filter((f) => f.channel !== 'recentTrades')
    if (wsFilters.length > 0) {
      yield new BittrexSingleConnectionRealTimeFeed(
        exchange,
        wsFilters,
        this.wssURL,
        this.httpURL,
        this.suffixes,
        timeoutIntervalMS,
        onError
      )
    }
  }
}

class BittrexSingleConnectionRealTimeFeed extends RealTimeFeedBase {
  constructor(
    exchange: string,
    filters: Filter<string>[],
    protected wssURL: string,
    private readonly _httpURL: string,
    private readonly _suffixes: { [key: string]: number },
    timeoutIntervalMS: number | undefined,
    onError?: (error: Error) => void
  ) {
    super(exchange, filters, timeoutIntervalMS, onError)
  }

  protected mapToSubscribeMessages(filters: Filter<string>[]): any[] {
    const payload = filters
      .filter((f) => f.channel !== 'depthSnapshot')
      .map((filter, index) => {
        if (!filter.symbols || filter.symbols.length === 0) {
          throw new Error('BittrexRealTimeFeed requires explicitly specified symbols when subscribing to live feed')
        }

        return {
          H: 'c3',
          M: 'Subscribe',
          A: filter.symbols.map((symbol) => ['heartbeat', `ticker_${symbol.toUpperCase()}`]),
          I: index + 1
        }
      })
    console.log('payload:', JSON.stringify(payload))
    return payload
  }

  protected messageIsError(message: any): boolean {
    console.log(message)
    return false
  }

  protected async provideManualSnapshots(filters: Filter<string>[], shouldCancel: () => boolean) {
    console.log('filters', filters)
    const depthSnapshotFilter = filters.find((f) => f.channel === 'depthSnapshot')
    if (!depthSnapshotFilter) {
      return
    }
    this.debug('requesting manual snapshots for: %s', depthSnapshotFilter.symbols)
    for (let symbol of depthSnapshotFilter.symbols!) {
      if (shouldCancel()) {
        return
      }

      const orderBookResponse = await got
        .get(`${this._httpURL}/markets/${symbol.toUpperCase()}/orderbook?depth=500`, { timeout: 2000 })
        .json()

      const snapshot = {
        stream: `${symbol.toLocaleLowerCase()}@depthSnapshot`,
        generated: true,
        timestamp: new Date(),
        data: orderBookResponse
      }

      this.manualSnapshotsBuffer.push(snapshot)
    }

    this.debug('requested manual snapshots successfully for: %s ', depthSnapshotFilter.symbols)
  }
}
export class BittrexRealTimeFeed extends BittrexRealTimeFeedBase {
  protected wssURL = 'wss://socket-v3.bittrex.com/signalr'
  protected httpURL = 'https://api.bittrex.com/v3'

  protected suffixes = {
    depth: 500,
  }
}
