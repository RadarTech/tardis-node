import got from 'got'
import { wait } from '../handy'
import { Filter } from '../types'
import { MultiConnectionRealTimeFeedBase, RealTimeFeedBase } from './realtimefeed'

abstract class BittrexRealTimeFeedBase extends MultiConnectionRealTimeFeedBase {
  protected abstract wssURL: string
  protected abstract httpURL: string
  protected abstract suffixes: { [key: string]: number }

  protected *_getRealTimeFeeds(exchange: string, filters: Filter<string>[], timeoutIntervalMS?: number, onError?: (error: Error) => void) {
    const wsFilters = filters.filter((f) => f.channel !== 'depthSnapshot')
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

        const depth = this._suffixes['depth'] || 25

        const subscription = {
          H: 'c3',
          M: 'Subscribe',
          A: [filter.symbols.map((symbol) => `orderbook_${symbol.toUpperCase()}_${depth}`)],
          I: index + 1
        }
        return subscription
      })
    return payload
  }

  protected messageIsError(message: BittrexMessageErrorType): boolean {
    if (message.R) {
      for (const msg of message.R) {
        if (!msg.Success) {
          return true
        }
      }
    }
    return false
  }

  protected async getWssPath() {
    let wssPath = undefined
    while (!wssPath) {
      try {
        const data = JSON.stringify([{ name: 'c3' }])
        const negotiations: { ConnectionToken: string } = await got
          .get(`https://socket-v3.bittrex.com/signalr/negotiate?connectionData=${data}&clientProtocol=1.5`)
          .json()
        const token = encodeURIComponent(negotiations.ConnectionToken)
        wssPath = `${this.wssURL}/connect?clientProtocol=1.5&transport=webSockets&connectionToken=${token}&connectionData=${data}&tid=10`
      } catch (error) {
        await wait(this._timeoutIntervalMS || 1000)
      }
    }
    return wssPath
  }

  protected async provideManualSnapshots(filters: Filter<string>[], shouldCancel: () => boolean) {
    const depthSnapshotFilter = filters.find((f) => f.channel === 'depthSnapshot')
    if (!depthSnapshotFilter) {
      return
    }
    this.debug('requesting manual snapshots for: %s', depthSnapshotFilter.symbols)
    for (const symbol of depthSnapshotFilter.symbols!) {
      if (shouldCancel()) {
        return
      }

      const orderBookResponse = await got
        .get(`${this._httpURL}/markets/${symbol.toUpperCase()}/orderbook?depth=25`, { timeout: 2000 })
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
    depth: 500
  }
}

type BittrexMessageErrorType = { R: BittrexResponseStatusType[]; I: '1' }

type BittrexResponseStatusType = { Success: boolean; ErrorCode: string }
