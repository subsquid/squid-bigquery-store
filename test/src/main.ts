import * as ss58 from '@subsquid/ss58'
import {Database} from '@subsquid/bigquery-store'
import {BigQuery} from '@google-cloud/bigquery'
import {assertNotNull} from '@subsquid/substrate-processor'
import {events} from './types'
import {Transfers} from './tables'
import {processor} from './processor'

let db = new Database({
    tables: {
        Transfers,
    },
    bq: new BigQuery(), // supply JSON creds via GOOGLE_APPLICATION_CREDENTIALS
    dataset: assertNotNull(process.env.BIGQUERY_DATASET)
})

processor.run(db, async (ctx) => {
    for (let block of ctx.blocks) {
        assertNotNull(block.header.timestamp, `Block ${block.header.height} arrived without a timestamp`)
        for (let event of block.events) {
            if (event.name == events.balances.transfer.name) {
                let rec: {from: string; to: string; amount: bigint}
                if (events.balances.transfer.v1020.is(event)) {
                    let [from, to, amount] = events.balances.transfer.v1020.decode(event)
                    rec = {from, to, amount}
                } else if (events.balances.transfer.v1050.is(event)) {
                    let [from, to, amount] = events.balances.transfer.v1050.decode(event)
                    rec = {from, to, amount}
                } else if (events.balances.transfer.v9130.is(event)) {
                    rec = events.balances.transfer.v9130.decode(event)
                } else {
                    throw new Error('Unsupported spec')
                }

                ctx.store.Transfers.insert({
                    blockNumber: block.header.height,
                    timestamp: new Date(block.header.timestamp!),
                    extrinsicHash: event.extrinsic?.hash,
                    from: ss58.codec('kusama').encode(rec.from),
                    to: ss58.codec('kusama').encode(rec.to),
                    amount: rec.amount,
                })
            }
        }
    }
})