import * as ss58 from '@subsquid/ss58'
import {decodeHex, SubstrateBatchProcessor, SubstrateBlock} from '@subsquid/substrate-processor'
import {Database} from '@subsquid/bigquery-store'
import {BalancesTransferEvent} from './types/events'
import {Extrinsics, Transfers} from './tables'
import {BigQuery} from '@google-cloud/bigquery'

const processor = new SubstrateBatchProcessor()
    .setDataSource({
        archive: 'https://kusama.archive.subsquid.io/graphql',
    })
    .addEvent('Balances.Transfer', {
        data: {
            event: {
                args: true,
                extrinsic: {
                    hash: true,
                    call: {
                        origin: true,
                    },
                },
            },
        },
    } as const)

let bq = new BigQuery({
    projectId: `bright-meridian-316511`,
    credentials: {
        // your cridentials
    },
})

let db = new Database({
    tables: {
        Transfers,
        Extrinsics,
    },
    bq,
    dataset: 'bigquery_store'
})

processor.run(db, async (ctx) => {
    for (let block of ctx.blocks) {
        let prevExtrinsic: string | undefined
        for (let item of block.items) {
            if (item.name == 'Balances.Transfer') {
                let e = new BalancesTransferEvent(ctx, item.event)
                let rec: {from: Uint8Array; to: Uint8Array; amount: bigint}
                if (e.isV1020) {
                    let [from, to, amount] = e.asV1020
                    rec = {from, to, amount}
                } else if (e.isV1050) {
                    let [from, to, amount] = e.asV1050
                    rec = {from, to, amount}
                } else if (e.isV9130) {
                    rec = e.asV9130
                } else {
                    throw new Error('Unsupported spec')
                }

                ctx.store.Transfers.insert({
                    blockNumber: block.header.height,
                    timestamp: new Date(block.header.timestamp),
                    extrinsicHash: item.event.extrinsic?.hash,
                    from: ss58.codec('kusama').encode(rec.from),
                    to: ss58.codec('kusama').encode(rec.to),
                    amount: rec.amount,
                })

                if (item.event.extrinsic && prevExtrinsic != item.event.extrinsic.hash) {
                    let signer = getOriginAccountId(item.event.extrinsic.call.origin)

                    if (signer) {
                        ctx.store.Extrinsics.insert({
                            blockNumber: block.header.height,
                            timestamp: new Date(block.header.timestamp),
                            hash: item.event.extrinsic.hash,
                            signer: ss58.codec('kusama').encode(signer),
                        })
                    }
                }
            }
        }
    }
})

export function getOriginAccountId(origin: any) {
    if (origin && origin.__kind === 'system' && origin.value.__kind === 'Signed') {
        const id = origin.value.value
        if (id.__kind === 'Id') {
            return decodeHex(id.value)
        } else {
            return decodeHex(id)
        }
    } else {
        return undefined
    }
}
