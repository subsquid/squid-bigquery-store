import {SubstrateBatchProcessor} from '@subsquid/substrate-processor'
import {events} from './types'

export const processor = new SubstrateBatchProcessor()
    .setDataSource({
        archive: 'https://v2.archive.subsquid.io/network/kusama',
        chain: {
            url: 'https://kusama-rpc.polkadot.io',
            rateLimit: 10
        }
    })
    .addEvent({
        name: [events.balances.transfer.name],
        extrinsic: true,
        call: true
    })
    .setFields({
        block: {
            timestamp: true
        },
        extrinsic: {
            hash: true
        }
    })
    .setBlockRange({ from: 1000000 })