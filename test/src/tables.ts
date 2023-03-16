import {Table, Types, Column} from '@subsquid/bigquery-store'

export const Transfers = new Table('transfers', {
    blockNumber: Column(Types.Int64()),
    timestamp: Column(Types.Timestamp()),
    extrinsicHash: Column(Types.String(), {nullable: true}),
    from: Column(Types.String()),
    to: Column(Types.String()),
    amount: Column(Types.Int64()),
})

export const Extrinsics = new Table('extrinsics', {
    blockNumber: Column(Types.Int64()),
    timestamp: Column(Types.Timestamp()),
    hash: Column(Types.String()),
    signer: Column(Types.String()),
})
