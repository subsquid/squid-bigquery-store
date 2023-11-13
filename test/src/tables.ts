import {Table, Types, Column} from '@subsquid/bigquery-store'

export const Transfers = new Table('transfers', {
    blockNumber: Column(Types.Int64()),
    timestamp: Column(Types.Timestamp()),
    extrinsicHash: Column(Types.String(), {nullable: true}),
    from: Column(Types.String()),
    to: Column(Types.String()),
    amount: Column(Types.BigNumeric(38)),
})