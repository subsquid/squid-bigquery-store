import {Table, StringType, Column, TimestampType, Int64} from '@subsquid/bigquery-store'

export const Transfers = new Table('transfers', {
    blockNumber: Column(Int64()),
    timestamp: Column(TimestampType()),
    extrinsicHash: Column(StringType(), {nullable: true}),
    from: Column(StringType()),
    to: Column(StringType()),
    amount: Column(Int64()),
})

export const Extrinsics = new Table('extrinsics', {
    blockNumber: Column(Int64()),
    timestamp: Column(TimestampType()),
    hash: Column(StringType()),
    signer: Column(StringType()),
})
