import * as bq from '@google-cloud/bigquery'
import assert from 'assert'
import {BigQueryTransaction} from './client'
import {ITable, Table, TableWriter} from './table'
import {FinalDatabase, FinalTxInfo, HashAndHeight} from '@subsquid/util-internal-processor-tools'

type Tables = Record<string, Table<any>>

export interface DatabaseOptions<T extends Tables> {
    tables: T

    bq: bq.BigQuery

    dataset: string
}

type DataBuffer<T extends Tables> = {
    [k in keyof T]: TableWriter<T[k] extends ITable<infer R> ? R : never>
}

type ToStoreWriter<W extends TableWriter<any>> = Pick<W, 'insert' | 'insertMany'>

export type Store<T extends Tables> = Readonly<{
    [k in keyof T]: ToStoreWriter<DataBuffer<T>[k]>
}> & {
    flush(): Promise<void>
}

interface StoreConstructor<T extends Tables> { // ????
    new (tx: () => BigQueryTransaction): Store<T>
}

export class Database<T extends Tables> {
    private tables: T
    private dataset: string
    private bq: bq.BigQuery

    private StoreConstructor: StoreConstructor<T>

    private state?: HashAndHeight

    constructor(options: DatabaseOptions<T>) {
        this.tables = options.tables
        this.dataset = options.dataset || 'squid'
        this.bq = options.bq

        class Store extends BaseStore {
            protected tables: Tables
            protected dataset: string
            protected chunk: DataBuffer<T>

            constructor(protected tx: () => BigQueryTransaction) {
                super(tx)
                this.dataset = options.dataset
                this.tables = options.tables
                this.chunk = this.createChunk()
            }

            private createChunk(): DataBuffer<T> {
                let chunk: DataBuffer<any> = {}
                for (let name in this.tables) {
                    chunk[name] = this.tables[name].createWriter()
                }
                return chunk
            }
        }
        for (let name in this.tables) {
            Object.defineProperty(Store.prototype, name, {
                get(this: Store) {
                    this.tx()
                    return this.chunk[name]
                },
            })
        }
        this.StoreConstructor = Store as any
    }

    async connect(): Promise<HashAndHeight> {
        for (let tableAlias in this.tables) {
            let table = this.tables[tableAlias]
            await this.bq.query(
                `CREATE TABLE IF NOT EXISTS ${this.dataset}.${table.name} (${table.columns
                    .map((c) => `\`${c.name}\` ${c.data.type.bqType}` + (c.data.options.nullable ? '' : ` NOT NULL`))
                    .join(`, `)})`
            )
        }

        await this.bq.query(`CREATE TABLE IF NOT EXISTS ${this.dataset}.status (height int not null, blockHash string not null)`)

        let status = await this.bq
            .query(`SELECT height, blockHash FROM ${this.dataset}.status`)
            .then(rows => rows[0])

        if (status.length == 0) { // BQ returned [[]]
            await this.bq.query(`INSERT INTO ${this.dataset}.status (height, blockHash) VALUES (-1, "0x")`)
            this.state = {hash: '0x', height: -1}
        } else { // BQ returned [[{height, blockHash}]]
            this.state = {height: status[0].height, hash: status[0].blockHash}
        }

        return this.state
    }

    async transact(info: FinalTxInfo, cb: (store: Store<T>) => Promise<void>): Promise<void> {
        let tx = await BigQueryTransaction.create(this.bq)
        let open = true

        let store = new this.StoreConstructor(() => {
            assert(open, `Transaction was already closed`)
            return tx
        })

        try {
            await cb(store)
            await store.flush()
        } catch (e: any) {
            open = false
            await tx.rollback().catch()
            throw e
        }

        let from = info.prevHead.height
        let to = info.nextHead.height
        let hash = info.nextHead.hash
        await tx.query(`UPDATE ${this.dataset}.status SET height = @to, blockHash = @hash WHERE height < @from`, {from, to, hash})
        await tx.commit()

        open = false
    }
}

abstract class BaseStore {
    protected abstract dataset: string
    protected abstract tables: Tables
    protected abstract chunk: DataBuffer<any>

    constructor(protected tx: () => BigQueryTransaction) {}

    async flush() {
        for (let tableAlias in this.tables) {
            let data = this.chunk[tableAlias].flush()
            let table = this.tables[tableAlias]

            let fields = table.columns.map((c) => `\`${c.name}\``)
            let params = table.columns.map((c) => `@${c.name}`)
            let types: Record<string, any> = {}
            for (let column of table.columns) {
                types[column.name] = [column.data.type.bqType]
            }

            await this.tx().query(
                `INSERT INTO ${this.dataset}.${this.tables[tableAlias].name} (${fields.join(
                    ', '
                )}) SELECT ${fields.join(', ')} FROM ${params
                    .map((p, i) =>
                        i == 0
                            ? `UNNEST(${p}) as ${fields[i]} WITH OFFSET`
                            : `JOIN UNNEST(${p}) as ${fields[i]} WITH OFFSET USING (OFFSET)`
                    )
                    .join(' ')}`,
                data,
                types
            )
        }
    }
}
