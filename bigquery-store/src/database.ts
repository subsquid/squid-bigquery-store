import * as bq from '@google-cloud/bigquery'
import assert from 'assert'
import {BigQueryTransaction} from './client'
import {ITable, Table, TableWriter} from './table'

type Tables = Record<string, Table<any>>

export interface DatabaseOptions<T extends Tables> {
    tables: T

    bq: bq.BigQuery

    dataset: string
}

type Chunk<T extends Tables> = {
    [k in keyof T]: TableWriter<T[k] extends ITable<infer R> ? R : never>
}

type ToStoreWriter<W extends TableWriter<any>> = Pick<W, 'insert' | 'insertMany'>

export type Store<T extends Tables> = {
    readonly [k in keyof T]: ToStoreWriter<Chunk<T>[k]>
} & {
    flush(): Promise<void>
}

interface StoreConstructor<T extends Tables> {
    new (tx: () => BigQueryTransaction): Store<T>
}

export class Database<T extends Tables> {
    protected tables: T
    protected dataset: string
    protected bq: bq.BigQuery

    protected lastCommitted?: number

    protected StoreConstructor: StoreConstructor<T>

    constructor(options: DatabaseOptions<T>) {
        this.tables = options.tables
        this.dataset = options.dataset || 'squid'
        this.bq = options.bq

        class Store extends BaseStore {
            protected tables: Tables
            protected dataset: string
            protected chunk: Chunk<T>

            constructor(protected tx: () => BigQueryTransaction) {
                super(tx)
                this.dataset = options.dataset
                this.tables = options.tables
                this.chunk = this.createChunk()
            }

            protected createChunk(): Chunk<T> {
                let chunk: Chunk<any> = {}
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

    async connect(): Promise<number> {
        for (let tableAlias in this.tables) {
            let table = this.tables[tableAlias]
            await this.bq.query(
                `CREATE TABLE IF NOT EXISTS ${this.dataset}.${table.name} (${table.columns
                    .map((c) => `\`${c.name}\` ${c.data.type.bqType}` + (c.data.options.nullable ? '' : ` NOT NULL`))
                    .join(`, `)})`
            )
        }

        await this.bq.query(`CREATE TABLE IF NOT EXISTS ${this.dataset}.status (height int not null)`)
        let status: {height: number}[] = await this.bq
            .query(`SELECT height FROM ${this.dataset}.status`)
            .then((r) => r[0])
        if (status.length == 0) {
            await this.bq.query(`INSERT INTO ${this.dataset}.status (height) VALUES (-1)`)
            this.lastCommitted = -1
        } else {
            this.lastCommitted = status[0].height
        }

        return this.lastCommitted
    }

    async transact(from: number, to: number, cb: (store: Store<T>) => Promise<void>): Promise<void> {
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

        await tx.query(`UPDATE ${this.dataset}.status SET height = @to WHERE height < @from`, {from, to})
        await tx.commit()

        open = false
    }

    async advance(height: number): Promise<void> {
        if (this.lastCommitted == height) return
        let tx = await BigQueryTransaction.create(this.bq)
        await tx.query(`UPDATE ${this.dataset}.status SET height = @height WHERE height < @height`, {height})
        await tx.commit()
    }
}

abstract class BaseStore {
    protected abstract dataset: string
    protected abstract tables: Tables
    protected abstract chunk: Chunk<any>

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
