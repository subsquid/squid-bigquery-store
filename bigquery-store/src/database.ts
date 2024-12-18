import * as bq from '@google-cloud/bigquery'
import assert from 'assert'
import {BigQueryTransaction} from './client'
import {ITable, Table, TableWriter} from './table'
import {FinalDatabase, FinalTxInfo, HashAndHeight} from '@subsquid/util-internal-processor-tools'
import {createLogger} from '@subsquid/logger'

type Tables = Record<string, Table<any>>

export interface DatabaseOptions<T extends Tables> {
    tables: T

    bq: bq.BigQuery

    dataset: string

    /**
     * WARNING: can lead to data loss. Only enable when you're certain that
     * your squid is the only app that is using sessions to access your BigQuery
     * project.
     *
     * At the indexer startup, abort all
     * {@link https://cloud.google.com/bigquery/docs/sessions-intro | sessions}
     * that are active for your project.
     *
     * If you enable this you also have to specify {@link datasetRegion}.
     */
    abortAllProjectSessionsOnStartup?: boolean
    /**
     * Dataset region string such as "region-us". Only used when
     * {@link abortAllProjectSessionsOnStartup} is enabled.
     */
    datasetRegion?: string
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

interface StoreConstructor<T extends Tables> {
    new (tx: () => BigQueryTransaction): Store<T>
}

const logger = createLogger('sqd:bigquery-store')
const statusUpdateConcurrencyError =
`Your squid likely restarted after being terminated at the status update,
leaving a dangling session. You have two options:

1. If you are not sure if your squid is the only app that uses sessions
   (https://cloud.google.com/bigquery/docs/sessions-intro) to access your
   BigQuery project, find the faulty session manually and terminate it. See
   https://cloud.google.com/bigquery/docs/sessions-get-ids#list_active
   https://cloud.google.com/bigquery/docs/sessions-terminating#terminate_a_session_by_id

2. DANGEROUS If you are absolutely certain that the squid is the only app
   that uses sessions to access your BigQuery project, you can terminate
   all the dangling sessions by running

   FOR session in (
     SELECT
       session_id,
       MAX(creation_time) AS last_modified_time,
     FROM \`region-us\`.INFORMATION_SCHEMA.SESSIONS_BY_PROJECT
     WHERE
       session_id IS NOT NULL
       AND is_active
     GROUP BY session_id
     ORDER BY last_modified_time DESC
   )
   DO
     CALL BQ.ABORT_SESSION(session.session_id);
   END FOR;

   You can also enable the abortAllProjectSessionsOnStartup and supply
   datasetRegion in your database config to perform this operation at startup.

All available details will be listed below.
`

export class Database<T extends Tables> {
    private tables: T
    private dataset: string
    private bq: bq.BigQuery
    private abortAllProjectSessionsOnStartup: boolean
    private datasetRegion: string | undefined

    private StoreConstructor: StoreConstructor<T>

    private state?: HashAndHeight

    constructor(options: DatabaseOptions<T>) {
        this.tables = options.tables
        this.dataset = options.dataset || 'squid'
        this.bq = options.bq
        this.abortAllProjectSessionsOnStartup = options.abortAllProjectSessionsOnStartup ?? false
        if (this.abortAllProjectSessionsOnStartup) {
            if (options.datasetRegion == null) {
                logger.fatal('You must specify dataset region if you enable abortAllProjectSessionsOnStartup')
                process.exit(1)
            }
            this.datasetRegion = options.datasetRegion
        }

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
            if (this.abortAllProjectSessionsOnStartup) {
                logger.info(`Aborting all project sessions...`)
                await this.bq.query(
                    `FOR session in (
                        SELECT
                            session_id,
                            MAX(creation_time) AS last_modified_time,
                        FROM \`${this.datasetRegion}\`.INFORMATION_SCHEMA.SESSIONS_BY_PROJECT
                        WHERE
                            session_id IS NOT NULL
                            AND is_active
                        GROUP BY session_id
                        ORDER BY last_modified_time DESC
                    )
                    DO
                        CALL BQ.ABORT_SESSION(session.session_id);
                    END FOR;`
                )
            }
            await this.bq.query(
                `CREATE TABLE IF NOT EXISTS ${this.dataset}.${table.name} (${table.columns
                    .map(
                        (c) =>
                            `\`${c.name}\` ${c.data.type.bqFullType || c.data.type.bqType}` +
                            (c.data.options.nullable ? '' : ` NOT NULL`)
                    )
                    .join(`, `)})`
            )
        }

        await this.bq.query(
            `CREATE TABLE IF NOT EXISTS ${this.dataset}.status (height int not null, blockHash string not null)`
        )

        let status = await this.bq.query(`SELECT height, blockHash FROM ${this.dataset}.status`).then((rows) => rows[0])

        if (status.length == 0) {
            // BQ returned [[]]
            await this.bq.query(`INSERT INTO ${this.dataset}.status (height, blockHash) VALUES (-1, "0x")`)
            this.state = {hash: '0x', height: -1}
        } else {
            // BQ returned [[{height, blockHash}]]
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
        try {
            await tx.query(`UPDATE ${this.dataset}.status SET height = @to, blockHash = @hash WHERE height < @from`, {
                from,
                to,
                hash,
            })
        } catch (e: any) {
            if (e.message.startsWith(`Transaction is aborted due to concurrent update against table`)) {
                logger.error(statusUpdateConcurrencyError)
            }
            throw e
        }
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
            if (Object.entries(data).map(([_, v]) => v.length).every(l => l === 0)) {
                logger.info(`Skipping the update for table ${tableAlias} - no data`)
                continue
            }

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
