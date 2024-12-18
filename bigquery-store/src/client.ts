import * as bq from '@google-cloud/bigquery'
import {createLogger} from '@subsquid/logger'

const logger = createLogger('sqd:bigquery-store')

export class BigQueryTransaction {
    private constructor(private bq: bq.BigQuery, readonly sessionId: string) {}

    static async create(bq: bq.BigQuery) {
        let [job] = await bq.createQueryJob({query: `BEGIN TRANSACTION`, createSession: true})
        let data = await job.getMetadata()
        let sessionId = data[0].statistics.sessionInfo.sessionId
        return new BigQueryTransaction(bq, sessionId)
    }

    query(query: string, params?: Record<string, any>, types?: Record<string, any>) {
        logger.info(query)
        return this.bq
            .query({
                query,
                params,
                types,
                connectionProperties: [
                    {
                        key: 'session_id',
                        value: this.sessionId,
                    },
                ],
                useLegacySql: false,
            })
            .then((r) => r[0])
    }

    async commit() {
        await this.bq.query({
            query: 'COMMIT TRANSACTION',
            connectionProperties: [
                {
                    key: 'session_id',
                    value: this.sessionId,
                },
            ],
            useLegacySql: false,
        })
        return await this.terminate()
    }

    async rollback() {
        await this.bq.query({
            query: 'ROLLBACK TRANSACTION',
            connectionProperties: [
                {
                    key: 'session_id',
                    value: this.sessionId,
                },
            ],
            useLegacySql: false,
        })
        return await this.terminate()
    }

    private terminate() {
        return this.bq.query(`CALL BQ.ABORT_SESSION('${this.sessionId}')`)
    }
}
