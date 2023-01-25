// import {BigQuery, Dataset} from '@google-cloud/bigquery'
// import {BigQueryTransaction} from './client'

// let bq = new BigQuery({
//     projectId: `bright-meridian-316511`,
//     credentials: {
//         private_key: `-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCx8Tfo5P6zcdWm\nf+7qDkDG9AaupTkUbPAwp00WosFar9rM8YrBcVIZMWSPa0we8SAPled2Go9CQzTx\nNTD2DhFk/mI1Mwf+x+a4mKNEy5jWVWFrqB6PvA1cmlfkUJuKl4YnZaIWLLtBEmwX\n9fD29wlXsfDkKKgTWAYXcn/d+9QokLlWePZpcHVX28pwiwxJFO2Kp8yWgXk8m3ct\nqTLNezaKSY7QzMYpMC77kW/2TirH275Ii+3xIRh6rzP/GupWGXGoNNBdyeSu0pSf\n4a1S9u3jPlq/6ABHNKUdiaynWhf4H0rOTKxm2NfMa3fHyea0hVSyfvhpYpAGBjfh\nfDq3msLvAgMBAAECggEAFoWohPLPBhlnaF6S9878BfKHEf3Znqs3L4FNcYsCHB9b\n+0qrPFbChGvLBZgYF2Z/n1li5YDxFvr4rXsFCm+3ZegIuCEQZwAYRM8VAzUd0CsY\nXI7350tvCYSDkWNefIk3Mq9bd6ELxm21fsbjS+7yoMXl7ory0xf2FWupoYgpT0Tk\nC467gTA0CC08VZXQFEVJ/Z+Hb9dRYSRW61RdhPpiQFhkVoH/SQBdQD22Ox35QKg0\ntgSMSfx0D1gwmCt7I+o6uO4rHe8vMtu2J21mOhQr0Vla/mzleuLULImlD+uAslM9\nFeSdChnswCOwvodgwsKPvctSzoq5ZBf4WayqRR8RqQKBgQDfFfNbiMYgKCcdL5U1\ndN03U/ywfRZYPzvY6YLMFdZ+8ydWZHaNue/gLlA1KAFzGiIopwCo4Kq0ZVb4MYUK\nnx5Qe4hv3jZ/f2IEX3eTaAEmO9a9MN6plQX57n8GMj6H/ccf9uZ1hu67zgWocds8\nNZS6XCGQjKBTPaLFHEQGrczCPQKBgQDMMi2Ny2kVcdWu7QLfj3IgGol/zYlV/xRf\n91NnE70PZqkY0tmTStBlAPMiZjoQwfTMGHJ6aH0eEFZaWGZ12JQH7KtDs3IHhJcz\nyO5DSfjG1eClQAiCICJKiyuPucITFbiRRKjwpEJo9iTZW+pt0SVsT6pzEcNhozgy\nbyUh3gBImwKBgQCICF+UEmwWEcYAIxLOPQvkAB/XEv+8IhsBYyrx/eMFGIqFQM+W\nDqq6PiOEtndj06y+s09Qq2cMh7snrzKcTnjyxNFmvc7noiqH1hsZVNNWmCiSxykP\nqaKyS/9DLQI6dMKmuCzBv9z1wmRq2brBFT8zAJkrBsWA3NXTci/9DqAMVQKBgEW+\n6eJh1R3XCQa7u9yCkZe3mLmHid5OxzXUM6+khVIqXZ21/00ZI78sKN/aDQFGTogb\n3ZZD9GB9chFf2ndsJ3vhccopE8zPlBnDCub+8DNyQE4RZhaURUIy8QkhiNGd/LHZ\nwt6XLHvPf1yi0Zr68g5h07WEHrlN19caMZO00WH7AoGAMQXBbWKlJIcb1d8CuAwe\nHjqTOR4Mfe0kcdu2G+6S4ROm0VDhYFht8c3sXngVWqR2a57tBz9AbcDLhKyXjgOK\n7NQ+X575HDZPuAakqiljYDK2BwHerhFt0ssvk5QMWeD8axEX61TrpatWEU6SnYb0\neZg54AEtdlD8R0DHX5sykps=\n-----END PRIVATE KEY-----\n`,
//         token_url: `https://oauth2.googleapis.com/token`,
//         client_email: `big-query-writer@bright-meridian-316511.iam.gserviceaccount.com`,
//         client_id: `100340509039605180296`,
//     },
// })

// async function test() {
//     let dataset: Dataset = await bq
//         .dataset(`bigquery_store`)
//         .get({autoCreate: true})
//         .then((r) => r[0])

//     await dataset.query(`CREATE TABLE IF NOT EXISTS test3 (foo STRING, bar STRING, test STRING)`)
//     // dataset.
//     // await session.(`BEGIN TRANSACTION`)
//     // session.
//     // let [job] = await dataset.createQueryJob({query: `BEGIN TRANSACTION`, createSession: true})
//     // let data = await job.getMetadata()
//     // let sessionId = data[0].statistics.sessionInfo.sessionId
//     let tx = await BigQueryTransaction.create(bq)

//     await tx
//         .query(
//             `INSERT INTO bigquery_store.test3 (foo, bar, test) SELECT foo, bar, test FROM 
//                 UNNEST(@foo) as foo WITH OFFSET
//                 JOIN UNNEST(@bar) as bar WITH OFFSET USING(OFFSET)
//                 JOIN UNNEST(@test) as test WITH OFFSET USING(OFFSET)
//     `,
//             {foo: ['foo', 'foo'], bar: ['bar', 'bar'], test: ['test', 'test']}
//         )
//         .then(console.log)
//     await tx.commit()

//     // await bq.query(`SELECT * FROM bigquery_store.test`).then(console.log)
//     // await dataset.query({query: `INSERT test VALUES ('aaa')`, connectionProperties: [{key: 'sessionId', value: a.statistics.sessionInfo.sessionId}]})
//     // await dataset.query({query: `ROLLBACK TRANSACTION`, connectionProperties: [{key: 'sessionId', value: a.statistics.sessionInfo.sessionId}]})
//     // await dataset.createQueryJob({query: `ROLLBACK TRANSACTION`, job: job})
//     // await job.getQueryResults().then(console.log)
//     // let b = await a.createDataset(`dapp_staking_astar`, {}).then(r => {
//     //     console.dir(r, {depth: 5})
//     //     return r[0]
//     // })
//     // await b.getTables().then(console.dir)
// }

// test()

export * from './database'
export * from './table'
export * from './types'