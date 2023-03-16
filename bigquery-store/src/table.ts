export interface Type<T> {
    bqType: string
    serialize(value: T): any
}

export interface ColumnOptions {
    nullable?: boolean
}

export interface ColumnData<
    T extends Type<any> = Type<any>,
    O extends Required<ColumnOptions> = Required<ColumnOptions>
> {
    type: T
    options: O
}

export interface Column {
    name: string
    data: ColumnData
}

export interface TableSchema {
    [column: string]: ColumnData
}

type NullableColumns<T extends Record<string, ColumnData>> = {
    [F in keyof T]: T[F] extends ColumnData<any, infer R> ? (R extends {nullable: true} ? F : never) : never
}[keyof T]

type ColumnsToTypes<T extends Record<string, ColumnData>> = {
    [F in Exclude<keyof T, NullableColumns<T>>]: T[F] extends ColumnData<Type<infer R>> ? R : never
} & {
    [F in Extract<keyof T, NullableColumns<T>>]?: T[F] extends ColumnData<Type<infer R>> ? R | null | undefined : never
}

export interface ITable<T extends Record<string, any>> {
    createWriter(): TableWriter<T>
}

export class Table<S extends TableSchema> {
    readonly columns: ReadonlyArray<Column>
    constructor(readonly name: string, schema: S) {
        let columns: Column[] = []
        for (let column in schema) {
            columns.push({
                name: column,
                data: schema[column],
            })
        }
        this.columns = columns
    }

    createWriter(): TableWriter<ColumnsToTypes<S>> {
        return new TableWriter(this.columns)
    }
}

export class TableWriter<T extends Record<string, any>> {
    private records: T[] = []

    constructor(private columns: ReadonlyArray<Column>) {}

    flush() {
        let res: Record<string, any[]> = {}

        for (let column of this.columns) {
            let values: string[] = []
            for (let record of this.records) {
                let value = record[column.name]
                values.push(value == null ? null : column.data.type.serialize(value))
            }

            res[column.name] = values
        }
        this.records = []
        return res
    }

    insert(record: T): this {
        this.records.push(record)
        return this
    }

    insertMany(records: T[]): this {
        this.records.push(...records)
        return this
    }
}

export type TableRecord<T extends Table<any>> = T extends Table<infer R> ? R : never

export function Column<T extends Type<any>>(type: T): ColumnData<T>
export function Column<T extends Type<any>, O extends ColumnOptions>(
    type: T,
    options?: O
): ColumnData<T, O & Required<ColumnOptions>>
export function Column<T extends Type<any>>(type: T, options?: ColumnOptions) {
    return {
        type,
        options: {nullable: false, ...options},
    }
}
