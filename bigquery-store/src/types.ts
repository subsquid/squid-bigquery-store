import {BigQueryInt, BigQueryTimestamp} from '@google-cloud/bigquery'
import assert from 'assert'

export interface Type<T> {
    bqType: string
    serialize(value: T): any
}

export function StringType(): Type<string> {
    return {
        bqType: 'STRING',
        serialize(value) {
            return value
        },
    }
}

export function NumericType(precision: number, scale = 0): Type<bigint> {
    assert(Number.isSafeInteger(precision) && precision > 0 && precision <= 38, 'Invalid precision')
    assert(Number.isSafeInteger(scale) && scale > 0 && scale <= precision, 'Invalid scale')
    return {
        bqType: `NUMERIC(${precision}, ${scale})`,
        serialize(value) {
            return value.toString()
        },
    }
}

export function BigNumericType(precision: number, scale = 0): Type<bigint> {
    assert(Number.isSafeInteger(precision) && precision > 0 && precision <= 76, 'Invalid precision')
    assert(Number.isSafeInteger(scale) && scale > 0 && scale <= precision, 'Invalid scale')
    return {
        bqType: `BIGNUMERIC(${precision}, ${scale})`,
        serialize(value) {
            return value.toString()
        },
    }
}

export function BoolType(): Type<bigint> {
    return {
        bqType: 'BOOL',
        serialize(value) {
            return value
        },
    }
}

export function TimestampType(): Type<Date> {
    return {
        bqType: 'TIMESTAMP',
        serialize(value) {
            return new BigQueryTimestamp(value)
        },
    }
}

export function Float64(): Type<number> {
    return {
        bqType: 'FLOAT64',
        serialize(value) {
            return value
        },
    }
}

export function Int64<T extends number | bigint>(): Type<T> {
    return {
        bqType: 'INT64',
        serialize(value) {
            return new BigQueryInt(value.toString())
        },
    }
}

export function ArrayType<T>(itemType: Type<T>): Type<T[]> {
    return {
        bqType: `ARRAY<${itemType.bqType}>`,
        serialize(value) {
            return value.map((i) => itemType.serialize(i))
        },
    }
}
