import {BigQueryInt, BigQueryTimestamp} from '@google-cloud/bigquery'
import assert from 'assert'
import {Type} from './table'

export function String(): Type<string> {
    return {
        bqType: 'STRING',
        serialize(value) {
            return value
        },
    }
}

export function Numeric(precision: number, scale = 0): Type<number | bigint> {
    assert(Number.isSafeInteger(precision) && precision > 0 && precision <= 38, 'Invalid precision')
    assert(Number.isSafeInteger(scale) && scale >= 0 && scale <= precision, 'Invalid scale')
    return {
        bqType: `NUMERIC(${precision}, ${scale})`,
        serialize(value) {
            return value.toString()
        },
    }
}

export function BigNumeric(precision: number, scale = 0): Type<number | bigint> {
    assert(Number.isSafeInteger(precision) && precision > 0 && precision <= 76, 'Invalid precision')
    assert(Number.isSafeInteger(scale) && scale >= 0 && scale <= precision, 'Invalid scale')
    return {
        bqType: `BIGNUMERIC(${precision}, ${scale})`,
        serialize(value) {
            return value.toString()
        },
    }
}

export function Bool(): Type<boolean> {
    return {
        bqType: 'BOOL',
        serialize(value) {
            return value
        },
    }
}

export function Timestamp(): Type<Date> {
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

export function Int64(): Type<number | bigint> {
    return {
        bqType: 'INT64',
        serialize(value) {
            return new BigQueryInt(value.toString())
        },
    }
}

export function Array<T>(itemType: Type<T>): Type<T[]> {
    return {
        bqType: `ARRAY<${itemType.bqType}>`,
        serialize(value) {
            return value.map((i) => itemType.serialize(i))
        },
    }
}
