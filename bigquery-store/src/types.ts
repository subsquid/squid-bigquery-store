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

export type String = ReturnType<typeof String>

export function Numeric(precision: number, scale: number = 0): Type<number> {
    /**
     * Valid scale values are {0, 1, ..., 9}
     * Valid precision values are {max(1,scale), ..., scale+29}
     */
    assert(Number.isSafeInteger(scale) && scale >= 0 && scale <= 9, `Invalid scale ${scale} for NUMERIC`)
    assert(
        Number.isSafeInteger(precision) && (scale > 1 ? scale : 1) <= precision && precision <= scale + 29,
        `Invalid precision ${precision} for scale ${scale} for NUMERIC`
    )
    return {
        bqFullType: `NUMERIC(${precision}, ${scale})`,
        bqType: `NUMERIC`,
        serialize(value) {
            return value.toString()
        },
    }
}

export type Numeric = ReturnType<typeof Numeric>

export function BigNumeric<T extends number | bigint>(precision: number, scale: number = 0): Type<T> {
    /**
     * Valid scale values are {0, 1, ..., 38}
     * Valid precision values are {max(1,scale), ..., scale+38}
     */
    assert(Number.isSafeInteger(scale) && scale >= 0 && scale <= 38, `Invalid scale ${scale} for BIGNUMERIC`)
    assert(
        Number.isSafeInteger(precision) && (scale > 1 ? scale : 1) <= precision && precision <= scale + 38,
        `Invalid precision ${precision} for scale ${scale} for BIGNUMERIC`
    )
    return {
        bqFullType: `BIGNUMERIC(${precision}, ${scale})`,
        bqType: `BIGNUMERIC`,
        serialize(value) {
            return value.toString()
        },
    }
}

export type BigNumeric<T extends number | bigint = number | bigint> = ReturnType<typeof BigNumeric<T>>

export function Bool(): Type<boolean> {
    return {
        bqType: 'BOOL',
        serialize(value) {
            return value
        },
    }
}

export type Bool = ReturnType<typeof Bool>

export function Timestamp(): Type<Date> {
    return {
        bqType: 'TIMESTAMP',
        serialize(value) {
            return new BigQueryTimestamp(value)
        },
    }
}

export type Timestamp = ReturnType<typeof Timestamp>

export function Float64(): Type<number> {
    return {
        bqType: 'FLOAT64',
        serialize(value) {
            return value
        },
    }
}

export type Float64 = ReturnType<typeof Float64>

export function Int64(): Type<number | bigint> {
    return {
        bqType: 'INT64',
        serialize(value) {
            return new BigQueryInt(value.toString())
        },
    }
}

export type Int64 = ReturnType<typeof Int64>

/*
// Arrays are unusable since the INSERT query at
// database.ts relies on joining arrays and GoogleSQL
// prohibits arrays of arrays
export function Array<T>(itemType: Type<T>): Type<T[]> {
    return {
        bqType: `ARRAY<${itemType.bqType}>`,
        serialize(value) {
            return value.map((i) => itemType.serialize(i))
        },
    }
}
*/
