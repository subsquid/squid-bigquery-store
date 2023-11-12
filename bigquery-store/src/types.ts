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
    /**
     * Valid scale values are {0, 1, ..., 9}
     * Valid precision values are {max(1,scale), ..., scale+29}
     */
    assert(Number.isSafeInteger(scale) && scale >= 0 && scale <= 9, `Invalid scale ${scale} for NUMERIC`)
    assert(Number.isSafeInteger(precision) && (scale>1?scale:1) <= precision && precision <= scale+29, `Invalid precision ${precision} for scale ${scale} for NUMERIC`)
    return {
        bqType: `NUMERIC(${precision}, ${scale})`,
        serialize(value) {
            return value.toString()
        },
    }
}

export function BigNumeric(precision: number, scale = 0): Type<number | bigint> {
    /**
     * Valid scale values are {0, 1, ..., 38}
     * Valid precision values are {max(1,scale), ..., scale+38}
     */
    assert(Number.isSafeInteger(scale) && scale >= 0 && scale <= 38, `Invalid scale ${scale} for BIGNUMERIC`)
    assert(Number.isSafeInteger(precision) && (scale>1?scale:1) <= precision && precision <= scale+38, `Invalid precision ${precision} for scale ${scale} for BIGNUMERIC`)
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
