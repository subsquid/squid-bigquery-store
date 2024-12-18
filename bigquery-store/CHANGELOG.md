# Change Log - @subsquid/bigquery-store

This log was last generated on Wed, 18 Dec 2024 22:57:16 GMT and should not be manually modified.

## 0.1.2
Wed, 18 Dec 2024 22:57:16 GMT

### Patches

- (1) added an optional project-wide session termination at startup; (2) now empty table updates won't cause a call to BQ, saving time on latency.

## 0.1.1
Mon, 13 Nov 2023 10:54:57 GMT

### Patches

- fix incorrect scale check condition

## 0.1.0
Thu, 16 Mar 2023 14:32:39 GMT

### Minor changes

- add BigNumeric type
- export type with namespace

### Patches

- fix boolean typescript type

