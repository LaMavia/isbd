Sizes in bytes

Header:

1. 7 - number of columns
2. 7 - number of fragments
3. 4 - fragment length
4. ? - list of `[VLQ-encoded length][column name][column type]` of length `number of columns`. Column type, 1 byte:
   1. 0 - i64
   2. 1 - varchar
5.
