# dstatsaggjson
Aggregation of stats across tab-delimited JSON records

Alpha release -- nested structures not supported.

A better README will come later.

## description

The dstatsaggjson program aggregates counts in tab-delimited files
where the first column is a key and the rest of the line is a JSON
object. Numeric values are incremented in JSON objects for matching
keys. For non-numeric values, the last one wins,unless there are
numeric values for the same key, in which case the numeric values are
used and non-numeric values are ignored.

E.g., these records:

    foo[tab]{"chips": 1, "drinks": 1, "frugal": false}
    bar[tab]{"pizza": 2, "cheese": 3}
    foo[tab]{"chips": 3, "frugal": true}

reduce to

    foo[tab]{"chips": 4, "drinks": 1, "frugal": true}
    bar[tab]{"pizza": 2, "cheese": 3}
