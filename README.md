# statsaggjs
Aggregation of stats across tab-delimited JSON records

## Usage

Usage: ./statsaggjs [options] inputfiles ...

Options:

    -del string
        Alternate delimiter between key and JSON object (default "\t")
    -limit limit
        If more than limit unique keys are found, the data will be flushed to output and aggregation starts over. A limit of zero means no limit.
    -outfile string
        Output file (defaults to standard output)


## Description

The statsaggjs program aggregates counts in tab-delimited files
where the first column is a key and the rest of the line is a JSON
object. Numeric values are incremented in JSON objects for matching
keys.

For non-numeric scalar values, the last one wins, unless there are
numeric values for the same key, in which case the numeric values
are used and non-numeric values are ignored.

Nested maps are aggregated the same way as the top
level.

Nested slices are appended.

E.g., these records:

    foo[tab]{"chips": 1, "drinks": 1, "frugal": false}
    foo[tab]{"deep": {"level1": {"level2": 1}}}
    foo[tab]{"chips": 3, "frugal": true, "nested": {"count": 1}}
    foo[tab]{"deep": {"level1": {"level2": 2}}, "versions": ["1.2"]}
    foo[tab]{"nested": {"count": 3}, "versions": ["2.0"] }
    bar[tab]{"pizza": 2, "cheese": 3}

reduce to

    foo[tab]{"chips":4,"deep":{"level1":{"level2":3}},"drinks":1,"frugal":true,"nested":{"count":4},"versions":["1.2","2.0"]}
    bar[tab]{"cheese":3,"pizza":2}
