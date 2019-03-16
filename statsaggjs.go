// Copyright (c) 2018-2019 Don Owens <don@regexguy.com>.  All rights reserved.
//
// This software is released under the BSD license:
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//
//  * Redistributions in binary form must reproduce the above
//    copyright notice, this list of conditions and the following
//    disclaimer in the documentation and/or other materials provided
//    with the distribution.
//
//  * Neither the name of the author nor the names of its
//    contributors may be used to endorse or promote products derived
//    from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
// FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
// COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
// OF THE POSSIBILITY OF SUCH DAMAGE.

// The statsaggjs program aggregates counts in tab-delimited files
// where the first column is a key and the rest of the line is a JSON
// object. Numeric values are incremented in JSON objects for matching
// keys.
// 
// For non-numeric scalar values, the last one wins, unless there are
// numeric values for the same key, in which case the numeric values
// are used and non-numeric values are ignored.
// 
// Nested maps are aggregated the same way as the top
// level.
//
// Nested slices are appended.
//
// E.g., these records:
//
//     foo[tab]{"chips": 1, "drinks": 1, "frugal": false}
//     foo[tab]{"deep": { "level1": { "level2": 1 } } }
//     foo[tab]{"chips": 3, "frugal": true, "nested": { "count": 1 } }
//     foo[tab]{ "deep": { "level1": { "level2": 2 } }, "versions": [ "1.2" ] }
//     foo[tab]{"nested": { "count": 3 }, "versions": [ "2.0" ] }
//     bar[tab]{"pizza": 2, "cheese": 3}
//
// reduce to
//
//     foo[tab]{"chips":4,"deep":{"level1":{"level2":3}},"drinks":1,"frugal":true,"nested":{"count":4},"versions":["1.2","2.0"]}
//     bar[tab]{"cheese":3,"pizza":2}
package main

import (
    "bufio"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "io"
    "os"
    "reflect"
    "sort"
    "strings"
)

type Ctx struct {
    Delimiter string
    Limit int
    Data map[string]map[string]interface{}
    Writer io.Writer
    SortOutput bool
}

func main() {
    var (
        delimiter string
        entry_limit int
        outfile string
        writer io.Writer
        sort_output bool
        help bool
    )

    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, "Usage: %s [options] inputfiles ...\n\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "Options:\n\n")
        flag.PrintDefaults()

    }

    flag.StringVar(&delimiter, "del", "\t", "Alternate delimiter between key and JSON object")
    flag.IntVar(&entry_limit, "limit", 0, "If more than `limit` unique keys" +
        " are found, the data will be flushed to output and aggregation starts" +
        " over. A limit of zero means no limit.")
    flag.StringVar(&outfile, "outfile", "", "Output file (defaults to standard output)")
    flag.BoolVar(&sort_output, "sort", false, "Sort output")
    flag.BoolVar(&help, "help", false, "Display this help message")

    flag.Parse()

    if help {
        flag.Usage()
        os.Exit(0)
    }

    if outfile == "" {
        writer = os.Stdout
    } else {
        out_fh, err := os.Create(outfile)
        if err != nil {
            log.Fatalf("couldn't open output file %s: %s\n", outfile, err)
        }
        defer out_fh.Close()

        writer = out_fh
    }

    ctx := new(Ctx)
    ctx.Delimiter = delimiter
    ctx.Limit = entry_limit
    ctx.Data = make(map[string]map[string]interface{})
    ctx.Writer = writer
    ctx.SortOutput = sort_output

    files := flag.Args()
    if len(files) == 0 {
        process_file(ctx, os.Stdin)
    } else {
        for _, file := range files {
            in_fh, err := os.Open(file)
            if err != nil {
                log.Fatalf("couldn't open input file %s: %s", file, err)
            }
            process_file(ctx, in_fh)
            in_fh.Close()
        }
    }

    write_data(ctx, writer)
}

func ProcessFile(reader io.Reader, delimiter string, limit int) map[string]map[string]interface{} {
    ctx := new(Ctx)
    ctx.Delimiter = delimiter
    ctx.Limit = 0
    ctx.Data = make(map[string]map[string]interface{})

    process_file(ctx, reader)
    return ctx.Data
}

func process_file(ctx *Ctx, reader io.Reader) {
    data := ctx.Data
    scanner := bufio.NewScanner(reader)
    line_cnt := 0

    for scanner.Scan() {
        line := scanner.Text()
        line_cnt++
        parts := strings.SplitN(line, ctx.Delimiter, 2)
        if len(parts) < 2 {
            log.Fatalf("wrong number of fields at line %d: %d: '%s'", line_cnt,
                len(parts), line)
        }
        this_data := make(map[string]interface{})
        err := json.Unmarshal([]byte(parts[1]), &this_data)
        if err != nil {
            log.Printf("couldn't parse JSON object '%s': %s", parts[1], err)
            continue
        }

        stored_val, ok := data[parts[0]]
        if !ok {
            if ctx.Limit > 0 && len(data) >= ctx.Limit {
                write_data(ctx, ctx.Writer)
                ctx.Data = make(map[string]map[string]interface{})
                data = ctx.Data
            }

            data[parts[0]] = this_data
            continue
        }

        err = aggregate(ctx, stored_val, this_data)
        if err != nil {
            log.Printf("couldn't aggregate: %s", err)
        }
    }
}

func aggregate(ctx *Ctx, stored_data map[string]interface{},
    this_data map[string]interface{}) error {

    for nk, nv := range this_data {
        // log.Printf("looking at key %+v, val %+v", nk, nv)
        ov, ok := stored_data[nk]
        if !ok {
            stored_data[nk] = nv
            continue
        }

        ov_v := reflect.ValueOf(ov)
        ov_is_num, ov_is_int, ov_is_signed := is_num_type(ov_v)

        nv_v := reflect.ValueOf(nv)
        nv_is_num, nv_is_int, nv_is_signed := is_num_type(nv_v)

        if ov_is_num && !nv_is_num {
            // Drop since the old value was a numeric type and this one isn't
            continue
        }

        if !ov_is_num {
            // Last non-numeric value wins

            nv_kind := nv_v.Kind()
            ov_kind := ov_v.Kind()

            if nv_kind != ov_kind {
                stored_data[nk] = nv
                continue
            }

            if nv_kind == reflect.Map {
                ov_map, ok := ov.(map[string]interface{})
                if !ok {
                    return fmt.Errorf("assertion of ov to map[string]interface{} failed")
                }
                nv_map, ok := nv.(map[string]interface{})
                if !ok {
                    return fmt.Errorf("assertion of nv to map[string]interface{} failed")
                }

                err := aggregate(ctx, ov_map, nv_map)
                if err != nil {
                    return err
                }
                continue
            }

            if nv_kind == reflect.Slice {
                ov_slice, ok := ov.([]interface{})
                if !ok {
                    return fmt.Errorf("assertion of ov to []interface{} failed")
                }
                nv_slice, ok := nv.([]interface{})
                if !ok {
                    return fmt.Errorf("assertion of nv to []interface{} failed")
                }

                stored_data[nk] = append(ov_slice, nv_slice...)
                continue
            }

            stored_data[nk] = nv
            continue
        }

        if ov_is_int && nv_is_int {
            if nv_is_signed == ov_is_signed {
                if nv_is_signed {
                    sum := ov_v.Int() + nv_v.Int()
                    stored_data[nk] = sum
                } else {
                    sum := ov_v.Uint() + nv_v.Uint()
                    stored_data[nk] = sum
                }
            } else {
                if nv_is_signed {
                    sum := int64(ov_v.Uint()) + nv_v.Int()
                    stored_data[nk] = sum
                } else {
                    sum := ov_v.Int() + int64(nv_v.Uint())
                    stored_data[nk] = sum
                }
            }
            continue
        }

        // FIXME: handle at least one of them being a float
        ov_float := float64(0)
        nv_float := float64(0)

        if ov_is_int {
            if ov_is_signed {
                ov_float = float64(ov_v.Int())
            } else {
                ov_float = float64(ov_v.Uint())
            }
        } else {
            ov_float = ov_v.Float()
        }

        if nv_is_int {
            if nv_is_signed {
                nv_float = float64(nv_v.Int())
            } else {
                nv_float = float64(nv_v.Uint())
            }
        } else {
            nv_float = nv_v.Float()
        }

        stored_data[nk] = ov_float + nv_float
    }

    return nil
}

func is_num_type(v reflect.Value) (bool, bool, bool) {
    switch v.Kind() {
    case reflect.Int64:
        fallthrough
    case reflect.Int32:
        fallthrough
    case reflect.Int16:
        fallthrough
    case reflect.Int8:
        fallthrough
    case reflect.Int:
        return true, true, true
    case reflect.Uint64:
        fallthrough
    case reflect.Uint32:
        fallthrough
    case reflect.Uint16:
        fallthrough
    case reflect.Uint8:
        fallthrough
    case reflect.Uint:
        return true, true, false
    case reflect.Float64:
        fallthrough
    case reflect.Float32:
        return true, false, true
    }

    return false, false, false
}

func write_data(ctx *Ctx, writer io.Writer) {
    out_delimiter := "\t"

    if ctx.SortOutput {
        data := make([][]string, 0, len(ctx.Data))
        for k,v := range ctx.Data {
            serialized, err := json.Marshal(v)
            if err != nil {
                log.Printf("couldn't convert data to JSON")
                continue
            }
            data = append(data, []string{k, string(serialized)})
        }

        sorter := new(DataSorter)
        sorter.Data = data

        sort.Sort(sorter)

        for _, d := range data {
            fmt.Fprintf(writer, "%s%s%s\n", d[0], out_delimiter, d[1])
        }

        return
    }

    for k,v := range ctx.Data {
        serialized, err := json.Marshal(v)
        if err != nil {
            log.Printf("couldn't convert data to JSON")
            continue
        }
        fmt.Fprintf(writer, "%s%s%s\n", k, out_delimiter, serialized)
    }
}

type DataSorter struct {
    Data [][]string
}

func (d *DataSorter) Len() int {
    return len(d.Data)
}

func (d *DataSorter) Less(i, j int) bool {
    return d.Data[i][0] < d.Data[j][0]
}

func (d *DataSorter) Swap(i, j int) {
    d.Data[i], d.Data[j] = d.Data[j], d.Data[i]
}
