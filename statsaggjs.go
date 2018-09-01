// Copyright (c) 2018 Don Owens <don@regexguy.com>.  All rights reserved.
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

// The dstatsaggjson program aggregates counts in tab-delimited files
// where the first column is a key and the rest of the line is a JSON
// object. Numeric values are incremented in JSON objects for matching
// keys. For non-numeric values, the last one wins, unless there are
// numeric values for the same key, in which case the numeric values
// are used and non-numeric values are ignored.
//
// E.g., these records:
//
//     foo[tab]{"chips": 1, "drinks": 1, "frugal": false}
//     bar[tab]{"pizza": 2, "cheese": 3}
//     foo[tab]{"chips": 3, "frugal": true}
//
// reduce to
//
//     foo[tab]{"chips": 4, "drinks": 1, "frugal": true}
//     bar[tab]{"pizza": 2, "cheese": 3}
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
    "strings"
)

type Ctx struct {
    Delimiter string
    Data map[string]map[string]interface{}
}

func main() {
    var (
        delimiter string
        outfile string
        writer io.Writer
    )

    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, "Usage: %s [options] inputfiles ...\n\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "Options:\n\n")
        flag.PrintDefaults()

    }

    flag.StringVar(&delimiter, "del", "\t", "Alternate delimiter between key and JSON object")
    flag.StringVar(&outfile, "outfile", "", "Output file (defaults to standard output)")
    flag.Parse()

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
    ctx.Data = make(map[string]map[string]interface{})

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
            data[parts[0]] = this_data
            continue
        }

        for nk, nv := range this_data {
            ov, ok := stored_val[nk]
            if !ok {
                stored_val[nk] = nv
                continue
            }

            // FIXME: check if int, uint, or FLOAT types
            
            ov_v := reflect.ValueOf(ov)
            ov_is_num, ov_is_int, ov_is_signed := is_num_type(ov_v)
            // if ov_is_int {
            //     stored_val[nk] = nv
            //     continue
            // }

            nv_v := reflect.ValueOf(nv)
            nv_is_num, nv_is_int, nv_is_signed := is_num_type(nv_v)

            if ov_is_num && !nv_is_num {
                // Drop since the old value was a numeric type and this one isn't
                continue
            }

            if !ov_is_num {
                // Last non-numeric value wins
                // FIXME: handle nested structures here

                stored_val[nk] = nv
                continue
            }

            if ov_is_int && nv_is_int {
                if nv_is_signed == ov_is_signed {
                    if nv_is_signed {
                        sum := ov_v.Int() + nv_v.Int()
                        stored_val[nk] = sum
                    } else {
                        sum := ov_v.Uint() + nv_v.Uint()
                        stored_val[nk] = sum
                    }
                } else {
                    if nv_is_signed {
                        sum := int64(ov_v.Uint()) + nv_v.Int()
                        stored_val[nk] = sum
                    } else {
                        sum := ov_v.Int() + int64(nv_v.Uint())
                        stored_val[nk] = sum
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

            stored_val[nk] = ov_float + nv_float
        }
    }
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
    for k,v := range ctx.Data {
        serialized, err := json.Marshal(v)
        if err != nil {
            log.Printf("couldn't convert data to JSON")
            continue
        }
        fmt.Fprintf(writer, "%s%s%s\n", k, out_delimiter, serialized)
    }
}
