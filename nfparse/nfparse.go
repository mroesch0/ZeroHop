package main

import (
    "bufio"
    "encoding/csv"
    "fmt"
    "io"
    "os"
    "net"
    "strconv"
    "time"
    "honnef.co/go/netdb"
    "github.com/ip2location/ip2location-go"
)

func readSample(rs io.ReadSeeker) ([][]string, error) {
    var proto *netdb.Protoent

    db, err := ip2location.OpenDB("./locdb/IP2LOCATION-LITE-DB3.BIN")
    if err != nil {
        panic(err)
    } 

    // Skip first row (line)
    row1, err := bufio.NewReader(rs).ReadSlice('\n')
    if err != nil {
        return nil, err
    }
    _, err = rs.Seek(int64(len(row1)), io.SeekStart)
    if err != nil {
        return nil, err
    }

    // Read remaining rows
    r := csv.NewReader(rs)
    r.Comma = ' ' // space delimited....

    rows, err := r.ReadAll()
    if err != nil {
        return nil, err
    }

    for _, each := range rows {
        var sa string
        var da string
        var sp *netdb.Servent
        var dp *netdb.Servent
        var ss string
        var ds string

        srcaddr, _ := net.LookupAddr(each[3])
        // take the first slice off the top only
        for _, sa = range srcaddr {
            break
        }
        if sa == "" {
            sa = each[3] 
        }

        srcloc, _ := db.Get_all(each[3])

        dstaddr, _  := net.LookupAddr(each[4])
        for _, da = range dstaddr {
            break
        }
        if da == "" {
            da = each[4]
        }

        dstloc, _ := db.Get_all(each[4])

        //convert protocol number string to protocol struct
        pnum, _ := strconv.Atoi(each[7])
        proto = netdb.GetProtoByNumber(pnum)

        // lookup source port service name
        pconv, _ := strconv.Atoi(each[5])
        sp = netdb.GetServByPort(pconv, proto)
        if(sp != nil){
            ss = sp.Name;
        } else {
            ss = each[5]
        }

        // lookup dst port service name
        pconv, _ = strconv.Atoi(each[6])
        dp = netdb.GetServByPort(pconv, proto)
        if(dp != nil){
            ds = dp.Name;
        } else {
            ds = each[5]
        }

        i, _ := strconv.ParseInt(each[10], 10, 64)
        stime := time.Unix(i, 0)

        j, _ := strconv.ParseInt(each[11], 10, 64)
        etime := time.Unix(j, 0)

        fmt.Printf("start: %v end: %v %s (%s)%s:%s -> (%s)%s:%s %s pkts %s bytes action: %s %s i/f:%s custid:%s %s\n", stime, etime, proto.Name, srcloc.Country_short, sa, ss, dstloc.Country_short, da, ds, each[8], each[9], each[12], each[13], each[2], each[1], each[0])
    }

    return rows, nil
}

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Missing parameter, provide file name!")
        return
    }

    f, err := os.Open(os.Args[1])
    if err != nil {
        panic(err)
    }

    defer f.Close()
    rows, err := readSample(f)
    if err != nil {
        panic(err)
    }
    fmt.Println("Rows processed: ", len(rows))
    
}