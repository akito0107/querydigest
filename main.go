package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type queryTime struct {
	QueryTime    float64
	LockTime     float64
	RowsSent     int
	RowsExamined int
}

type SlowQueryInfo struct {
	RawQuery  []string
	Time      time.Time
	QueryTime *queryTime
}

func main() {
	f, err := os.Open("./slow.log")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 4096)

	var slowqueries []SlowQueryInfo
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		if !strings.HasPrefix(string(line), "# Time:") {
			continue
		}

		strs := strings.Split(string(line), " ")
		var slowquery SlowQueryInfo

		t, err := time.Parse("2006-01-02T15:04:05.000000Z", strs[2])
		if err != nil {
			log.Fatal(err)
		}
		slowquery.Time = t
		nextLine(reader)

		slowquery.QueryTime = parseQueryTime(nextLine(reader))

		slowqueries = append(slowqueries, slowquery)
	}
}

func nextLine(reader *bufio.Reader) string {
	l, _, err := reader.ReadLine()
	if err != nil {
		log.Fatal(err)
	}

	return string(l)
}

func parseQueryTime(str string) *queryTime {

	queryTimes := strings.Split(str, " ")
	// Query_time
	qt, err := strconv.ParseFloat(queryTimes[2], 64)
	if err != nil {
		log.Fatal(err)
	}
	// Lock_time
	lt, err := strconv.ParseFloat(queryTimes[5], 64)
	if err != nil {
		log.Fatal(err)
	}
	// Rows_sent
	rs, err := strconv.ParseInt(queryTimes[7], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	// Rows_examined
	re, err := strconv.ParseInt(queryTimes[10], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return &queryTime{
		QueryTime:    qt,
		LockTime:     lt,
		RowsSent:     int(rs),
		RowsExamined: int(re),
	}
}
