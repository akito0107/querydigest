package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
)

type queryTime struct {
	QueryTime    float64
	LockTime     float64
	RowsSent     int
	RowsExamined int
}

type SlowQueryInfo struct {
	RawQuery  string
	Time      time.Time
	QueryTime *queryTime
}

func main() {
	f, err := os.Open("./slow.log")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := parseSlowQuery(f); err != nil {
		log.Fatal(err)
	}
}

func parseSlowQuery(r io.Reader) error {
	reader := bufio.NewReaderSize(r, 4096)

	var slowqueries []SlowQueryInfo

	bline, _, err := reader.ReadLine()
	if err != nil {
		return err
	}
	line := string(bline)
PARSE_LOOP:
	for {
		if !strings.HasPrefix(line, "# Time:") {
			l, err := nextLine(reader)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			line = l
			continue
		}

		strs := strings.Split(line, " ")
		var slowquery SlowQueryInfo

		t, err := time.Parse("2006-01-02T15:04:05.000000Z", strs[2])
		if err != nil {
			return err
		}
		slowquery.Time = t
		nextLine(reader)

		qt, err := nextLine(reader)
		if err != nil {
			return err
		}
		slowquery.QueryTime = parseQueryTime(qt)

		var query string
		for {
			l, err := nextLine(reader)
			if err == io.EOF {
				break PARSE_LOOP
			} else if err != nil {
				return err
			}

			if parsableQueryLine(l) {
				query = l
				break
			} else if strings.HasPrefix(l, "#") {
				line = l
				continue PARSE_LOOP
			}
		}

		slowquery.RawQuery = query
		slowqueries = append(slowqueries, slowquery)

		line, err = nextLine(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	log.Println("split done")

	var wg sync.WaitGroup
	limit := make(chan struct{}, 6)

	log.Println(len(slowqueries))
	for _, s := range slowqueries {
		wg.Add(1)
		go func(s SlowQueryInfo) {
			defer func() {
				<-limit
				wg.Done()
			}()
			limit <- struct{}{}
			parser, err := xsqlparser.NewParser(bytes.NewBufferString(s.RawQuery), &dialect.GenericSQLDialect{})
			if err != nil {
				log.Println(err)
			}
			stmt, err := parser.ParseStatement()
			if err != nil {
				log.Println(err)
			}
			log.Println(stmt.ToSQLString())
		}(s)
	}

	wg.Wait()

	log.Println("parse done")

	return nil
}

func nextLine(reader *bufio.Reader) (string, error) {
	l, _, err := reader.ReadLine()
	if err != nil {
		return "", err
	}

	return string(l), nil
}

var supportedSQLs = []string{"SELECT", "INSERT", "ALTER", "WITH", "CREATE", "DELETE", "UPDATE"}

func parsableQueryLine(str string) bool {
	for _, s := range supportedSQLs {
		if strings.HasPrefix(str, s) {
			return true
		}
	}

	return false
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
