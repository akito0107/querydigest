package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/profile"
	"golang.org/x/sync/errgroup"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/astutil"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
)

type queryTime struct {
	QueryTime    float64
	LockTime     float64
	RowsSent     int
	RowsExamined int
}

type SlowQueryInfo struct {
	ParsedQuery string
	RawQuery    string
	Time        time.Time
	QueryTime   *queryTime
}

var slowLogPath = flag.String("f", "slow.log", "slow log filepath (default slow.log)")
var concurrency = flag.Int("j", 0, "concurrency (default = num of cpus)")

func main() {
	defer profile.Start(profile.ProfilePath("."), profile.MemProfile).Stop()
	flag.Parse()

	f, err := os.Open(*slowLogPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if *concurrency == 0 {
		*concurrency = runtime.NumCPU()
	}

	if err := parseSlowQuery(f, *concurrency); err != nil {
		log.Fatal(err)
	}
}

func parseSlowQuery(r io.Reader, concurrency int) error {
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

	limit := make(chan struct{}, concurrency)
	queue := make(chan *SlowQueryInfo, 1)

	var g errgroup.Group
	log.Println(len(slowqueries))
	for _, s := range slowqueries {
		func(s SlowQueryInfo) {
			g.Go(func() error {
				defer func() {
					<-limit
				}()
				limit <- struct{}{}

				res, err := replaceWithZeroValue(s.RawQuery)
				s.ParsedQuery = res
				queue <- &s

				return err
			})
		}(s)
	}

	go func() {
		if err := g.Wait(); err != nil {
			log.Fatal(err)
		}
		close(queue)
	}()

	sqmap := make(map[string]*slowQuerySummary)
	for s := range queue {
		summary, ok := sqmap[s.ParsedQuery]
		if !ok {
			summary = &slowQuerySummary{
				rowSample: s.RawQuery,
			}
		}
		summary.appendQueryTime(s.QueryTime)
		sqmap[s.ParsedQuery] = summary
	}

	var qs []*slowQuerySummary

	for _, v := range sqmap {
		qs = append(qs, v)
	}

	sort.Slice(qs, func(i, j int) bool {
		return qs[i].totalTime > qs[j].totalTime
	})

	for _, s := range qs {
		fmt.Println("------------------------------")
		fmt.Printf("row: %s\n", s.rowSample)
		fmt.Printf("query time: %f\n", s.totalTime)
		fmt.Printf("total query count: %d\n", s.totalQueryCount)
		fmt.Println("------------------------------")
	}

	return nil
}

type slowQuerySummary struct {
	rowSample         string
	totalTime         float64
	totalLockTime     float64
	totalQueryCount   int
	totalRowsSent     int
	totalRowsExamined int
}

func (s *slowQuerySummary) appendQueryTime(q *queryTime) {
	s.totalLockTime += q.LockTime
	s.totalTime += q.QueryTime
	s.totalRowsSent += q.RowsSent
	s.totalRowsExamined += q.RowsExamined

	s.totalQueryCount += 1
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

func replaceWithZeroValue(src string) (string, error) {
	parser, err := xsqlparser.NewParser(bytes.NewBufferString(src), &dialect.GenericSQLDialect{})
	if err != nil {
		return "", err
	}
	stmt, err := parser.ParseStatement()
	if err != nil {
		return "", err
	}

	res := astutil.Apply(stmt, func(cursor *astutil.Cursor) bool {
		switch cursor.Node().(type) {
		case *sqlast.LongValue:
			cursor.Replace(sqlast.NewLongValue(0))
		case *sqlast.DoubleValue:
			cursor.Replace(sqlast.NewDoubleValue(0))
		case *sqlast.BooleanValue:
			cursor.Replace(sqlast.NewBooleanValue(true))
		case *sqlast.SingleQuotedString:
			cursor.Replace(sqlast.NewSingleQuotedString(""))
		case *sqlast.TimestampValue:
			cursor.Replace(sqlast.NewTimestampValue(time.Date(1970, 1, 1, 0, 0, 0, 0, nil)))
		case *sqlast.TimeValue:
			cursor.Replace(sqlast.NewTimeValue(time.Date(1970, 1, 1, 0, 0, 0, 0, nil)))
		case *sqlast.DateTimeValue:
			cursor.Replace(sqlast.NewDateTimeValue(time.Date(1970, 1, 1, 0, 0, 0, 0, nil)))
		}
		return true
	}, nil)

	return res.ToSQLString(), nil
}
