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
	"sync"
	"time"
	"unsafe"

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
	QueryTime   *queryTime
}

var slowLogPath = flag.String("f", "slow.log", "slow log filepath (default slow.log)")
var concurrency = flag.Int("j", 0, "concurrency (default = num of cpus)")

func main() {
	// defer profile.Start(profile.ProfilePath("."), profile.TraceProfile).Stop()
	// defer profile.Start(profile.ProfilePath("."), profile.CPUProfile).Stop()
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

func parseRawFile(r io.Reader, parsequeue chan *SlowQueryInfo) {
	slowqueryscanner := NewSlowQueryScanner(r)

	for slowqueryscanner.Next() {
		parsequeue <- slowqueryscanner.SlowQueryInfo()
	}
	if err := slowqueryscanner.Err(); err != nil {
		log.Fatal(err)
	}

	close(parsequeue)
}

type SlowQueryScanner struct {
	reader      *bufio.Reader
	line        string
	currentInfo *SlowQueryInfo
	err         error
}

func NewSlowQueryScanner(r io.Reader) *SlowQueryScanner {
	return &SlowQueryScanner{
		reader: bufio.NewReaderSize(r, 1024*1024*16),
	}
}

func (s *SlowQueryScanner) SlowQueryInfo() *SlowQueryInfo {
	return s.currentInfo
}

func (s *SlowQueryScanner) Err() error {
	return s.err
}

func (s *SlowQueryScanner) Next() bool {
	if s.err != nil {
		return false
	}
	for {
		for !strings.HasPrefix(s.line, "# Time:") {
			if err := s.nextLine(); err == io.EOF {
				return false
			} else if err != nil {
				s.err = err
				return false
			}
		}
		var slowquery SlowQueryInfo

		if err := s.nextLine(); err != nil {
			s.err = err
			return false
		}

		if err := s.nextLine(); err != nil {
			s.err = err
			return false
		}

		slowquery.QueryTime = parseQueryTime(s.line)

		var query string
		for {
			if err := s.nextLine(); err == io.EOF {
				return false
			} else if err != nil {
				s.err = err
				return false
			}

			if parsableQueryLine(s.line) {
				query = s.line
				slowquery.RawQuery = query
				s.currentInfo = &slowquery
				return true
			} else if strings.HasPrefix(s.line, "#") {
				break
			}
		}
	}
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (s *SlowQueryScanner) nextLine() error {
	l, _, err := s.reader.ReadLine()
	if err != nil {
		return err
	}
	s.line = string(l)

	return nil
}

func parseSlowQuery(r io.Reader, concurrency int) error {
	parsequeue := make(chan *SlowQueryInfo, 500)

	go parseRawFile(r, parsequeue)

	summ := NewSummarizer()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range parsequeue {
				res, err := replaceWithZeroValue(s.RawQuery)
				if err != nil {
					continue
				}
				s.ParsedQuery = res

				summ.collect(s)
			}
		}()
	}
	wg.Wait()

	var qs []*slowQuerySummary

	for _, v := range summ.m {
		qs = append(qs, v)
	}

	sort.Slice(qs, func(i, j int) bool {
		return qs[i].totalTime > qs[j].totalTime
	})

	for _, s := range qs {
		fmt.Println("------------------------------")
		fmt.Printf("row: %s\n", s.rowSample)
		fmt.Printf("query time: %f\ns", s.totalTime)
		fmt.Printf("total query count: %d\n", s.totalQueryCount)
		fmt.Println("------------------------------")
	}

	return nil
}

type summarizer struct {
	m  map[string]*slowQuerySummary
	mu sync.Mutex
}

func NewSummarizer() *summarizer {
	return &summarizer{
		m: make(map[string]*slowQuerySummary),
	}
}

func (s *summarizer) collect(i *SlowQueryInfo) {
	s.mu.Lock()
	summary, ok := s.m[i.ParsedQuery]
	if !ok {
		summary = &slowQuerySummary{
			rowSample: i.RawQuery,
		}
	}
	summary.appendQueryTime(i.QueryTime)
	s.m[i.ParsedQuery] = summary
	s.mu.Unlock()
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

var supportedSQLs = []string{"SELECT", "INSERT", "ALTER", "WITH", "CREATE", "DELETE", "UPDATE"}

func parsableQueryLine(str string) bool {
	if len(str) > 8 {
		str = str[:8]
	}
	str = strings.ToUpper(str)
	for _, s := range supportedSQLs {
		if strings.HasPrefix(str, s) {
			return true
		}
	}

	return false
}

func parseQueryTime(str string) *queryTime {

	queryTimes := strings.SplitN(str, " ", 12)
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
		log.Printf("Parse failed: invalied sql: %s \n", src)
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
