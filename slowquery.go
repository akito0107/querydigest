package querydigest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"unsafe"

	"github.com/stuartcarnie/go-simd/unicode/utf8"

	"github.com/akito0107/querydigest/internal/dart"
)

type SlowQueryScanner struct {
	reader       *bufio.Reader
	line         string
	currentInfo  SlowQueryInfo
	err          error
	queryBuf     *bytes.Buffer
	queryTimeBuf *bytes.Buffer
}

const ioBufSize = 128 * 1024 * 1024

func NewSlowQueryScanner(r io.Reader) *SlowQueryScanner {
	return &SlowQueryScanner{
		reader:       bufio.NewReaderSize(r, ioBufSize),
		queryBuf:     &bytes.Buffer{},
		queryTimeBuf: &bytes.Buffer{},
	}
}

func (s *SlowQueryScanner) SlowQueryInfo() *SlowQueryInfo {
	return &s.currentInfo
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

		if err := s.nextLine(); err != nil {
			s.err = err
			return false
		}

		if err := s.nextLine(); err != nil {
			s.err = err
			return false
		}

		s.queryTimeBuf.Reset()
		s.queryTimeBuf.WriteString(s.line)

		for {
			if err := s.nextLine(); err == io.EOF {
				return false
			} else if err != nil {
				s.err = err
				return false
			}

			s.queryBuf.Reset()

			for {
				s.queryBuf.WriteString(s.line)
				if strings.HasSuffix(s.line, ";") {
					break
				}
				if err := s.nextLine(); err != nil {
					s.err = err
					return false
				}
			}

			b := s.queryBuf.Bytes()
			if len(b) > 6 && parsableQueryLine(b[:6]) {
				if cap(s.currentInfo.RawQuery) < len(b) {
					s.currentInfo.RawQuery = make([]byte, len(b))
				}
				s.currentInfo.RawQuery = s.currentInfo.RawQuery[:len(b)]
				copy(s.currentInfo.RawQuery, b)
				parseQueryTime(&s.currentInfo.QueryTime, unsafeString(s.queryTimeBuf.Bytes()))
				return true
			} else if strings.HasPrefix(s.line, "#") {
				break
			}
		}
	}
}

func (s *SlowQueryScanner) nextLine() error {
	l, _, err := s.reader.ReadLine()
	if err != nil {
		return err
	}
	if utf8.Valid(l) {
		s.line = unsafeString(l)
	} else {
		s.line = fmt.Sprintf("%q", l)
	}

	return nil
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

var supportedSQLs = dart.Must(dart.Build([]string{
	"SELECT",
	"INSERT",
	"UPDATE",
	"DELETE",
	"WITH",
	"ALTER",
}))

func parsableQueryLine(b []byte) bool {
	return supportedSQLs.Match(b)
}

type QueryTime struct {
	QueryTime    float64
	LockTime     float64
	RowsSent     int
	RowsExamined int
}

type SlowQueryInfo struct {
	ParsedQuery string
	RawQuery    []byte
	QueryTime   QueryTime
}

func (i *SlowQueryInfo) clone() *SlowQueryInfo {
	rawQuery := make([]byte, len(i.RawQuery))
	copy(rawQuery, i.RawQuery)
	return &SlowQueryInfo{
		RawQuery:  rawQuery,
		QueryTime: i.QueryTime,
	}
}

func parseHeader(str string) (queryTime, lockTime, rowsSent, rowsExamined string) {
	// skip `# Query_time: `
	str = str[14:]

	var i int
	for i = 0; i < len(str); i++ {
		if str[i] == ' ' {
			break
		}
	}
	queryTime = str[:i]
	str = str[i+13:]
	for i = 0; i < len(str); i++ {
		if str[i] == ' ' {
			break
		}
	}
	lockTime = str[:i]
	str = str[i+12:]
	for i = 0; i < len(str); i++ {
		if str[i] == ' ' {
			break
		}
	}
	rowsSent = str[:i]
	rowsExamined = str[i+17:]

	return queryTime, lockTime, rowsSent, rowsExamined
}

func parseQueryTime(q *QueryTime, str string) {

	queryTime, lockTime, rowsSent, rowsExamined := parseHeader(str)

	// queryTimes := strings.SplitN(str, ":", 5)
	// Query_time
	qt, err := strconv.ParseFloat(queryTime, 64)
	if err != nil {
		log.Fatal(err)
	}
	// Lock_time
	lt, err := strconv.ParseFloat(lockTime, 64)
	if err != nil {
		log.Fatal(err)
	}
	// Rows_sent
	rs, err := strconv.ParseInt(rowsSent, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	// Rows_examined
	re, err := strconv.ParseInt(rowsExamined, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	q.QueryTime = qt
	q.LockTime = lt
	q.RowsSent = int(rs)
	q.RowsExamined = int(re)
}
