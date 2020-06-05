package querydigest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/stuartcarnie/go-simd/unicode/utf8"
)

type SlowQueryScanner struct {
	reader      *bufio.Reader
	line        string
	currentInfo *SlowQueryInfo
	err         error
	bufPool     sync.Pool
}

const size = 1024 * 1024

func NewSlowQueryScanner(r io.Reader) *SlowQueryScanner {
	return &SlowQueryScanner{
		reader: bufio.NewReaderSize(r, size),
		bufPool: sync.Pool{
			New: func() interface{} {
				buf := &bytes.Buffer{}
				buf.Grow(size)
				return buf
			},
		},
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

		for {
			if err := s.nextLine(); err == io.EOF {
				return false
			} else if err != nil {
				s.err = err
				return false
			}

			buf := s.bufPool.Get().(*bytes.Buffer)

			for {
				buf.WriteString(s.line)
				if strings.HasSuffix(s.line, ";") {
					break
				}
				if err := s.nextLine(); err != nil {
					s.err = err
					buf.Reset()
					s.bufPool.Put(buf)
					return false
				}
			}

			b := buf.Bytes()
			q := make([]byte, len(b))
			copy(q, b)

			if len(b) > 6 && parsableQueryLine(b[:6]) {
				slowquery.RawQuery = q
				s.currentInfo = &slowquery

				buf.Reset()
				s.bufPool.Put(buf)

				return true
			} else if strings.HasPrefix(s.line, "#") {
				buf.Reset()
				s.bufPool.Put(buf)
				break
			}
			buf.Reset()
			s.bufPool.Put(buf)
		}
	}
}

func (s *SlowQueryScanner) nextLine() error {
	l, _, err := s.reader.ReadLine()
	if err != nil {
		return err
	}
	if utf8.Valid(l) {
		s.line = *(*string)(unsafe.Pointer(&l))
	} else {
		s.line = fmt.Sprintf("%q", l)
	}

	return nil
}

var supportedSQLs = []string{"SELECT", "INSERT", "ALTER", "WITH", "DELETE", "UPDATE"}

func parsableQueryLine(str []byte) bool {
	for i := 0; i < len(str); i++ {
		if 'a' <= str[i] && str[i] <= 'z' {
			str[i] -= 'a' - 'A'
		}
	}

	q := *(*string)(unsafe.Pointer(&str))

	for _, s := range supportedSQLs {
		if strings.HasPrefix(q, s) {
			return true
		}
	}

	return false
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
	QueryTime   *QueryTime
}

func parseHeader(str string) []string {
	// skip `# Query_time: `
	str = str[14:]

	var i int
	for i = 0; i < len(str); i++ {
		if str[i] == ' ' {
			break
		}
	}
	queryTime := str[:i]
	str = str[i+13:]
	for i = 0; i < len(str); i++ {
		if str[i] == ' ' {
			break
		}
	}
	lockTime := str[:i]
	str = str[i+12:]
	for i = 0; i < len(str); i++ {
		if str[i] == ' ' {
			break
		}
	}
	rowsSent := str[:i]
	rowsExamined := str[i+17:]

	return []string{queryTime, lockTime, rowsSent, rowsExamined}
}

func parseQueryTime(str string) *QueryTime {

	queryTimes := parseHeader(str)

	// queryTimes := strings.SplitN(str, ":", 5)
	// Query_time
	qt, err := strconv.ParseFloat(queryTimes[0], 64)
	if err != nil {
		log.Fatal(err)
	}
	// Lock_time
	lt, err := strconv.ParseFloat(queryTimes[1], 64)
	if err != nil {
		log.Fatal(err)
	}
	// Rows_sent
	rs, err := strconv.ParseInt(queryTimes[2], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	// Rows_examined
	re, err := strconv.ParseInt(queryTimes[3], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return &QueryTime{
		QueryTime:    qt,
		LockTime:     lt,
		RowsSent:     int(rs),
		RowsExamined: int(re),
	}
}
