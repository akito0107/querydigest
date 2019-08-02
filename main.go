package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/akito0107/xsqlparser/sqlast"
)

type queryTime struct {
	QueryTime    float32
	LockTime     float32
	RowsSent     int
	RowsExamined int
}

type SlowQueryInfo struct {
	Query     *sqlast.SQLStmt
	Time      time.Time
	QueryTime queryTime
}

func main() {
	f, err := os.Open("./slow.log")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 4096)

	var isQueryBlock bool
	var slowqueries []SlowQueryInfo
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}

		if strings.HasPrefix(string(line), "#") {
			isQueryBlock = true
		}
	}
}
