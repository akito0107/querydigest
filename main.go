package main

import (
	"bufio"
	"io"
	"log"
	"os"
)

func main() {
	f, err := os.Open("./slow.log")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 4096)

	var isQueryBlock bool
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}
}
