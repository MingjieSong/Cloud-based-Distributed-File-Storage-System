package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"sort"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}

	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])

	inputFileName := os.Args[1]

	inputFile, err := os.Open(inputFileName)
	if err != nil {
		log.Fatal(err)
	}

	result := make([][]byte, 0)
	reader := bufio.NewReader(inputFile)

	for {
		record := make([]byte, 100)
		// FIXME: how to ensure read in 100 bytes each time
		// https://piazza.com/class/lcduqvnr9qh2po/post/55
		// https://pkg.go.dev/io#ReadFull
		//_, err := reader.Read(record)
		_, err := io.ReadFull(reader, record)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}

		if err == io.EOF {
			break
		}

		result = append(result, record)
	}
	inputFile.Close()

	//sort FIXME (what's the difference  )
	// sort.Slice(result, func(i, j int) bool { return string(result[i][:10]) < string(result[j][:10]) })
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i][:10], result[j][:10]) < 0 })

	//write sorted reacord to the output file
	outputFileName := os.Args[2]
	outputFile, err := os.Create(outputFileName)

	if err != nil {
		log.Fatal(err)
	}

	writer := bufio.NewWriter(outputFile)
	for i := 0; i < len(result); i++ {
		_, err := writer.Write(result[i])
		if err != nil {
			log.Fatal(err)
		}
		writer.Flush()
	}
	outputFile.Close()

}

// sort
// slice
// interface {define a collection of methods }
// struct defines a type { a collection of fields }
// struct/type can have functions implement an interface
// https://pkg.go.dev/sort@go1.19.5 (sortKeys ???? )
