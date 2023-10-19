package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	yaml.Unmarshal(f, &scs)

	return scs
}

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/

	// read data from input file and partition it by server number
	inputFilePath := os.Args[2]
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatal("Error opening file: ", err)
	}

	numServers := len(scs.Servers)
	serverBits := int(math.Log2(float64(numServers))) //int is an allowed assumprion
	partition_map := make(map[int][][]byte)
	results := make([][]byte, 0)
	for i := 0; i < numServers; i++ {
		partition_map[i] = make([][]byte, 0)
	}
	for {
		record := make([]byte, 100)
		_, err := inputFile.Read(record)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal("Error reading file: ", err)
			}
		}
		results = append(results, record)
		respServerId := int(record[0] >> (8 - serverBits))
		partition_map[respServerId] = append(partition_map[respServerId], record)
	}
	inputFile.Close()

	fmt.Println("-------reach here --------size of the map", len(partition_map))
	fmt.Println("-------reach here --------size of the results", len(results))

}

// FIXME :
// dealock problem
// instead of keep creating new connections, can i create only one connection to each server
