package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

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

func listenForClientConnections(write_only_ch chan<- []byte, host string, port string) {

	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil { //we want the server keep running even there is an error
			continue
		}
		// Spawn a new goroutine to handle this client's connections
		// and go back to listening for additional connections
		go handleClientConnection(conn, write_only_ch)
	}

}

func handleClientConnection(conn net.Conn, write_only_ch chan<- []byte) {
	defer conn.Close()
	record := make([]byte, 101) //  extra 1 byte for stream_complete
	for {
		n, err := conn.Read(record)
		if err != nil {
			break
		}
		record = record[:n]
		write_only_ch <- record

	}

}

func dialForServerConnections(host string, port string, record []byte) {
	var conn net.Conn
	var err error
	for i := 0; i < 5; i++ {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			log.Println("Could not dial: ", err)
			time.Sleep(50 * time.Millisecond)
		} else {
			break
		}
	}

	defer conn.Close()
	_, err = conn.Write(record)
	if err != nil {
		log.Fatalf("Error writing to server: %s", err)
	}
	//go sendingToServer(conn, record) //FIXME

}

// send record to other servers
// parameter: connection to a specific server ,   record
/*func sendingToServer(conn net.Conn, record []byte) {
	_, err := conn.Write(record)
	if err != nil {
		log.Fatalf("Error writing to server: %s", err)
	}
	conn.Close()

}*/

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

	// set up server-side
	bidirectional_ch := make(chan []byte)
	defer close(bidirectional_ch)

	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			go listenForClientConnections(bidirectional_ch, server.Host, server.Port)
			break
		}
	}

	//client-side: sendinng data to a specific server
	inputFilePath := os.Args[2]
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatal("Error opening file: ", err)
	}
	reader := bufio.NewReader(inputFile)

	numServers := len(scs.Servers)
	serverBits := int(math.Log2(float64(numServers))) //int is an allowed assumprion

	for {
		record := make([]byte, 100)
		_, err := io.ReadFull(reader, record)

		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal("Error reading file: ", err)
			}

		}

		respServerId := int(record[0] >> (8 - serverBits))
		for _, server := range scs.Servers {
			if respServerId == server.ServerId {
				record = append([]byte{0}, record...)
				dialForServerConnections(server.Host, server.Port, record)
				break
			}
		}
	}
	inputFile.Close()

	//current node/server finish reading
	record := make([]byte, 101)
	record[0] = 1
	for _, server := range scs.Servers {
		dialForServerConnections(server.Host, server.Port, record)
	}

	// read own record from channel
	validRecords := make([][]byte, 0)
	finishServer := 0
	for {
		record := <-bidirectional_ch

		if int(record[0]) != 0 { //finish reading record
			finishServer++
			if finishServer == numServers {
				break
			}
		} else {
			validRecords = append(validRecords, record[1:])
		}

	}
	//sort
	sort.Slice(validRecords, func(i, j int) bool { return bytes.Compare(validRecords[i][:10], validRecords[j][:10]) < 0 })

	//write sorted reacord to the output file
	outputFileName := os.Args[3]
	outputFile, err := os.Create(outputFileName)

	if err != nil {
		log.Fatal(err)
	}

	writer := bufio.NewWriter(outputFile)
	for i := 0; i < len(validRecords); i++ {
		_, err := writer.Write(validRecords[i])
		if err != nil {
			log.Fatal(err)
		}
		writer.Flush()
	}
	outputFile.Close()

}

// FIXME :
// dealock problem
// instead of keep creating new connections, can i create only one connection to each server
