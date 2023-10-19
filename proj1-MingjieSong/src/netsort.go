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

	for {
		record := make([]byte, 101) //  extra 1 byte for stream_complete
		_, err := io.ReadFull(conn, record)
		if err != nil {
			break
		}

		write_only_ch <- record
		/* FIXME : reach stop sign, break
		if int(record[0]) != 0 {
			break
		}*/

	}

}

func dialForServerConnections(serverId int, partition_map map[int][][]byte, scs ServerConfigs) {
	fmt.Println("Server id", serverId, "-------reach here 2 --------")
	fmt.Println("---------------", len(partition_map[0]))
	fmt.Println("---------------", len(partition_map[1]))
	fmt.Println("---------------", len(partition_map[2]))
	fmt.Println("---------------", len(partition_map[3]))
	for _, serv := range scs.Servers {
		send_data := partition_map[serv.ServerId]
		fmt.Println("send_data size:  ", len(send_data))
		if serv.ServerId == serverId {
			continue
		} else {
			for i := 0; i < 5; i++ {
				conn, err := net.Dial("tcp", serv.Host+":"+serv.Port)
				if err != nil {
					log.Println("server id: ", serverId, " Could not dial: ", err)
					time.Sleep(50 * time.Millisecond)
					continue
				} else {
					fmt.Println("Server id", serverId, "-------reach here 3 --------")
					sendingToServer(conn, send_data)
					fmt.Println("Server id", serverId, "-------reach here 4 --------")
					break
				}
			}
		}
	}

}

// send all the records to other servers
// parameter: connection to a specific server ,   record
func sendingToServer(conn net.Conn, allRecords [][]byte) {
	defer conn.Close()
	fmt.Println("------------debug for record value  ", len(allRecords))
	for _, record := range allRecords {
		record = append([]byte{0}, record...)
		fmt.Println("------------debug for record value  ", record[0])
		_, err := conn.Write(record)

		if err != nil {
			log.Fatalf("Error writing to server: %s", err)
		}
	}

	// append stop sign
	record := make([]byte, 100)
	record = append([]byte{1}, record...)
	_, err := conn.Write(record)
	if err != nil {
		log.Fatalf("Error writing to server: %s", err)
	}

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
	reader := bufio.NewReader(inputFile)

	numServers := len(scs.Servers)
	serverBits := int(math.Log2(float64(numServers))) //int is an allowed assumprion
	partition_map := make(map[int][][]byte)
	for i := 0; i < numServers; i++ {
		partition_map[i] = make([][]byte, 0)
	}
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
		partition_map[respServerId] = append(partition_map[respServerId], record)
	}
	inputFile.Close()

	fmt.Println("Server id", serverId, "-------reach here 1  --------size of the map", len(partition_map[serverId]))

	// set up server-side lisener socket
	bidirectional_ch := make(chan []byte)
	defer close(bidirectional_ch)

	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			go listenForClientConnections(bidirectional_ch, server.Host, server.Port)
			break
		}
	}

	//client-side: sending data to a specific server
	dialForServerConnections(serverId, partition_map, scs)

	// read record from channel
	validRecords := make([][]byte, 0)
	finishServer := 0

	/*for _, record := range partition_map[serverId] {
		validRecords = append(validRecords, record)
	} */
	validRecords = append(validRecords, partition_map[serverId]...)

	for {
		fmt.Println("Server id", serverId, "-------reach here 5 --------")
		record := <-bidirectional_ch
		fmt.Println("Server id", serverId, "-------not even here for once ?  --------")
		if int(record[0]) != 0 { //finish reading record
			finishServer++
			if finishServer == (numServers - 1) {
				fmt.Println("Server id", serverId, "-------reach here 6 finished  --------")
				break
			}
		} else {
			validRecords = append(validRecords, record[1:])
			fmt.Println("Server id", serverId, "-------reach here 7 appending records--------")
		}
	}
	fmt.Println("Server id", serverId, "-------reach here 8 -------- record size: ", len(validRecords))

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
