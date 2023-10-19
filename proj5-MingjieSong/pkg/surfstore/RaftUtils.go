package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	serverAdds := config.RaftAddrs
	peers := make(map[int]*Peer)
	for i, addr := range serverAdds {
		if int64(i) == id {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatal("error in dialing: ", err)
		}
		client := NewRaftSurfstoreClient(conn)
		peers[i] = &Peer{
			client:     client,
			nextIndex:  0,
			matchIndex: -1,
		}
	}

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		serverId:       int(id),
		majorityNum:    len(serverAdds)/2 + 1,
		peers:          peers,
		commitResult:   make([]*Result, 0),
		addr:           serverAdds[id],
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	listener, _ := net.Listen("tcp", server.addr)
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)
	s.Serve(listener)
	return nil
}
