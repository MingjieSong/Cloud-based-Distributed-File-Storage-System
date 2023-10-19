package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap        map[string]string
	sortedServerHash []string
	// <server hash, serverAdd> 	"blockstore” + address
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

// return the correct server id for the input block(blockid)
func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	for i := 0; i < len(c.sortedServerHash); i++ {
		if c.sortedServerHash[i] > blockId {
			return c.ServerMap[c.sortedServerHash[i]]
		}
	}
	return c.ServerMap[c.sortedServerHash[0]]
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	var hashRing ConsistentHashRing
	hashRing.ServerMap = make(map[string]string)
	for _, serverAddr := range serverAddrs {
		serverHash := hashRing.Hash("blockstore" + serverAddr)
		hashRing.ServerMap[serverHash] = serverAddr
		hashRing.sortedServerHash = append(hashRing.sortedServerHash, serverHash)
	}
	sort.Strings(hashRing.sortedServerHash)
	return &hashRing
}
