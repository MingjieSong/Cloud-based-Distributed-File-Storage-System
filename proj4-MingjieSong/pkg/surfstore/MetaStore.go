package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap map[string]*FileMetaData
	//BlockStoreAddr string
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	metaStoreLock      sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.metaStoreLock.Lock()
	FileInfoMap := FileInfoMap{FileInfoMap: m.FileMetaMap}
	m.metaStoreLock.Unlock()
	return &FileInfoMap, nil

}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.metaStoreLock.Lock()
	currentMetaData, ok := m.FileMetaMap[fileMetaData.Filename]
	m.metaStoreLock.Unlock()
	newVersion := fileMetaData.Version
	if ok {
		currentVersion := currentMetaData.Version
		if (newVersion - currentVersion) != 1 {
			return &Version{Version: -1}, nil // error , version conflict
		} else { //correct version number , update map
			m.metaStoreLock.Lock()
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			m.metaStoreLock.Unlock()
			return &Version{Version: newVersion}, nil
		}
	} else { // add new file to the map
		m.metaStoreLock.Lock()
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		m.metaStoreLock.Unlock()
		return &Version{Version: newVersion}, nil
	}
}

// given a list of blockhashes, find out which block server they belong to
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	//fmt.Println("reaching here whitin rpc")
	blockstoreMap := make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		serverAddr := m.ConsistentHashRing.GetResponsibleServer(hash)
		if _, ok := blockstoreMap[serverAddr]; ok {
			blockstoreMap[serverAddr].Hashes = append(blockstoreMap[serverAddr].Hashes, hash)
		} else {
			var emptyHash []string
			blockstoreMap[serverAddr] = &BlockHashes{Hashes: emptyHash}
			blockstoreMap[serverAddr].Hashes = append(blockstoreMap[serverAddr].Hashes, hash)
		}
	}
	return &BlockStoreMap{BlockStoreMap: blockstoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	adds := BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}
	return &adds, nil

}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
