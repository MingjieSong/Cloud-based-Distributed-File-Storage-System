package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	metaStoreLock  sync.Mutex
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

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
