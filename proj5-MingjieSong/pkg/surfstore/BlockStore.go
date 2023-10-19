package surfstore

import (
	context "context"
	"fmt"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap     map[string]*Block
	blockmapLock sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.blockmapLock.Lock()
	if data, ok := bs.BlockMap[blockHash.Hash]; ok {
		bs.blockmapLock.Unlock()
		return data, nil
	}
	bs.blockmapLock.Unlock()
	return nil, fmt.Errorf("Request block does not exist in blockmap")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hashString := GetBlockHashString(block.BlockData)
	bs.blockmapLock.Lock()
	bs.BlockMap[hashString] = block
	bs.blockmapLock.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesOut := []string{}
	for _, blockHash := range blockHashesIn.Hashes {
		bs.blockmapLock.Lock()
		if _, ok := bs.BlockMap[blockHash]; ok {
			bs.blockmapLock.Unlock()
			blockHashesOut = append(blockHashesOut, blockHash)
		} else {
			bs.blockmapLock.Unlock()
		}

	}
	return &BlockHashes{Hashes: blockHashesOut}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	var hashes []string
	for hash, _ := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
