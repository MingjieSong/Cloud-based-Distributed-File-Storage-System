package surfstore

import (
	context "context"
	"math"
	"sync"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation // term and fileMetaData

	metaStore    *MetaStore
	serverId     int
	majorityNum  int
	numberAgreed int
	commitResult []*Result // list of results that have been committed
	addr         string
	peers        map[int]*Peer

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

// result that sends back to client
type Result struct {
	version *Version
	err     error
}

type Peer struct {
	client     RaftSurfstoreClient
	nextIndex  int
	matchIndex int
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	s.numberAgreed = 1
	for s.numberAgreed < s.majorityNum {
		s.SendHeartbeat(context.Background(), &emptypb.Empty{})
		time.Sleep(10 * time.Microsecond)
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
	}
	return &FileInfoMap{
		FileInfoMap: s.metaStore.FileMetaMap,
	}, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	s.numberAgreed = 1
	for s.numberAgreed < s.majorityNum {
		s.SendHeartbeat(context.Background(), &emptypb.Empty{})
		time.Sleep(10 * time.Microsecond)
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	s.numberAgreed = 1
	for s.numberAgreed < s.majorityNum {
		s.SendHeartbeat(context.Background(), &emptypb.Empty{})
		time.Sleep(10 * time.Microsecond)
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	s.log = append(s.log, &UpdateOperation{ // append log to the leader
		Term:         s.term,
		FileMetaData: filemeta,
	})
	index := len(s.log) - 1
	for index >= len(s.commitResult) { // keep appending to commitResult until the current log has been committed
		s.SendHeartbeat(context.Background(), &emptypb.Empty{})
		time.Sleep(10 * time.Microsecond)
	}
	s.SendHeartbeat(context.Background(), &emptypb.Empty{})
	return s.commitResult[index].version, s.commitResult[index].err
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	// check 1
	if s.term > input.Term {
		return &AppendEntryOutput{
			ServerId:     int64(s.serverId),
			Term:         s.term,
			Success:      false,
			MatchedIndex: 0,
		}, nil
	}
	if input.Term > s.term {
		s.term = input.Term
	}
	s.isLeaderMutex.RLock()
	s.isLeader = false
	s.isLeaderMutex.RUnlock()

	//check 2
	if input.PrevLogIndex != -1 && (len(s.log) <= int(input.PrevLogIndex) || s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		return &AppendEntryOutput{
			ServerId:     int64(s.serverId),
			Term:         s.term,
			Success:      false,
			MatchedIndex: 0,
		}, nil
	}
	// check 3, 4
	s.log = append(s.log[:input.PrevLogIndex+1], input.Entries...)

	//update commitResult and act newState operation on the follower node
	if int(input.LeaderCommit) >= len(s.commitResult) {
		// check 5
		end := math.Min(float64(input.LeaderCommit+1), float64(len(s.log)))
		for _, act := range s.log[len(s.commitResult):int(end)] {
			version, err := s.metaStore.UpdateFile(context.Background(), act.FileMetaData)
			s.commitResult = append(s.commitResult, &Result{
				version: version,
				err:     err,
			})
		}
	}

	return &AppendEntryOutput{
		ServerId:     int64(s.serverId),
		Term:         s.term,
		Success:      true,
		MatchedIndex: int64(len(s.log)) - 1,
	}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.RLock()
	s.isLeader = true
	s.isLeaderMutex.RUnlock()
	s.term += 1
	for _, peer := range s.peers {
		peer.matchIndex = -1
		peer.nextIndex = len(s.log)
	}
	return &Success{
		Flag: true,
	}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	s.numberAgreed = 1
	for _, peer := range s.peers {
		// initailize prevTerm for prevLogIndex (the prevTerm is leader's term and the )
		var prevTerm int64 = -1
		prevIndex := int64(peer.nextIndex) - 1
		if peer.nextIndex-1 >= 0 {
			prevTerm = s.log[peer.nextIndex-1].Term
		}
		response, err := peer.client.AppendEntries(context.Background(), &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      s.log[peer.nextIndex:],
			LeaderCommit: int64(len(s.commitResult)) - 1,
		})
		if err != nil {
			continue
		}
		// current server steps from leader to follower, return false
		// 1. Reply false if term < currentTerm (§5.1)
		if response.Term > s.term {
			s.isLeader = false
			return &Success{
				Flag: false,
			}, nil
		}
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
		// matches prevLogTerm (§5.3) in what situation would this happen?
		if !response.Success {
			peer.nextIndex -= 1
			continue
		}
		peer.matchIndex = int(response.MatchedIndex)
		peer.nextIndex = peer.matchIndex + 1
		s.numberAgreed += 1
	}
	//&& len(s.log) >= 1 && s.log[len(s.log)-1].Term == s.term
	if s.numberAgreed >= s.majorityNum && len(s.log) >= 1 && s.log[len(s.log)-1].Term == s.term {
		for _, act := range s.log[len(s.commitResult):] {
			version, err := s.metaStore.UpdateFile(context.Background(), act.FileMetaData)
			s.commitResult = append(s.commitResult, &Result{
				version: version,
				err:     err,
			})
		}
	}
	return &Success{
		Flag: true,
	}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
