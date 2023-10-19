package surfstore

import (
	"errors"
	"io/ioutil"
	"log"
	"math"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	baseDir := client.BaseDir
	blockSize := client.BlockSize
	// get all block store server addresses
	blockStoreServerAddrs := &BlockStoreAddrs{}
	blockAddrErr := client.GetBlockStoreAddrs(&blockStoreServerAddrs.BlockStoreAddrs)
	if blockAddrErr != nil {
		log.Printf("Error when gettting block store address %v", blockAddrErr)
	}

	// get local files, get local indexes from index.db, get remote index from metaStore server, compare difference

	/* Step1
	local has new file(compare index.db with local files) - > update index.db, update remote index, then update blockstore
	(1) UpdateFile needs to return the valid version before we can update block data
	(2) for conflict, UpdateFile returns -1. so bring the current version on the server to the local dir and index.db
	*/

	/* Step2
	server side has new file (compare index.db with remote index) - > update index.db and local files
	*/

	// Read files from base directory
	files, readDirErr := ioutil.ReadDir(baseDir)
	if readDirErr != nil {
		log.Printf("Error when reading files in base directory %v", readDirErr)
	}

	// Check if index.db exist
	isExist := false
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			isExist = true
		}
	}

	localIndex := make(map[string]*FileMetaData)
	if isExist {
		//if exist,  get localIndex
		var localIndexErr error
		localIndex, localIndexErr = LoadMetaFromMetaFile(baseDir)
		if localIndexErr != nil {
			log.Printf("Error when loading local meta file %v ", localIndexErr)
		}
	} else {
		//if not exist, create index.db and initialize it with empty map and set localIndex to empty
		initLocalIndexErr := WriteMetaFile(localIndex, baseDir)
		if initLocalIndexErr != nil {
			log.Printf("Error when initializing local index with empty map %v ", initLocalIndexErr)
		}

	}

	//Compare local files with local index, update local index record and get a list of files that need to upload
	filesToUpload := []string{}
	for _, file := range files {
		filename := file.Name()
		if (filename != DEFAULT_META_FILENAME) && (!file.IsDir()) {
			// Get file local meta data
			fileLocalMeta, ok := localIndex[filename]
			// calculate file's hashlist
			filePath := ConcatPath(baseDir, filename)
			data, err := ioutil.ReadFile(filePath)
			if err != nil {
				log.Printf("Error when reading file %v", err)
			}
			blockNums := int(math.Ceil(float64(len(data)) / float64(blockSize)))
			hashList := []string{}
			for i := 0; i < blockNums; i++ {
				block := data[i*blockSize : int(math.Min(float64((i+1)*blockSize), float64(len(data))))]
				hashString := GetBlockHashString(block)
				hashList = append(hashList, hashString)
			}
			//Compare hashlist with local meta data
			if ok {
				//File is in local index
				//Check if File has uncommited changes
				if !CheckUncommitedChanges(hashList, fileLocalMeta.BlockHashList) {
					filesToUpload = append(filesToUpload, filename)
					// here updates all the hash value inside the hashlist (better way: only update the hash value that changes )
					fileLocalMeta.BlockHashList = hashList
					//localIndex[filename] = &FileMetaData{Filename: filename, Version: fileLocalMeta.Version, BlockHashList: hashList}
				}
			} else {
				// File is not in local index
				filesToUpload = append(filesToUpload, filename)
				localIndex[filename] = &FileMetaData{Filename: filename, Version: 0, BlockHashList: hashList}
			}

		}
	}

	// Check if there is any files got deleted and then update local index
	// File exists in local index but is not in the base directory
	filesDeleted := []string{}
	for filename, _ := range localIndex {
		path := ConcatPath(baseDir, filename)
		_, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) && !CheckUncommitedChanges(localIndex[filename].BlockHashList, []string{"0"}) {
			filesDeleted = append(filesDeleted, filename)
			//localIndex[filename].BlockHashList = []string{"0"}
		}

	}

	//Get list of files that need to be downloaded
	// file exists in the remote index but not in the local index
	remoteIndex := make(map[string]*FileMetaData)
	err := client.GetFileInfoMap(&remoteIndex)
	//fmt.Println("is the remote correct? ", remoteIndex)
	if err != nil {
		log.Printf("Error in GetFileInfoMap %v", err)
	}

	filesToDownload := []string{}
	for filename, remoteFileMeta := range remoteIndex {
		localFileMeta, ok := localIndex[filename]
		if ok {
			if localFileMeta.Version < remoteFileMeta.Version {
				filesToDownload = append(filesToDownload, filename)
				isInUpload, idx1 := ContainString(filename, filesToUpload)
				isInDelete, idx2 := ContainString(filename, filesDeleted)
				if isInUpload {
					filesToUpload = append(filesToUpload[:idx1], filesToUpload[idx1+1:]...)
				}
				if isInDelete {
					filesDeleted = append(filesDeleted[:idx2], filesDeleted[idx1+2:]...)
				}
			}
		} else {
			filesToDownload = append(filesToDownload, filename)
		}
	}

	//Download files
	for _, filename := range filesToDownload {
		newMeta, err := DownloadFile(client, filename, remoteIndex[filename], baseDir)
		if err != nil {
			log.Printf("Received error downloading file %v", err)
		} else {
			localIndex[filename] = &newMeta
		}

	}

	//Upload files
	for _, filename := range filesToUpload {
		newMeta, err := UploadFile(client, filename, localIndex[filename], baseDir, blockSize)
		if err != nil {
			log.Printf("Received error downloading file %v", err)
		} else {
			localIndex[filename] = &newMeta
		}
	}
	remoteIndex = make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remoteIndex)

	//Delete files
	for _, filename := range filesDeleted {
		newMeta, err := DeleteFile(client, filename, remoteIndex, localIndex[filename], baseDir)
		if err != nil {
			log.Printf("Received error when deleting file %v", err)
		} else {
			localIndex[filename] = &newMeta
		}
	}

	//write localIndex to the index.db
	err = WriteMetaFile(localIndex, baseDir)
	if err != nil {
		log.Printf("Received error writing local index %v", err)
	}

}

func getKeyByValue(m map[string][]string, value string) (key string, ok bool) {
	for k, v := range m {
		for _, hash := range v {
			if hash == value {
				return k, true
			}
		}

	}
	return "", false
}
func DownloadFile(client RPCClient, filename string, remoteMeta *FileMetaData, baseDir string) (FileMetaData, error) {
	remoteList := remoteMeta.BlockHashList
	localMeta := FileMetaData{}
	if CheckUncommitedChanges(remoteList, []string{"0"}) {
		localMeta = *remoteMeta
		err := os.Remove(ConcatPath(baseDir, filename))
		if err != nil {
			return localMeta, err
		}
		return localMeta, nil
	}
	writesToFile := []byte{}
	// Get blocks via rpc call
	bsMap := make(map[string][]string)          // blockServerAddr: blockhashes
	client.GetBlockStoreMap(remoteList, &bsMap) // wrong, order matters here
	//fmt.Println("remote hashList", remoteList)
	for _, hashString := range remoteList {
		block := &Block{}
		blockAdd, ok := getKeyByValue(bsMap, hashString)
		//fmt.Println("is it returns ok? ", ok)
		if ok {
			err := client.GetBlock(hashString, blockAdd, block)
			if err != nil {
				log.Printf("Error when getting block %v", err)
			}
			writesToFile = append(writesToFile, block.BlockData...)

		}
	}
	// bsMap := make(map[string][]string) // blockServerAddr: blockhashes
	// client.GetBlockStoreMap(remoteList, &bsMap)
	// for addr, hashes := range bsMap {
	// 	for _, hash := range hashes {
	// 		block := &Block{}
	// 		err := client.GetBlock(hash, addr, block)
	// 		if err != nil {
	// 			log.Printf("Error when getting block %v", err)
	// 		}
	// 		writesToFile = append(writesToFile, block.BlockData...)
	// 	}
	// }

	// Write out file in base_dir
	localMeta = *remoteMeta
	path := ConcatPath(baseDir, filename)
	err := ioutil.WriteFile(path, writesToFile, 0644)
	if err != nil {
		log.Printf("Error when writing to file %v", err)
	}
	return localMeta, err
}

func UploadFile(client RPCClient, filename string, localMeta *FileMetaData,
	baseDir string, blockSize int) (FileMetaData, error) {
	path := ConcatPath(baseDir, filename)
	data, err := ioutil.ReadFile(path) // read all the data from the file
	if err != nil {
		log.Printf("Received error while reading file %v", err)
	}
	blockNums := int(math.Ceil(float64(len(data)) / float64(blockSize)))

	//Get blocks and map them to hash strings from file's local meta
	//at this point, localIndex should be the same as local files, hashlist in the localIndex has been updated
	// map (hash-value, block data)
	hashStringToBlock := make(map[string]*Block)
	for i := 0; i < blockNums; i++ {
		hashString := localMeta.BlockHashList[i]
		block := data[i*blockSize : int(math.Min(float64((i+1)*blockSize), float64(len(data))))]
		hashStringToBlock[hashString] = &Block{BlockData: block, BlockSize: int32(len(block))}
	}

	// update remote index
	var serverVersion int32
	localMeta.Version = localMeta.Version + 1
	updateFileErr := client.UpdateFile(localMeta, &serverVersion)
	if updateFileErr != nil {
		log.Printf("Received err updating remote index %v", err)
	}

	// if version number is -1, download the server version
	// else Put blocks into blockstore
	if serverVersion == -1 {
		remoteIndex := make(map[string]*FileMetaData)
		err = client.GetFileInfoMap(&remoteIndex)
		if err != nil {
			log.Printf("Received error in GetFileInfoMap %v", err)
		}
		newMeta, err := handleConflict(client, remoteIndex, filename, baseDir)
		if err != nil {
			log.Printf("Error when handling conflicts %v", err)
		}
		return newMeta, nil

	} else { // else Put blocks into blockstore

		bsMap := make(map[string][]string) // blockServerAddr: blockhashes
		//fmt.Println("localMeta.BlockHashList ", localMeta.BlockHashList)
		client.GetBlockStoreMap(localMeta.BlockHashList, &bsMap)
		//fmt.Println("bdMap: ", bsMap)
		for addr, hashes := range bsMap {
			//fmt.Println("reaching here!!!!!!!!")
			//Get hashes for file that are in blockstore
			unchangedBlocks := []string{} // the block that already exists in the block server
			err = client.HasBlocks(hashes, addr, &unchangedBlocks)
			if err != nil {
				log.Printf("Received error while getting hashstrings in blockstore %v ", err)
			}
			for _, hash := range hashes {
				flag, _ := ContainString(hash, unchangedBlocks)
				if !flag {
					success := &Success{Flag: false}
					//fmt.Println("also reaching here!!!!!!!!")
					err = client.PutBlock(hashStringToBlock[hash], addr, &success.Flag)
					if err != nil {
						log.Printf("Received error putting block %v ", err)
					}

				}
			}

		}
		return *localMeta, nil

	}
}

func DeleteFile(client RPCClient, filename string, remoteIndex map[string]*FileMetaData, local_meta *FileMetaData,
	baseDir string) (FileMetaData, error) {
	newMeta := FileMetaData{Filename: filename, Version: local_meta.Version + 1, BlockHashList: []string{"0"}}
	var serverVersion int32

	err := client.UpdateFile(&newMeta, &serverVersion)
	if err != nil {
		log.Printf("Received err updating remote index %v", err)
	}

	// Conflict, need to download server version
	if serverVersion == -1 {
		newMeta, err = handleConflict(client, remoteIndex, filename, baseDir)
		if err != nil {
			log.Printf("Error when handling conflicts %v", err)
		}
	}
	return newMeta, err

}

// download the server version to local file
func handleConflict(client RPCClient, remoteIndex map[string]*FileMetaData, filename string, baseDir string) (FileMetaData, error) {
	err := client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Printf("Received error in GetFileInfoMap %v", err)
	}
	remoteMeta := remoteIndex[filename]
	hashList := remoteMeta.BlockHashList
	writesToFile := []byte{}
	// Get blocks via rpc call
	bsMap := make(map[string][]string)        // blockServerAddr: blockhashes
	client.GetBlockStoreMap(hashList, &bsMap) // wrong, order matters here
	//fmt.Println("remote hashList", hashList)
	for _, hashString := range hashList {
		block := &Block{}
		blockAdd, ok := getKeyByValue(bsMap, hashString)
		if ok {
			err := client.GetBlock(hashString, blockAdd, block)
			if err != nil {
				log.Printf("Error when getting block %v", err)
			}
			writesToFile = append(writesToFile, block.BlockData...)

		}
	}

	// bsMap := make(map[string][]string) // blockServerAddr: blockhashes
	// client.GetBlockStoreMap(hashList, &bsMap)
	// for addr, hashes := range bsMap {
	// 	for _, hash := range hashes {
	// 		block := &Block{}
	// 		err := client.GetBlock(hash, addr, block)
	// 		if err != nil {
	// 			log.Printf("Received error getting block %v", err)
	// 		}
	// 		writesToFile = append(writesToFile, block.BlockData...)
	// 	}
	// }

	// Write out file in base_dir
	path := ConcatPath(baseDir, filename)
	err = ioutil.WriteFile(path, writesToFile, 0644)
	if err != nil {
		log.Printf("Received error writing file locally %v", err)
	}
	return *remoteMeta, nil

}

func CheckUncommitedChanges(listA []string, listB []string) bool {
	if len(listA) != len(listB) {
		return false
	}
	for i, str := range listA {
		// hash list is ordered
		if str != listB[i] {
			return false
		}
	}
	return true
}

func ContainString(s string, arr []string) (contain bool, idx int) {
	for i, str := range arr {
		if str == s {
			return true, i
		}
	}
	return false, -1
}
