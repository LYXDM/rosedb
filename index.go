package rosedb

import (
	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/server"
	"github.com/flower-corp/rosedb/util"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func (db *RoseDB) buildIndex(dataType server.DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case server.String:
		db.buildStrsIndex(ent, pos)
	case server.List:
		db.buildListIndex(ent, pos)
	case server.Hash:
		db.buildHashIndex(ent, pos)
	case server.Set:
		db.buildSetsIndex(ent, pos)
	case server.ZSet:
		db.buildZSetIndex(ent, pos)
	}
}

func (db *RoseDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildListIndex(ent *logfile.LogEntry, pos *valuePos) {
	var listKey = ent.Key
	if ent.Type != logfile.TypeListMeta {
		listKey, _ = db.decodeListKey(ent.Key)
	}
	idxTree := db.listIndex.GetTreeWithNew(listKey)
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		idxTree.Delete(ent.Key)
		return
	}
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildHashIndex(ent *logfile.LogEntry, pos *valuePos) {
	key, field := db.decodeKey(ent.Key)
	idxTree := db.hashIndex.GetTreeWithNew(key)

	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		idxTree.Delete(field)
		return
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(field, idxNode)
}

func (db *RoseDB) buildSetsIndex(ent *logfile.LogEntry, pos *valuePos) {
	idxTree := db.setIndex.GetTreeWithNew(ent.Key)

	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		idxTree.Delete(ent.Value)
		return
	}

	if err := db.setIndex.murhash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(sum, idxNode)
}

func (db *RoseDB) buildZSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.zsetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))

		if db.zsetIndex.GetTree(ent.Key) != nil {
			db.zsetIndex.GetTree(ent.Key).Delete(ent.Value)
		}
		return
	}

	key, scoreBuf := db.decodeKey(ent.Key)
	score, _ := util.StrToFloat64(string(scoreBuf))
	if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	idxTree := db.zsetIndex.GetTreeWithNew(key)

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	idxTree.Put(sum, idxNode)
}

func (db *RoseDB) loadIndexFromLogFiles() error {
	iterateAndHandle := func(dataType server.DataType, wg *sync.WaitGroup) {
		defer wg.Done()

		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *logfile.LogFile
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}
			if logFile == nil {
				logger.Fatalf("log file is nil, failed to open db")
				continue
			}

			var offset int64
			for {
				entry, esize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					logger.Fatalf("read log entry from file err, failed to open db")
				}
				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += esize
			}
			// set latest log file`s WriteAt.
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(int(server.ZSet + 1))
	for i := server.String; i <= server.ZSet; i++ {
		go iterateAndHandle(i, wg)
	}
	wg.Wait()
	return nil
}

func (db *RoseDB) updateIndexTree(idxTree art.RadixTreeInterface,
	ent *logfile.LogEntry, pos *valuePos, sendDiscard bool) error {

	var size = pos.entrySize
	if idxTree.DataType() == server.String || idxTree.DataType() == server.List {
		_, size = logfile.EncodeEntry(ent)
	}
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	oldVal, updated := idxTree.Put(ent.Key, idxNode)
	if sendDiscard {
		db.sendDiscard(oldVal, updated, idxTree.DataType())
	}
	return nil
}

// get index node info from an adaptive radix tree in memory.
func (db *RoseDB) getIndexNode(idxTree *art.AdaptiveRadixTree, key []byte) (*indexNode, error) {
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, server.ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, server.ErrKeyNotFound
	}
	return idxNode, nil
}

func (db *RoseDB) getVal(idxTree art.RadixTreeInterface, key []byte) ([]byte, error) {

	// Get index info from an adaptive radix tree in memory.
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, server.ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, server.ErrKeyNotFound
	}

	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		return nil, server.ErrKeyNotFound
	}
	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.opts.IndexMode == KeyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// In KeyOnlyMemMode, the value not in memory, so get the value from log file at the offset.
	logFile := db.getActiveLogFile(idxTree.DataType())
	if logFile.Fid != idxNode.fid {
		logFile = db.getArchivedLogFile(idxTree.DataType(), idxNode.fid)
	}
	if logFile == nil {
		return nil, server.ErrLogFileNotFound
	}

	ent, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}
	// key exists, but is invalid(deleted or expired)
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		return nil, server.ErrKeyNotFound
	}
	return ent.Value, nil
}
