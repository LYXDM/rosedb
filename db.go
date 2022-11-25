package rosedb

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/flower-corp/rosedb/ds/art"
	"github.com/flower-corp/rosedb/ds/zset"
	"github.com/flower-corp/rosedb/flock"
	"github.com/flower-corp/rosedb/logfile"
	"github.com/flower-corp/rosedb/logger"
	"github.com/flower-corp/rosedb/server"
	"github.com/flower-corp/rosedb/util"
)

type (
	// RoseDB a db instance.
	RoseDB struct {
		activeLogFiles   map[server.DataType]*logfile.LogFile
		archivedLogFiles map[server.DataType]archivedFiles
		fidMap           map[server.DataType][]uint32 // only used at startup, never update even though log files changed.
		discards         map[server.DataType]*discard
		opts             Options
		strIndex         *strIndex  // String indexes(adaptive-radix-tree).
		listIndex        *listIndex // List indexes.
		hashIndex        *hashIndex // Hash indexes.
		setIndex         *setIndex  // Set indexes.
		zsetIndex        *zsetIndex // Sorted set indexes.
		mu               sync.RWMutex
		fileLock         *flock.FileLockGuard
		closed           uint32
		gcState          int32
	}

	archivedFiles map[uint32]*logfile.LogFile

	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}

	listIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTreeExpire
	}

	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTreeExpire
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTreeExpire
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewART().SetDataType(server.String), mu: new(sync.RWMutex)}
}

func newListIdx() *listIndex {
	return &listIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func (h *listIndex) GetTree(key []byte) *art.AdaptiveRadixTree {
	tree := h.trees[string(key)]
	return tree
}

func (h *listIndex) GetTreeWithNew(key []byte) *art.AdaptiveRadixTree {
	tree := h.GetTree(key)
	if tree != nil {
		return tree
	}
	tree = art.NewART()
	tree.SetDataType(server.List)
	h.trees[string(key)] = tree
	return tree
}

func newHashIdx() *hashIndex {
	return &hashIndex{trees: make(map[string]*art.AdaptiveRadixTreeExpire), mu: new(sync.RWMutex)}
}

//some method
func (h *hashIndex) GetTree(key []byte) *art.AdaptiveRadixTreeExpire {
	tree := h.trees[string(key)]
	if tree == nil {
		return nil
	}
	if tree.ExpireAt != 0 && tree.ExpireAt < time.Now().Unix() {
		delete(h.trees, string(key))
		return nil
	}
	return tree
}

func (h *hashIndex) GetTreeWithNew(key []byte) *art.AdaptiveRadixTreeExpire {
	tree := h.GetTree(key)
	if tree != nil {
		return tree
	}
	tree = art.NewARTExpire()
	tree.SetDataType(server.Hash)
	h.trees[string(key)] = tree
	return tree
}

func newSetIdx() *setIndex {
	return &setIndex{murhash: util.NewMurmur128(), trees: make(map[string]*art.AdaptiveRadixTreeExpire), mu: new(sync.RWMutex)}
}

func (s *setIndex) GetTree(key []byte) *art.AdaptiveRadixTreeExpire {
	tree := s.trees[string(key)]
	if tree == nil {
		return nil
	}
	if tree.ExpireAt != 0 && tree.ExpireAt < time.Now().Unix() {
		delete(s.trees, string(key))
		return nil
	}
	return tree
}

func (s *setIndex) GetTreeWithNew(key []byte) *art.AdaptiveRadixTreeExpire {
	tree := s.GetTree(key)
	if tree != nil {
		return tree
	}
	tree = art.NewARTExpire()
	tree.SetDataType(server.Set)
	s.trees[string(key)] = tree
	return tree
}

func newZSetIdx() *zsetIndex {
	return &zsetIndex{indexes: zset.New(), murhash: util.NewMurmur128(), trees: make(map[string]*art.AdaptiveRadixTreeExpire),
		mu: new(sync.RWMutex),
	}
}

func (z *zsetIndex) GetTree(key []byte) *art.AdaptiveRadixTreeExpire {
	tree := z.trees[string(key)]
	if tree == nil {
		return nil
	}
	if tree.ExpireAt != 0 && tree.ExpireAt < time.Now().Unix() {
		delete(z.trees, string(key))
		delete(z.indexes.Record, string(key))
		return nil
	}
	return tree
}

func (z *zsetIndex) GetTreeWithNew(key []byte) *art.AdaptiveRadixTreeExpire {
	tree := z.GetTree(key)
	if tree != nil {
		return tree
	}
	tree = art.NewARTExpire()
	tree.SetDataType(server.ZSet)
	z.trees[string(key)] = tree
	return tree
}

// Open a rosedb instance. You must call Close after using it.
func Open(opts Options) (*RoseDB, error) {
	// create the dir path if not exists.
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// acquire file lock to prevent multiple processes from accessing the same directory.
	lockPath := filepath.Join(opts.DBPath, server.LockFileName)
	lockGuard, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		activeLogFiles:   make(map[server.DataType]*logfile.LogFile),
		archivedLogFiles: make(map[server.DataType]archivedFiles),
		opts:             opts,
		fileLock:         lockGuard,
		strIndex:         newStrsIndex(),
		listIndex:        newListIdx(),
		hashIndex:        newHashIdx(),
		setIndex:         newSetIdx(),
		zsetIndex:        newZSetIdx(),
	}

	// init discard file.
	if err := db.initDiscard(); err != nil {
		return nil, err
	}

	// load the log files from disk.
	if err := db.loadLogFiles(); err != nil {
		return nil, err
	}

	// load indexes from log files.
	if err := db.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	// handle log files garbage collection.
	go db.handleLogFileGC()
	return db, nil
}

// Close db and save relative configs.
func (db *RoseDB) Close() error {
	//before stop do once gc
	db.doRunGCSync()
	db.dbFilePersist()
	return nil
}

func (db *RoseDB) dbFilePersist() {
	if db.fileLock != nil {
		_ = db.fileLock.Release()
	}
	// close and sync the active file.
	for _, activeFile := range db.activeLogFiles {
		if activeFile == nil {
			continue
		}
		_ = activeFile.Sync()
		_ = activeFile.Close()
	}
	// close the archived files.
	for _, archived := range db.archivedLogFiles {
		for _, file := range archived {
			if file == nil {
				continue
			}
			_ = file.Sync()
			_ = file.Close()
		}
	}
	// close discard channel.
	for _, dis := range db.discards {
		if dis == nil {
			continue
		}
		dis.closeChan()
	}
	atomic.StoreUint32(&db.closed, 1)
	db.strIndex = nil
	db.hashIndex = nil
	db.listIndex = nil
	db.zsetIndex = nil
	db.setIndex = nil
}

// Sync persist the db files to stable storage.
func (db *RoseDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// iterate and sync all the active files.
	for _, activeFile := range db.activeLogFiles {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	// sync discard file.
	for _, dis := range db.discards {
		if err := dis.sync(); err != nil {
			return err
		}
	}
	return nil
}

// Backup copies the db directory to the given path for backup.
// It will create the path if it does not exist.
func (db *RoseDB) Backup(path string) error {
	// if log file gc is running, can not backuo the db.
	if atomic.LoadInt32(&db.gcState) > 0 {
		return server.ErrGCRunning
	}

	if err := db.Sync(); err != nil {
		return err
	}
	if !util.PathExist(path) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return util.CopyDir(db.opts.DBPath, path)
}

// RunLogFileGC run log file garbage collection manually.
func (db *RoseDB) RunLogFileGC(dataType server.DataType, fid int, gcRatio float64) error {
	if atomic.LoadInt32(&db.gcState) > 0 {
		return server.ErrGCRunning
	}
	return db.doRunGC(dataType, fid, gcRatio)
}

func (db *RoseDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

func (db *RoseDB) getActiveLogFile(dataType server.DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}

func (db *RoseDB) getArchivedLogFile(dataType server.DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	logFiles := db.archivedLogFiles[dataType]
	if logFiles != nil {
		lf = logFiles[fid]
	}
	return lf
}

// write entry to log file.
func (db *RoseDB) writeLogEntry(ent *logfile.LogEntry, dataType server.DataType) (*valuePos, error) {
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, server.ErrLogFileNotFound
	}

	opts := db.opts
	entBuf, esize := logfile.EncodeEntry(ent)
	if activeLogFile.WriteAt+int64(esize) > opts.LogFileSizeThreshold {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}

		db.mu.Lock()
		defer db.mu.Unlock()
		// save the old log file in archived files.
		activeFileId := activeLogFile.Fid
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		// open a new log file.
		ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
		lf, err := logfile.OpenLogFile(opts.DBPath, activeFileId+1, opts.LogFileSizeThreshold, ftype, iotype)
		if err != nil {
			return nil, err
		}
		db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
		db.activeLogFiles[dataType] = lf
		activeLogFile = lf
	}

	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	// write entry and sync(if necessary)
	if err := activeLogFile.Write(entBuf); err != nil {
		return nil, err
	}
	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &valuePos{fid: activeLogFile.Fid, offset: writeAt}, nil
}

func (db *RoseDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	fileInfos, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}

	fidMap := make(map[server.DataType][]uint32)
	for _, file := range fileInfos {
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := server.DataType(logfile.FileTypesMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}
	db.fidMap = fidMap

	for dataType, fids := range fidMap {
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		// load log file in order.
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		opts := db.opts
		for i, fid := range fids {
			ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
			lf, err := logfile.OpenLogFile(opts.DBPath, fid, opts.LogFileSizeThreshold, ftype, iotype)
			if err != nil {
				return err
			}
			// latest one is active log file.
			if i == len(fids)-1 {
				db.activeLogFiles[dataType] = lf
			} else {
				db.archivedLogFiles[dataType][fid] = lf
			}
		}
	}
	return nil
}

func (db *RoseDB) initLogFile(dataType server.DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeLogFiles[dataType] != nil {
		return nil
	}
	opts := db.opts
	ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.IoType)
	lf, err := logfile.OpenLogFile(opts.DBPath, logfile.InitialLogFileId, opts.LogFileSizeThreshold, ftype, iotype)
	if err != nil {
		return err
	}

	db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
	db.activeLogFiles[dataType] = lf
	return nil
}

func (db *RoseDB) initDiscard() error {
	discardPath := filepath.Join(db.opts.DBPath, server.DiscardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[server.DataType]*discard)
	for i := logfile.Strs; i <= logfile.ZSet; i++ {
		name := logfile.FileNamesMap[i] + discardFileName
		dis, err := newDiscard(discardPath, name, db.opts.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[server.DataType(i)] = dis
	}
	db.discards = discards
	return nil
}

func (db *RoseDB) encodeKey(key, subKey []byte) []byte {
	header := make([]byte, server.EncodeHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(subKey)))
	length := len(key) + len(subKey)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf[:index], header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):], subKey)
		return buf
	}
	return header[:index]
}

func (db *RoseDB) decodeKey(key []byte) ([]byte, []byte) {
	var index int
	keySize, i := binary.Varint(key[index:])
	index += i
	_, i = binary.Varint(key[index:])
	index += i
	sep := index + int(keySize)
	return key[index:sep], key[sep:]
}

func (db *RoseDB) sendDiscard(oldVal interface{}, updated bool, dataType server.DataType) {
	if !updated || oldVal == nil {
		return
	}
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	case db.discards[dataType].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
}

func (db *RoseDB) handleLogFileGC() {
	if db.opts.LogFileGCInterval <= 0 {
		return
	}

	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	ticker := time.NewTicker(db.opts.LogFileGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&db.gcState) > 0 {
				logger.Warn("log file gc is running, skip it")
				break
			}
			db.doRunGCAsync()
		case <-quitSig:
			logger.Infof("db %s exit", db.opts.DBPath)
			return
		}
	}
}

func (db *RoseDB) doRunGCAsync() {
	for dType := server.String; dType <= server.ZSet; dType++ {
		go func(dataType server.DataType) {
			err := db.doRunGC(dataType, -1, db.opts.LogFileGCRatio)
			if err != nil {
				logger.Errorf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
			}
		}(dType)
	}
}

func (db *RoseDB) doRunGCSync() {
	for dType := server.String; dType <= server.ZSet; dType++ {
		if err := db.doRunGC(dType, -1, 0); err != nil { //all archived file do gc
			logger.Errorf("log file gc err, dataType: [%v], err: [%v]", dType, err)
		}
	}
	logger.Infof("log file gc success")
}

func (db *RoseDB) doRunGC(dataType server.DataType, specifiedFid int, gcRatio float64) error {
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)

	maybeRewriteStrs := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.strIndex.mu.Lock()
		defer db.strIndex.mu.Unlock()
		indexVal := db.strIndex.idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, server.String)
			if err != nil {
				return err
			}
			// update index
			if err = db.updateIndexTree(db.strIndex.idxTree, ent, valuePos, false); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteList := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()
		var listKey = ent.Key
		if ent.Type != logfile.TypeListMeta {
			listKey, _ = db.decodeListKey(ent.Key)
		}
		if db.listIndex.trees[string(listKey)] == nil {
			return nil
		}
		idxTree := db.listIndex.trees[string(listKey)]
		indexVal := idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, server.List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, ent, valuePos, false); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteHash := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()
		key, field := db.decodeKey(ent.Key)
		idxTree := db.hashIndex.GetTree(key)
		if idxTree == nil {
			return nil
		}
		indexVal := idxTree.Get(field)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, server.Hash)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: field, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteSets := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.setIndex.mu.Lock()
		defer db.setIndex.mu.Unlock()
		idxTree := db.setIndex.GetTree(ent.Key)
		if idxTree == nil {
			return nil
		}
		if err := db.setIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, server.Set)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteZSet := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.zsetIndex.mu.Lock()
		defer db.zsetIndex.mu.Unlock()
		key, _ := db.decodeKey(ent.Key)
		idxTree := db.zsetIndex.GetTree(key)
		if idxTree == nil {
			return nil
		}
		if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.zsetIndex.murhash.EncodeSum128()
		db.zsetIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, server.ZSet)
			if err != nil {
				return err
			}
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false); err != nil {
				return err
			}
		}
		return nil
	}

	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil
	}
	if err := db.discards[dataType].sync(); err != nil {
		return err
	}
	ccl, err := db.discards[dataType].getCCL(activeLogFile.Fid, gcRatio)
	if err != nil {
		return err
	}
	for _, fid := range ccl {
		if specifiedFid >= 0 && uint32(specifiedFid) != fid {
			continue
		}
		archivedFile := db.getArchivedLogFile(dataType, fid)
		if archivedFile == nil {
			continue
		}

		var offset int64
		for {
			ent, size, err := archivedFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}
			var off = offset
			offset += size
			if ent.Type == logfile.TypeDelete {
				continue
			}
			ts := time.Now().Unix()
			if ent.ExpiredAt != 0 && ent.ExpiredAt <= ts {
				continue
			}
			var rewriteErr error
			switch dataType {
			case server.String:
				rewriteErr = maybeRewriteStrs(archivedFile.Fid, off, ent)
			case server.List:
				rewriteErr = maybeRewriteList(archivedFile.Fid, off, ent)
			case server.Hash:
				rewriteErr = maybeRewriteHash(archivedFile.Fid, off, ent)
			case server.Set:
				rewriteErr = maybeRewriteSets(archivedFile.Fid, off, ent)
			case server.ZSet:
				rewriteErr = maybeRewriteZSet(archivedFile.Fid, off, ent)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		}

		// delete older log file.
		db.mu.Lock()
		delete(db.archivedLogFiles[dataType], fid)
		_ = archivedFile.Delete()
		db.mu.Unlock()
		// clear discard state.
		db.discards[dataType].clear(fid)
	}
	return nil
}
