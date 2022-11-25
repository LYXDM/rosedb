package server

import (
	"math"
)

const (
	EncodeHeaderSize = 10
	InitialListSeq   = math.MaxUint32 / 2
	DiscardFilePath  = "DISCARD"
	LockFileName     = "FLOCK"
	DBName           = "rosedb-%04d"
	DefaultDB        = 0
)

// DataType Define the data structure type.
type DataType = int8

// Five different data types, support String, List, Hash, Set, Sorted Set right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)