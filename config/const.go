package config

import (
	"math"
)

const (
	LogFileTypeNum   = 5
	EncodeHeaderSize = 10
	InitialListSeq   = math.MaxUint32 / 2
	DiscardFilePath  = "DISCARD"
	LockFileName     = "FLOCK"
	DBName = "rosedb-%04d"
	DefaultDB = 0
)