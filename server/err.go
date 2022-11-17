package server

import (
	"errors"
)

var (
	ErrClientIsNil = errors.New("ERR client conn is nil")

	// ErrKeyNotFound key not found
	ErrKeyNotFound = errors.New("key not found")

	// ErrLogFileNotFound log file not found
	ErrLogFileNotFound = errors.New("log file not found")

	// ErrWrongNumberOfArgs doesn't match key-value pair numbers
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")

	// ErrIntegerOverflow overflows int64 limitations
	ErrIntegerOverflow = errors.New("increment or decrement overflow")

	// ErrWrongValueType value is not a number
	ErrWrongValueType = errors.New("value is not an integer")

	// ErrWrongIndex index is out of range
	ErrWrongIndex = errors.New("index is out of range")

	// ErrGCRunning log file gc is running
	ErrGCRunning = errors.New("log file gc is running, retry later")
)
