package localconfig

import (
	"os"
	"strconv"
)

const (
	Original = iota
	FoccSharp
)

type CCType int // Concurrency Control techinque

func MustGetCCType() CCType {
	return FoccSharp
}

func IsOCC() bool {
	ccType := MustGetCCType()
	return ccType == FoccSharp 
}

func LineageSupported() bool {
	return true
}

func MustGetMvStoragePath() string {
	if storagePath, ok := os.LookupEnv("MV_STORE_PATH"); ok {
		return storagePath
	} else {
		panic("STORE_PATH not set ")
	}
}

// GetFSharpTxnSpanLimitWithDefault is for focc-sharp only. Default is 10
func GetFSharpTxnSpanLimitWithDefault() uint64 {
	if limit, err := strconv.Atoi(os.Getenv("TXN_SPAN_LIMIT")); err != nil || limit <= 0 {
		return 10
	} else {
		return uint64(limit)
	}
}

func TryGetBlockSize() int {
	if blockSize, err := strconv.Atoi(os.Getenv("BLOCK_SIZE")); err != nil || blockSize <= 0 {
		return -1
	} else {
		return blockSize
	}
}
