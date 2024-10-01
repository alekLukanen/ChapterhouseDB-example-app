package app

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type Dataset interface {
	Done() bool
	Reset()
	BuildRecord(mem *memory.GoAllocator) arrow.Record
}
