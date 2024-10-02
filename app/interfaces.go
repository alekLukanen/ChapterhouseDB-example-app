package app

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type Dataset interface {
	Done() bool
	BuildRecord(mem *memory.GoAllocator) arrow.Record
}
