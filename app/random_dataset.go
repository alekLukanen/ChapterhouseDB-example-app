package app

import (
	"fmt"
	"math/rand/v2"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type RandomDataset struct {
	idx                 int
	iterationsCompleted int

	rowsPerRecord int
	maxIdValue    int
	maxIterations int

	randGen *rand.Rand
}

func NewRandomDataset(rowsPerRecord, maxIdValue, maxIterations int) *RandomDataset {
	return &RandomDataset{
		idx:                 0,
		iterationsCompleted: 0,
		rowsPerRecord:       rowsPerRecord,
		maxIdValue:          maxIdValue,
		maxIterations:       maxIterations,
		randGen:             rand.New(rand.NewPCG(64, 1024)),
	}
}

func NewMediumRandomDataset() *RandomDataset {
	return NewRandomDataset(1000, 100_000, 3)
}

func (obj *RandomDataset) Done() bool {
	return obj.iterationsCompleted >= obj.maxIterations
}

func (obj *RandomDataset) Reset() {
	obj.idx = 0
	obj.iterationsCompleted = 0
}

func (obj *RandomDataset) BuildRecord(mem *memory.GoAllocator) arrow.Record {

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "column1", Type: &arrow.Int32Type{}},
			{Name: "column2", Type: &arrow.BooleanType{}},
			{Name: "column3", Type: &arrow.Float64Type{}},
			{Name: "eventName", Type: &arrow.StringType{}},
		}, nil,
	)
	recBuilder := array.NewRecordBuilder(mem, schema)
	defer recBuilder.Release()

	genNums := make(map[int]*struct{}, obj.rowsPerRecord)
	genNum := func(maxVal int) int {
		for {
			val := obj.randGen.IntN(maxVal)
			if _, ok := genNums[val]; !ok {
				genNums[val] = &struct{}{}
				return val
			}
		}
	}

	for c := obj.idx; c < obj.idx+obj.rowsPerRecord; c++ {
		recBuilder.Field(0).(*array.Int32Builder).Append(int32(genNum(obj.maxIdValue)))
		// recBuilder.Field(0).(*array.Int32Builder).Append(int32(c))
		recBuilder.Field(1).(*array.BooleanBuilder).Append(c%2 == 0)
		recBuilder.Field(2).(*array.Float64Builder).Append(float64(c))
		recBuilder.Field(3).(*array.StringBuilder).Append(fmt.Sprintf("event%d", c))
	}

	obj.idx += obj.rowsPerRecord
	obj.iterationsCompleted++

	return recBuilder.NewRecord()

}
