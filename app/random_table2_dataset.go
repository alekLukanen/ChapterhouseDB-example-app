package app

import (
	"fmt"
	"math/rand/v2"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type RandomTable2Dataset struct {
	idx                 int
	iterationsCompleted int

	rowsPerRecord int
	maxIterations int
	maxIdValue    int

	randGen *rand.Rand
	genNums map[int]struct{}
}

func NewRandomTable2Dataset(rowsPerRecord, maxIdValue, maxIterations int) *RandomTable2Dataset {
	return &RandomTable2Dataset{
		idx:                 0,
		iterationsCompleted: 0,
		rowsPerRecord:       rowsPerRecord,
		maxIdValue:          maxIdValue,
		maxIterations:       maxIterations,
		randGen:             rand.New(rand.NewPCG(64, 1024)),
		genNums:             make(map[int]struct{}, rowsPerRecord*maxIterations),
	}
}

func NewMediumRandomTable2Dataset() *RandomTable2Dataset {
	return NewRandomTable2Dataset(1000, 100_000, 10)
}

func (obj *RandomTable2Dataset) genRandNum(maxVal int) int {
	for {
		val := obj.randGen.IntN(maxVal)
		if _, ok := obj.genNums[val]; !ok {
			obj.genNums[val] = struct{}{}
			return val
		}
	}
}

func (obj *RandomTable2Dataset) Done() bool {
	return obj.iterationsCompleted >= obj.maxIterations
}

func (obj *RandomTable2Dataset) BuildRecord(mem *memory.GoAllocator) arrow.Record {

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "column1", Type: &arrow.StringType{}},
			{Name: "column2", Type: &arrow.BooleanType{}},
			{Name: "column3", Type: &arrow.Float64Type{}},
			{Name: "eventName", Type: &arrow.StringType{}},
			{Name: "sampleId", Type: &arrow.Int32Type{}},
		}, nil,
	)
	recBuilder := array.NewRecordBuilder(mem, schema)
	defer recBuilder.Release()

	for c := obj.idx; c < obj.idx+obj.rowsPerRecord; c++ {
		strId := fmt.Sprintf("string-id-%d", obj.genRandNum(obj.maxIdValue))
		recBuilder.Field(0).(*array.StringBuilder).Append(strId)
		// recBuilder.Field(0).(*array.Int32Builder).Append(int32(c))
		recBuilder.Field(1).(*array.BooleanBuilder).Append(c%2 == 0)
		recBuilder.Field(2).(*array.Float64Builder).Append(float64(c))
		recBuilder.Field(3).(*array.StringBuilder).Append(fmt.Sprintf("event%d", c))
		recBuilder.Field(4).(*array.Int32Builder).Append(int32(c))
	}

	obj.idx += obj.rowsPerRecord
	obj.iterationsCompleted++

	return recBuilder.NewRecord()

}
