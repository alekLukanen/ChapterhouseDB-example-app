package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"github.com/alekLukanen/ChapterhouseDB-example-app/app"
	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/ChapterhouseDB/tasker"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	logger.Info("Running ChapterhouseDB Example App")

	ctx := context.Background()

	tableRegistry, err := app.BuildTableRegistry(ctx, logger)
	if err != nil {
		logger.Error("unable to create the table registry", slog.String("error", err.Error()))
		return
	}

	IntsertTupleOnInterval(ctx, logger, tableRegistry, 10*time.Second, 10, 100_000)

}

func IntsertTupleOnInterval(
	ctx context.Context,
	logger *slog.Logger,
	tableRegistry *operations.TableRegistry,
	interval time.Duration,
	stopAfterNIteration int,
	maxIdValue int,
) {

	keyStorage, err := storage.NewKeyStorage(
		ctx,
		logger,
		storage.KeyStorageOptions{
			Address:   "chdb-keydb:6379",
			Password:  "",
			KeyPrefix: "chapterhouseDB",
		},
	)
	if err != nil {
		logger.Error("unable to start storage", slog.String("error", errs.ErrorWithStack(err)))
		return
	}

	tr, err := operations.BuildTasker(
		ctx,
		logger,
		tasker.Options{
			KeyDBAddress:  "chdb-keydb:6379",
			KeyDBPassword: "",
			KeyPrefix:     "chapterhouseDB",
		},
	)
	if err != nil {
		logger.Error("unable to build the tasker", slog.String("error", errs.ErrorWithStack(err)))
		return
	}

	mem := memory.NewGoAllocator()
	inserter := operations.NewInserter(
		logger,
		tableRegistry,
		keyStorage,
		tr,
		mem,
		operations.InserterOptions{
			PartitionLockDuration: 60 * time.Second,
		},
	)

	randSource := rand.NewPCG(64, 1024)
	randGen := rand.New(randSource)

	i := 0
	width := 1000
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for iter := 0; iter < stopAfterNIteration; iter++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Insert Tuple
			logger.Info("interting tuples")
			rec := BuildRecord(mem, i, width, randGen, maxIdValue)
			insertErr := inserter.InsertTuples(ctx, "table1", "external.sourceSystemTable1", rec)
			if insertErr != nil {
				logger.Error("failed to insert tuple", slog.String("error", errs.ErrorWithStack(insertErr)))
			}

			// prepare for next iteration
			rec.Release()
			i += width
		}
	}

}

func BuildRecord(mem *memory.GoAllocator, i, width int, randGen *rand.Rand, maxIdValue int) arrow.Record {

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

	genNums := make(map[int]*struct{}, width)
	genNum := func(maxVal int) int {
		for {
			val := randGen.IntN(maxVal)
			if _, ok := genNums[val]; !ok {
				genNums[val] = &struct{}{}
				return val
			}
		}
	}

	for c := i; c < i+width; c++ {
		recBuilder.Field(0).(*array.Int32Builder).Append(int32(genNum(maxIdValue)))
		// recBuilder.Field(0).(*array.Int32Builder).Append(int32(c))
		recBuilder.Field(1).(*array.BooleanBuilder).Append(c%2 == 0)
		recBuilder.Field(2).(*array.Float64Builder).Append(float64(c))
		recBuilder.Field(3).(*array.StringBuilder).Append(fmt.Sprintf("event%d", c))
	}

	return recBuilder.NewRecord()

}