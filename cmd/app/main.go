package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/partitionFuncs"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/ChapterhouseDB/warehouse"
	arrowops "github.com/alekLukanen/arrow-ops"
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

	tableRegistry, err := BuildTableRegistry(ctx, logger)
	if err != nil {
		logger.Error("failed to build table registry", slog.String("error", errs.ErrorWithStack(err)))
		return
	}

	go IntsertTupleOnInterval(ctx, logger, tableRegistry, 1*time.Second)

	warehouse, err := warehouse.NewWarehouse(
		ctx,
		logger,
		"warehouse1",
		tableRegistry,
		storage.KeyStorageOptions{
			Address:   "localhost:6379",
			Password:  "",
			KeyPrefix: "chapterhouseDB",
		},
		storage.ObjectStorageOptions{
			Endpoint:     "http://localhost:9090",
			Region:       "us-west-2",
			AuthKey:      "key",
			AuthSecret:   "secret",
			UsePathStyle: true,
			AuthType:     storage.ObjectStorageAuthTypeStatic,
		},
		storage.ManifestStorageOptions{
			BucketName: "default",
			KeyPrefix:  "chdb",
		},
	)
	if err != nil {
		logger.Error("failed to create warehouse", slog.String("error", errs.ErrorWithStack(err)))
		return
	}

	warehouse.Run(ctx)

}

func BuildTableRegistry(ctx context.Context, logger *slog.Logger) (*operations.TableRegistry, error) {
	tableRegistry := operations.NewTableRegistry(ctx, logger)
	table1 := elements.NewTable("table1").
		AddColumns(
			elements.NewColumn("column1", arrow.PrimitiveTypes.Int32),
			elements.NewColumn("column2", arrow.FixedWidthTypes.Boolean),
			elements.NewColumn("column3", arrow.PrimitiveTypes.Float64),
		).
    SetOptions(
      elements.TableOptions{
        BatchProcessingDelay: 10 * time.Second,
        BatchProcessingSize:  1000,
        MaxObjectSize:        250,
      },
    ).
		AddColumnPartitions(
			elements.NewColumnPartition(
				"column1",
				partitionFuncs.NewIntegerRangePartitionOptions(1000, 10),
			),
		).
		AddSubscriptionGroups(
			elements.NewSubscriptionGroup(
				"group1",
			).
				AddSubscriptions(
					elements.NewExternalSubscription(
						"sourceSystemTable1",
						Table1Transformer,
						[]elements.Column{
							elements.NewColumn("column1", &arrow.Int32Type{}),
							elements.NewColumn("column2", &arrow.BooleanType{}),
							elements.NewColumn("column3", &arrow.Float64Type{}),
							elements.NewColumn("eventName", &arrow.StringType{}),
						},
					),
				),
		)

	err := tableRegistry.AddTables(table1)
	if err != nil {
		return nil, err
	}

	// validate that the table exists in the registery
	t1, err := tableRegistry.GetTable("table1")
	if err != nil {
		return nil, err
	}

	logger.Info("table1", slog.Any("table1", t1))
	logger.Info("table1.TableName()", slog.String("TableName", t1.TableName()))
	logger.Info("table.Options()", slog.Any("Options", t1.Options()))

	return tableRegistry, nil

}

func Table1Transformer(
	ctx context.Context,
	mem *memory.GoAllocator,
	logger *slog.Logger,
	record arrow.Record,
) (arrow.Record, error) {
	logger.Info("transforming table1 data")
	defer func() {
		logger.Info("finished transformating table1 data")
	}()

	// claim the record
	record.Retain()
	defer record.Release()

	// Transform data here
	columns := []string{"column1", "column2", "column3"}
	takenRec, err := arrowops.TakeRecordColumns(record, columns)
	if err != nil {
		return nil, errs.Wrap(err, fmt.Errorf("failed taking columns: %v", columns))
	}

	return takenRec, nil
}

func IntsertTupleOnInterval(
	ctx context.Context,
	logger *slog.Logger,
	tableRegistry *operations.TableRegistry,
	interval time.Duration) {

	keyStorage, err := storage.NewKeyStorage(
		ctx,
		logger,
		storage.KeyStorageOptions{
			Address:   "localhost:6379",
			Password:  "",
			KeyPrefix: "chapterhouseDB",
		},
	)
	if err != nil {
		logger.Error("unable to start storage", slog.String("error", errs.ErrorWithStack(err)))
		return
	}

	mem := memory.NewGoAllocator()
	inserter := operations.NewInserter(
		logger,
		tableRegistry,
		keyStorage,
		mem,
		operations.InserterOptions{
			PartitionLockDuration: 60 * time.Second,
		},
	)

  i := 0
  width := 1000
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Insert Tuple
			logger.Info("interting tuples")
			rec := BuildRecord(mem, i, width)
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

func BuildRecord(mem *memory.GoAllocator, i, width int) arrow.Record {

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

	for c := i; c < i+width; c++ {
		recBuilder.Field(0).(*array.Int32Builder).Append(int32(c))
		recBuilder.Field(1).(*array.BooleanBuilder).Append(c%2 == 0)
		recBuilder.Field(2).(*array.Float64Builder).Append(float64(c))
		recBuilder.Field(3).(*array.StringBuilder).Append(fmt.Sprintf("event%d", c))
	}

	return recBuilder.NewRecord()

}
