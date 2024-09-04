package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/partitionFuncs"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/ChapterhouseDB/warehouse"
	arrowops "github.com/alekLukanen/arrow-ops"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
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
			Endpoint:     "localhost:9000",
			Region:       "us-east-1",
			AuthKey:      "",
			AuthSecret:   "",
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
		AddColumnPartitions(
			elements.NewColumnPartition(
				"column1",
				partitionFuncs.NewIntegerRangePartitionOptions(10, 10),
			),
		).
		AddSubscriptionGroups(
			elements.NewSubscriptionGroup(
				"group1",
			).
				AddSubscriptions(
					elements.NewExternalSubscription(
						"externalTable1",
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
