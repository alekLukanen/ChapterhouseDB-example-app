package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/alekLukanen/ChapterhouseDB/partitionFuncs"
	arrowops "github.com/alekLukanen/arrow-ops"
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func BuildTable1() *elements.Table {
	table1 := elements.NewTable("table1").
		AddColumns(
			elements.NewColumn("column1", arrow.PrimitiveTypes.Int32),
			elements.NewColumn("column2", arrow.FixedWidthTypes.Boolean),
			elements.NewColumn("column3", arrow.PrimitiveTypes.Float64),
		).
		SetOptions(
			elements.TableOptions{
				BatchProcessingDelay: 1 * time.Second,
				BatchProcessingSize:  5000,
				MaxObjectSize:        10_000,
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

	return table1

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
	defer takenRec.Release()

	// deduplicate the record
	dedupColumns := []string{"column1"}
	dedupRec, err := arrowops.DeduplicateRecord(mem, takenRec, dedupColumns, false)
	if err != nil {
		return nil, errs.Wrap(err, fmt.Errorf("deduplicating by columns: %v", dedupColumns))
	}

	return dedupRec, nil
}
