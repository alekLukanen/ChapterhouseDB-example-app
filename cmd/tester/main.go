package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/alekLukanen/ChapterhouseDB-example-app/app"
	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/ChapterhouseDB/tasker"
	arrowops "github.com/alekLukanen/arrow-ops"
	"github.com/alekLukanen/errs"
)

type DBValidationResp struct {
	IsValid bool `db:"is_valid"`
}

type DBValidationMismatchResp struct {
	IsValid       bool `db:"is_valid"`
	MismatchCount int  `db:"mismatch_count"`
}

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

	table1Dataset := app.NewMediumRandomTable1Dataset()
	IntsertTupleOnInterval(
		ctx,
		logger,
		tableRegistry,
		1*time.Second,
		table1Dataset,
		"table1",
	)

	logger.Info("waiting for data to finish processing...")
	time.Sleep(10 * time.Second)

	table2Dataset := app.NewMediumRandomTable2Dataset()
	IntsertTupleOnInterval(
		ctx,
		logger,
		tableRegistry,
		1*time.Second,
		table2Dataset,
		"table2",
	)

	table1Dataset = app.NewMediumRandomTable1Dataset()
	err = ValidateData(ctx, logger, table1Dataset, "table1")
	if err != nil {
		logger.Error("data validation failed", slog.String("error", err.Error()))
	} else {
		logger.Info("the data was properly written to the warehouse")
	}

	table2Dataset = app.NewMediumRandomTable2Dataset()
	err = ValidateData(ctx, logger, table2Dataset, "table2")
	if err != nil {
		logger.Error("data validation failed", slog.String("error", err.Error()))
	} else {
		logger.Info("the data was properly written to the warehouse")
	}

	for {
		logger.Info("done running the test; waiting forever...")
		time.Sleep(5 * time.Second)
	}
}

func ValidateData(ctx context.Context, logger *slog.Logger, dataset app.Dataset, tableName string) error {

	logger.Info("waiting for the data to finish processing......")

	// wait for the data to finish processing
	warehouse, err := app.BuildWarehouse(ctx, logger)
	if err != nil {
		logger.Error("warehouse creation failed", slog.String("error", err.Error()))
		return err
	}

	itersEmpty := 0
	for {
		ln, err := warehouse.Tasker.QueueLength(ctx, "tuple-processing")
		if err != nil {
			logger.Error("unable to get the length of the tuple processing queue", slog.String("error", err.Error()))
		} else {
			logger.Info(fmt.Sprintf("tuple-processing queue length: %d", ln))
		}

		if ln == 0 {
			itersEmpty++
		}
		if itersEmpty == 3 {
			logger.Info("the producer has stopped producing data; breaking out of wait loop")
			break
		}
		time.Sleep(1 * time.Second)
	}

	time.Sleep(3 * time.Second)
	logger.Info(fmt.Sprintf("validating the data consumed by the workers for table %s", tableName))

	tmpDir, err := os.MkdirTemp("", "ValidateData")
	if err != nil {
		return err
	}

	// write all of the test data to a temporary directory
	mem := memory.NewGoAllocator()
	idx := 0
	for !dataset.Done() {
		fp := filepath.Join(tmpDir, fmt.Sprintf("d%d.parquet", idx))
		rec := dataset.BuildRecord(mem)

		forErr := arrowops.WriteRecordToParquetFile(ctx, mem, rec, fp)
		if forErr != nil {
			rec.Release()
			return forErr
		}
		rec.Release()
		idx++
	}
	defer os.RemoveAll(tmpDir)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer db.Close()

	// register the s3 credentials
	_, err = db.Exec(`
  INSTALL httpfs;
  LOAD httpfs;
  create secret locals3mock3 (
    TYPE S3,
    KEY_ID "minioadmin",
    SECRET "minioadmin",
    ENDPOINT "pi0:30006",
    URL_STYLE "path",
    USE_SSL false
  );`)
	if err != nil {
		return err
	}

	xdb := sqlx.NewDb(db, "duckdb")
	defer xdb.Close()

	// basic validation for duplicated data
	// since column1 is the primary key on the table
	// we should never have more than one row
	// for each value in column1
	query := `select count(column1) = 0 as is_valid from(
      select column1 from 's3://chdb-test-warehouse/chdb/table-state/part-data/%s/*/*.parquet' 
      group by column1 
      having count(*) > 1 
      order by column1
    );`
	query = fmt.Sprintf(query, tableName)
	var dbResp DBValidationResp
	err = xdb.Get(&dbResp, query)
	if err != nil {
		return err
	}
	if !dbResp.IsValid {
		return fmt.Errorf("one or more ids in column1 have been duplicated")
	}

	dataPattern := fmt.Sprintf("%s/*.parquet", tmpDir)

	query = `
WITH 
  window_func_rows AS (
    SELECT 
      *,
      row_number() OVER (PARTITION BY column1 ORDER BY sampleId DESC) AS row_num
    FROM read_parquet('%s')
  ),
  mismatched_rows AS (
    SELECT t1.column1, t1.column2, t1.column3, t2.column2 AS t2_column2, t2.column3 AS t2_column3
    FROM read_parquet('s3://chdb-test-warehouse/chdb/table-state/part-data/%s/*/*.parquet') t1
    LEFT JOIN (
        SELECT
            column1, column2, column3
        FROM window_func_rows subquery
        WHERE row_num = 1
    ) t2 ON t1.column1 = t2.column1
    WHERE t2.column1 IS NULL
          OR t1.column2 != t2.column2 
          OR t1.column3 != t2.column3
    )
SELECT COUNT(*) = 0 AS is_valid, COUNT(*) AS mismatch_count
FROM mismatched_rows;
`
	query = fmt.Sprintf(query, dataPattern, tableName)

	var dbMismatchResp DBValidationMismatchResp
	err = xdb.Get(&dbMismatchResp, query)
	if err != nil {
		return err
	}
	if !dbMismatchResp.IsValid {
		return fmt.Errorf("the dataset in object storage has %d rows that do not match the expected data", dbMismatchResp.MismatchCount)
	}

	return nil

}

func IntsertTupleOnInterval(
	ctx context.Context,
	logger *slog.Logger,
	tableRegistry *operations.TableRegistry,
	interval time.Duration,
	dataset app.Dataset,
	tableName string,
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
	defer keyStorage.Close()

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
		logger.Error("unable to build the tasker", slog.String("error", err.Error()))
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

	table, err := tableRegistry.GetTable(tableName)
	if err != nil {
		logger.Error("unable to find table in registry", slog.String("error", err.Error()))
		return
	}
	sub := table.SubscriptionGroups()[0].Subscriptions()[0]

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for !dataset.Done() {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Insert Tuple
			logger.Info("interting tuples")
			rec := dataset.BuildRecord(mem)
			insertErr := inserter.InsertTuples(ctx, table.TableName(), sub.SourceName(), rec)
			if insertErr != nil {
				logger.Error("failed to insert tuple", slog.String("error", insertErr.Error()))
			}
			// prepare for next iteration
			rec.Release()
		}
	}

}
