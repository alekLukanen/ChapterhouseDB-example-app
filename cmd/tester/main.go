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
	Valid bool `db:"valid"`
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

	dataset := app.NewMediumRandomDataset()
	IntsertTupleOnInterval(
		ctx,
		logger,
		tableRegistry,
		1*time.Second,
		dataset,
	)

	dataset.Reset()
	err = ValidateData(ctx, logger, dataset)
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

func ValidateData(ctx context.Context, logger *slog.Logger, dataset app.Dataset) error {

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
	logger.Info("validating the data consumed by the workers")

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
	}
	defer os.RemoveAll(tmpDir)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer db.Close()

	// register the s3 credentials
	_, err = db.Exec(`
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

	var dbResp DBValidationResp
	err = xdb.Select(
		&dbResp,
		`select !exists(column1) valid from(
      select column1 from 's3://chdb-test-warehouse/chdb/table-state/part-data/table1/*/*.parquet' 
      group by column1 
      having count(*) > 1 
      order by column1
    );
    `,
	)
	if err != nil {
		return err
	}

	return nil

}

func IntsertTupleOnInterval(
	ctx context.Context,
	logger *slog.Logger,
	tableRegistry *operations.TableRegistry,
	interval time.Duration,
	dataset app.Dataset,
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
			insertErr := inserter.InsertTuples(ctx, "table1", "external.sourceSystemTable1", rec)
			if insertErr != nil {
				logger.Error("failed to insert tuple", slog.String("error", errs.ErrorWithStack(insertErr)))
			}
			// prepare for next iteration
			rec.Release()
		}
	}

}
