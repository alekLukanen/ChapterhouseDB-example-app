package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/alekLukanen/ChapterhouseDB-example-app/app"
	"github.com/alekLukanen/ChapterhouseDB/operations"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/ChapterhouseDB/tasker"
	"github.com/alekLukanen/errs"
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

	dataset := app.NewMediumRandomDataset()
	IntsertTupleOnInterval(
		ctx,
		logger,
		tableRegistry,
		1*time.Second,
		dataset,
	)

	for {
		logger.Info("done writing data to the db; waiting forever...")
		time.Sleep(5 * time.Second)
	}
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
