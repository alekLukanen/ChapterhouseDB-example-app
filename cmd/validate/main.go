package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/arrow-ops"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	logger.Info("Running ChapterhouseDB Example App")

	ctx := context.Background()
	mem := memory.NewGoAllocator()

	objectsStorage, err := storage.NewObjectStorage(
		ctx,
		logger,
		storage.ObjectStorageOptions{
			Endpoint:     "http://localhost:9090",
			Region:       "us-west-2",
			AuthKey:      "key",
			AuthSecret:   "secret",
			UsePathStyle: true,
			AuthType:     storage.ObjectStorageAuthTypeStatic,
		},
	)
	if err != nil {
		logger.Error("failed to create object storage", slog.String("error", err.Error()))
		return
	}

	// fetch partition 0 from object storage
	tmpDir, err := os.MkdirTemp("", "validate")
	if err != nil {
		logger.Error("failed to create temp dir", slog.String("error", err.Error()))
		return
	}
	defer os.RemoveAll(tmpDir)

	bucket := "default"
	dataFileKey := "chdb/table-state/part-data/table1/0/d_1_0.parquet"
	localDataFile := filepath.Join(tmpDir, "d_3_0.parquet")
	err = objectsStorage.DownloadFile(ctx, bucket, dataFileKey, localDataFile)
	if err != nil {
		logger.Error("failed to download object", slog.String("error", err.Error()))
		return
	}

	recs, err := arrowops.ReadParquetFile(ctx, mem, localDataFile)
	if err != nil {
		logger.Error("failed to read parquet file", slog.String("error", err.Error()))
		return
	}

	for _, rec := range recs {
		fmt.Println(rec)
	}

}
