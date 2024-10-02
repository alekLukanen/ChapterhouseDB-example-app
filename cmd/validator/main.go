package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/alekLukanen/ChapterhouseDB-example-app/app"
	arrowops "github.com/alekLukanen/arrow-ops"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	logger.Info("Running ChapterhouseDB Example App")
	ctx := context.Background()

	dataset := app.NewMediumRandomDataset()

	// write all of the test data to a temporary directory
	mem := memory.NewGoAllocator()
	idx := 0
	for !dataset.Done() {
		fp := filepath.Join("./dDir", fmt.Sprintf("d%d.parquet", idx))
		rec := dataset.BuildRecord(mem)

		forErr := arrowops.WriteRecordToParquetFile(ctx, mem, rec, fp)
		if forErr != nil {
			rec.Release()
			fmt.Println(forErr)
			return
		}
		rec.Release()
		idx++
	}

}
