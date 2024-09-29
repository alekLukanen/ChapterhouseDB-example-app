package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/alekLukanen/ChapterhouseDB-example-app/app"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	logger.Info("Running ChapterhouseDB Example App")

	ctx := context.Background()

	warehouse, err := app.BuildWarehouse(ctx, logger)
	if err != nil {
		logger.Error("warehouse creation failed", slog.String("error", err.Error()))
		return
	}

	err = warehouse.Run(ctx)
	if err != nil {
		logger.Error("warehouse run loop failed", slog.String("error", err.Error()))
	}

}
