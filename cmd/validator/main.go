package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

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

	producerStarted := false
	itersEmpty := 0
	for {
		ln, err := warehouse.Tasker.QueueLength(ctx, "tuple-processing")
		if err != nil {
			logger.Error("unable to get the length of the tuple processing queue", slog.String("error", err.Error()))
		} else {
			logger.Info(fmt.Sprintf("tuple-processing queue length: %d", ln))
		}

		if ln > 0 {
			producerStarted = true
		}
		if producerStarted && ln == 0 {
			itersEmpty++
		}
		if itersEmpty == 3 {
			logger.Info("the producer has stopped producing data; breaking out of wait loop")
			break
		}
		time.Sleep(1 * time.Second)
	}

	logger.Info("validating the data consumed by the workers")

}
