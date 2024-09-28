package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/alekLukanen/ChapterhouseDB-example-app/app"
	"github.com/alekLukanen/ChapterhouseDB/storage"
	"github.com/alekLukanen/ChapterhouseDB/tasker"
	"github.com/alekLukanen/ChapterhouseDB/warehouse"
	"github.com/alekLukanen/errs"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
	logger.Info("Running ChapterhouseDB Example App")

	ctx := context.Background()

	// create the test bucket
	objStorOpts := storage.ObjectStorageOptions{
		Endpoint:     "http://chdb-minio-api:9000",
		Region:       "us-west-2",
		AuthKey:      "minioadmin",
		AuthSecret:   "minioadmin",
		UsePathStyle: true,
		AuthType:     storage.ObjectStorageAuthTypeStatic,
	}
	objectStorage, err := storage.NewObjectStorage(ctx, logger, objStorOpts)
	if err != nil {
		logger.Error("failed to create object storage struct", slog.String("error", err.Error()))
		return
	}

	err = objectStorage.CreateBucket(ctx, "chdb-test-warehouse")
	if err != nil {
		var ifErr *types.BucketAlreadyOwnedByYou
		if errors.As(err, &ifErr) {
			logger.Info("bucket already exists")
		} else {
			logger.Error("failed to create the bucket", slog.String("error", err.Error()))
			return
		}
	}

	tableRegistry, err := app.BuildTableRegistry(ctx, logger)
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
			Address:   "chdb-keydb:6379",
			Password:  "",
			KeyPrefix: "chapterhouseDB",
		},
		storage.ObjectStorageOptions{
			Endpoint:     "http://chdb-minio-api:9000",
			Region:       "us-west-2",
			AuthKey:      "minioadmin",
			AuthSecret:   "minioadmin",
			UsePathStyle: true,
			AuthType:     storage.ObjectStorageAuthTypeStatic,
		},
		storage.ManifestStorageOptions{
			BucketName: "chdb-test-warehouse",
			KeyPrefix:  "chdb",
		},
		tasker.Options{
			KeyDBAddress:  "chdb-keydb:6379",
			KeyDBPassword: "",
			KeyPrefix:     "chapterhouseDB",
			TaskTimeout:   1 * time.Minute,
		},
	)
	if err != nil {
		logger.Error("failed to create warehouse", slog.String("error", err.Error()))
		return
	}

	err = warehouse.Run(ctx)
	if err != nil {
		logger.Error("warehouse run loop failed", slog.String("error", err.Error()))
	}

}
