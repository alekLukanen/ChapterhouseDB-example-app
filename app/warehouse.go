package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/alekLukanen/ChapterhouseDB-v1/storage"
	"github.com/alekLukanen/ChapterhouseDB-v1/tasker"
	"github.com/alekLukanen/ChapterhouseDB-v1/warehouse"
	"github.com/alekLukanen/errs"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func BuildWarehouse(ctx context.Context, logger *slog.Logger) (*warehouse.Warehouse, error) {
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
		return nil, err
	}

	err = objectStorage.CreateBucket(ctx, "chdb-test-warehouse")
	if err != nil {
		var ifErr *types.BucketAlreadyOwnedByYou
		if errors.As(err, &ifErr) {
			logger.Info("bucket already exists")
		} else {
			logger.Error("failed to create the bucket", slog.String("error", err.Error()))
			return nil, err
		}
	}

	tableRegistry, err := BuildTableRegistry(ctx, logger)
	if err != nil {
		logger.Error("failed to build table registry", slog.String("error", errs.ErrorWithStack(err)))
		return nil, err
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
		return nil, err
	}

	return warehouse, nil

}
