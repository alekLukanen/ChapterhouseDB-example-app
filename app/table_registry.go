package app

import (
	"context"
	"log/slog"

	"github.com/alekLukanen/ChapterhouseDB-v1/elements"
	"github.com/alekLukanen/ChapterhouseDB-v1/operations"
)

func BuildTableRegistry(ctx context.Context, logger *slog.Logger) (*operations.TableRegistry, error) {

	tableRegistry := operations.NewTableRegistry(ctx, logger)

	// add all tables here
	tables := []*elements.Table{
		BuildTable1(), BuildTable2(),
	}

	err := tableRegistry.AddTables(tables...)
	if err != nil {
		return nil, err
	}

	// validate that the tables exists in the registery
	for _, tbl := range tableRegistry.Tables() {
		logger.Info("table.TableName()", slog.String("TableName", tbl.TableName()))
		logger.Info("table.Options()", slog.Any("Options", tbl.Options()))
	}

	return tableRegistry, nil

}
