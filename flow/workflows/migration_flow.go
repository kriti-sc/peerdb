package peerflow

import (
	"log/slog"
	"time"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func MigrateSchemaWorkflow(ctx workflow.Context, input *protos.MigrationConfig) error {
	logger := workflow.GetLogger(ctx)

	logger.Info("----- testing temporal migration workflow -----")

	migrateSchemaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})

	if err := workflow.ExecuteActivity(
		migrateSchemaCtx, flowable.MigrateSchemaTables, input,
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("----- failed to migrate schema tables", slog.Any("error", err))
		return err
	}

	if err := workflow.ExecuteActivity(
		migrateSchemaCtx, flowable.MigrateSchemaViews, input,
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("----- failed to migrate schema views", slog.Any("error", err))
		return err
	}

	if err := workflow.ExecuteActivity(
		migrateSchemaCtx, flowable.MigrateSchemaIndexes, input,
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("----- failed to migrate schema indexes", slog.Any("error", err))
		return err
	}

	if err := workflow.ExecuteActivity(
		migrateSchemaCtx, flowable.MigrateSchemaFunctions, input,
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("----- failed to migrate schema functions", slog.Any("error", err))
		return err
	}

	if err := workflow.ExecuteActivity(
		migrateSchemaCtx, flowable.MigrateSchemaTriggers, input,
	).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("----- failed to migrate schema triggers", slog.Any("error", err))
		return err
	}

	logger.Info("----- end testing temporal migration workflow -----")
	return nil
}
