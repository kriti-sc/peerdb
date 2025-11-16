package peerflow

import (
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"go.temporal.io/sdk/workflow"
)

func MigrateSchemaWorkflow(ctx workflow.Context, input *protos.MigrationConfig) error {
	logger := workflow.GetLogger(ctx)

	logger.Info("----- testing temporal migration workflow -----")
	logger.Info("Source Peer: ", input.SourcePeer, "Target Peer: ", input.TargetPeer, "Flow Job Name: ", input.FlowJobName)
	logger.Info("----- end testing temporal migration workflow -----")
	return nil
}
