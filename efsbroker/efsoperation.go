package efsbroker

import "code.cloudfoundry.org/lager"

//go:generate counterfeiter -o efsfakes/fake_operation.go . Operation

type Operation interface {
	Execute()
}

func NewProvisionOperation(underlying *broker, logger lager.Logger, fsID string) Operation {
	return &provisionOperation{underlying, logger, fsID}
}

type provisionOperation struct {
	underlying *broker
	logger     lager.Logger
	fsID       string
}

func (o *provisionOperation) Execute() {
	o.underlying.createMountTargets(o.logger, o.fsID)
}
