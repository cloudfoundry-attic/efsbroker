package efsbroker

import (
	"errors"
	"fmt"

	"time"

	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
)

//go:generate counterfeiter -o efsfakes/fake_operation.go . Operation

type Operation interface {
	Execute()
}

//go:generate counterfeiter -o efsfakes/fake_clock.go . Clock

type Clock interface {
	Sleep(d time.Duration)
}

func NewProvisionOperation(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnetIds []string, securityGroup string, underlying interface{}, clock Clock) Operation {
	return NewProvisionStateMachine(logger, instanceID, planID, efsService, efsTools, subnetIds, securityGroup, underlying.(*broker), clock)
}

func NewProvisionStateMachine(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnetIds []string, securityGroup string, underlying *broker, clock Clock) *ProvisionOperationStateMachine {
	return &ProvisionOperationStateMachine{
		instanceID,
		planID,
		efsService,
		efsTools,
		subnetIds,
		securityGroup,
		underlying,
		logger,
		clock,
		"",
		nil,
		nil,
	}
}

type ProvisionOperationStateMachine struct {
	instanceID      string
	planID          string
	efsService      EFSService
	efsTools        efsvoltools.VolTools
	subnetIds       []string
	securityGroup   string
	abroker         *broker
	logger          lager.Logger
	clock           Clock
	fsID            string
	nextState       func()
	stateAfterSleep func()
}

func (o *ProvisionOperationStateMachine) Execute() {
	o.nextState = o.Start
	for o.State() != nil {
		o.State()
	}
}

func (o *ProvisionOperationStateMachine) State() func() {
	return o.nextState
}

func (o *ProvisionOperationStateMachine) Start() {
	logger := o.logger.Session("provision-state-start")
	defer logger.Info("end")
	fsDescriptor, err := o.efsService.CreateFileSystem(&efs.CreateFileSystemInput{
		CreationToken:   aws.String(o.instanceID),
		PerformanceMode: planIDToPerformanceMode(o.planID),
	})
	if err != nil {
		logger.Error("provision-state-start-failed-to-create-fs", err)
		o.nextState = o.Finish
		return
	}
	o.fsID = *fsDescriptor.FileSystemId
	o.nextState = o.CheckForFs
}

func (o *ProvisionOperationStateMachine) CheckForFs() {
	logger := o.logger.Session("provision-state-check-fs")
	defer logger.Info("end")

	output, err := o.efsService.DescribeFileSystems(&efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(o.fsID),
	})
	if err != nil {
		logger.Error("err-getting-fs-status", err)
		o.nextState = o.Finish
		return
	}
	if len(output.FileSystems) != 1 {
		logger.Error("err-at-amazon", fmt.Errorf("AWS returned an unexpected number of filesystems: %d", len(output.FileSystems)))
		o.nextState = o.Finish
		return
	}
	if output.FileSystems[0].LifeCycleState == nil {
		logger.Error("err-at-amazon", errors.New("AWS returned an unexpected filesystem state"))
		o.nextState = o.Finish
		return
	}

	switch *output.FileSystems[0].LifeCycleState {
	case efs.LifeCycleStateAvailable:
		o.nextState = o.CreateMountTarget
		return
	default:
		o.stateAfterSleep = o.CheckForFs
		o.nextState = o.Sleep
		return
	}
}

func (o *ProvisionOperationStateMachine) CreateMountTarget() {
	logger := o.logger.Session("provision-state-create-mount-target")
	defer logger.Info("end")

	_, err := o.efsService.CreateMountTarget(&efs.CreateMountTargetInput{
		FileSystemId:   aws.String(o.fsID),
		SubnetId:       aws.String(o.subnetIds[0]),
		SecurityGroups: []*string{aws.String(o.securityGroup)},
	})

	if err != nil {
		logger.Error("failed-to-create-mounts", err)
		o.nextState = o.Finish
		return
	}

	o.nextState = o.CheckMountTarget
	return
}

func (o *ProvisionOperationStateMachine) CheckMountTarget() {
	logger := o.logger.Session("provision-state-check-mount-target")
	defer logger.Info("end")

	mtOutput, err := o.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(o.fsID),
	})
	if err != nil {
		logger.Error("err-getting-mount-target-status", err)
		o.nextState = o.Finish
		return
	}
	if len(mtOutput.MountTargets) != 1 {
		logger.Error("error-at-amazon", fmt.Errorf("AWS returned an unexpected number of mount targets: %d", len(mtOutput.MountTargets)))
		o.nextState = o.Finish
		return
	}

	logger.Info("getMountsStatus-returning: " + *mtOutput.MountTargets[0].LifeCycleState)
	switch *mtOutput.MountTargets[0].LifeCycleState {
	case efs.LifeCycleStateAvailable:
		o.nextState = o.OpenPerms
		return
	default:
		o.StateAfterSleep(o.CheckMountTarget)
		return
	}
}

func (o *ProvisionOperationStateMachine) OpenPerms() {
	logger := o.logger.Session("provision-state-get-mount-ip")
	defer logger.Info("end")

	// get mount point details from ews to return in bind response
	mtOutput, err := o.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(o.fsID),
	})
	if err != nil {
		logger.Error("err-getting-mount-target-status", err)
		o.nextState = o.Finish
		return
	}
	if len(mtOutput.MountTargets) != 1 {
		logger.Error("error-at-amazon", fmt.Errorf("AWS returned an unexpected number of mount targets: %d", len(mtOutput.MountTargets)))
		o.nextState = o.Finish
		return
	}

	if mtOutput.MountTargets[0].LifeCycleState == nil ||
		*mtOutput.MountTargets[0].LifeCycleState != efs.LifeCycleStateAvailable {
		logger.Error("mount-point-unavailable", ErrMountTargetUnavailable)
		o.nextState = o.Finish
		return
	}

	opts := map[string]interface{}{"ip": *mtOutput.MountTargets[0].IpAddress}

	resp := o.efsTools.OpenPerms(logger, efsvoltools.OpenPermsRequest{Name: o.fsID, Opts: opts})
	if resp.Err != "" {
		logger.Error("failed-to-open-mount-permissions", errors.New(resp.Err))
	}

	o.nextState = o.Finish
}

func (o *ProvisionOperationStateMachine) StateAfterSleep(stateAfterSleep func()) {
	o.stateAfterSleep = stateAfterSleep
	o.nextState = o.Sleep
}

func (o *ProvisionOperationStateMachine) Sleep() {
	o.clock.Sleep(PollingInterval)
	o.nextState = o.stateAfterSleep
}
func (o *ProvisionOperationStateMachine) Finish() {
	logger := o.logger.Session("provision-state-finish")
	defer logger.Info("end")
	o.nextState = nil
}
