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

type OperationState struct {
	InstanceID       string
	FsID             string
	FsState          string
	MountTargetID    string
	MountTargetState string
	MountTargetIp    string
	Err              error
}

func NewProvisionOperation(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnetIds []string, securityGroup string, clock Clock, updateCb func(*OperationState)) Operation {
	return NewProvisionStateMachine(logger, instanceID, planID, efsService, efsTools, subnetIds, securityGroup, clock, updateCb)
}

func NewProvisionStateMachine(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnetIds []string, securityGroup string, clock Clock, updateCb func(*OperationState)) *ProvisionOperationStateMachine {
	return &ProvisionOperationStateMachine{
		planID,
		efsService,
		efsTools,
		subnetIds,
		securityGroup,
		logger,
		clock,
		&OperationState{
			InstanceID:       instanceID,
			FsID:             "",
			FsState:          "",
			MountTargetID:    "",
			MountTargetState: "",
			Err:              nil},
		updateCb,
		nil,
		nil,
	}
}

type ProvisionOperationStateMachine struct {
	planID          string
	efsService      EFSService
	efsTools        efsvoltools.VolTools
	subnetIds       []string
	securityGroup   string
	logger          lager.Logger
	clock           Clock
	state           *OperationState
	updateCb        func(*OperationState)
	nextState       func()
	stateAfterSleep func()
}

func (o *ProvisionOperationStateMachine) Execute() {
	logger := o.logger.Session("execute")
	defer logger.Info("end")

	o.nextState = o.Start
	for o.State() != nil {
		o.State()()
	}
}

func (o *ProvisionOperationStateMachine) State() func() {
	return o.nextState
}

func (o *ProvisionOperationStateMachine) Start() {
	logger := o.logger.Session("provision-state-start")
	logger.Info("start")
	defer logger.Info("end")
	defer func() {
		o.updateCb(o.state)
	}()

	var fsDescriptor *efs.FileSystemDescription
	fsDescriptor, o.state.Err = o.efsService.CreateFileSystem(&efs.CreateFileSystemInput{
		CreationToken:   aws.String(o.state.InstanceID),
		PerformanceMode: planIDToPerformanceMode(o.planID),
	})
	if o.state.Err != nil {
		logger.Error("provision-state-start-failed-to-create-fs", o.state.Err)
		o.state.Err = o.state.Err
		o.nextState = o.Finish
		return
	}
	o.state.FsID = *fsDescriptor.FileSystemId
	o.nextState = o.CheckForFs
}

func (o *ProvisionOperationStateMachine) CheckForFs() {
	logger := o.logger.Session("provision-state-check-fs")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	var output *efs.DescribeFileSystemsOutput
	output, o.state.Err = o.efsService.DescribeFileSystems(&efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(o.state.FsID),
	})
	if o.state.Err != nil {
		logger.Error("err-getting-fs-status", o.state.Err)
		o.nextState = o.Finish
		return
	}
	if len(output.FileSystems) != 1 {
		o.state.Err = fmt.Errorf("AWS returned an unexpected number of filesystems: %d", len(output.FileSystems))
		logger.Error("err-at-amazon", o.state.Err)
		o.nextState = o.Finish
		return
	}
	if output.FileSystems[0].LifeCycleState == nil {
		o.state.Err = errors.New("AWS returned an unexpected filesystem state")
		logger.Error("err-at-amazon", o.state.Err)
		o.nextState = o.Finish
		return
	}

	o.state.FsState = *output.FileSystems[0].LifeCycleState

	switch o.state.FsState {
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
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	_, o.state.Err = o.efsService.CreateMountTarget(&efs.CreateMountTargetInput{
		FileSystemId:   aws.String(o.state.FsID),
		SubnetId:       aws.String(o.subnetIds[0]),
		SecurityGroups: []*string{aws.String(o.securityGroup)},
	})

	if o.state.Err != nil {
		logger.Error("failed-to-create-mounts", o.state.Err)
		o.nextState = o.Finish
		return
	}

	o.nextState = o.CheckMountTarget
	return
}

func (o *ProvisionOperationStateMachine) CheckMountTarget() {
	logger := o.logger.Session("provision-state-check-mount-target")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	var mtOutput *efs.DescribeMountTargetsOutput
	mtOutput, o.state.Err = o.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(o.state.FsID),
	})
	if o.state.Err != nil {
		logger.Error("err-getting-mount-target-status", o.state.Err)
		o.nextState = o.Finish
		return
	}
	if len(mtOutput.MountTargets) != 1 {
		o.state.Err = fmt.Errorf("AWS returned an unexpected number of mount targets: %d", len(mtOutput.MountTargets))
		logger.Error("error-at-amazon", o.state.Err)
		o.nextState = o.Finish
		return
	}

	logger.Info("getMountsStatus-returning: " + *mtOutput.MountTargets[0].LifeCycleState)
	switch *mtOutput.MountTargets[0].LifeCycleState {
	case efs.LifeCycleStateAvailable:
		o.state.MountTargetID = *mtOutput.MountTargets[0].MountTargetId
		o.state.MountTargetIp = *mtOutput.MountTargets[0].IpAddress
		o.nextState = o.OpenPerms
		return
	default:
		o.StateAfterSleep(o.CheckMountTarget)
		return
	}
}

func (o *ProvisionOperationStateMachine) OpenPerms() {
	logger := o.logger.Session("provision-state-get-mount-ip")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	opts := map[string]interface{}{"ip": o.state.MountTargetIp}

	resp := o.efsTools.OpenPerms(logger, efsvoltools.OpenPermsRequest{Name: o.state.FsID, Opts: opts})
	if resp.Err != "" {
		o.state.Err = errors.New(resp.Err)
		logger.Error("failed-to-open-mount-permissions", o.state.Err)
	}

	o.nextState = o.Finish
}

func (o *ProvisionOperationStateMachine) StateAfterSleep(stateAfterSleep func()) {
	o.stateAfterSleep = stateAfterSleep
	o.nextState = o.Sleep
}

func (o *ProvisionOperationStateMachine) Sleep() {
	logger := o.logger.Session("provision-state-sleep")
	logger.Info("start")
	defer logger.Info("end")
	o.clock.Sleep(PollingInterval)
	o.nextState = o.stateAfterSleep
}
func (o *ProvisionOperationStateMachine) Finish() {
	logger := o.logger.Session("provision-state-finish")
	logger.Info("start")
	defer logger.Info("end")
	o.nextState = nil
}
