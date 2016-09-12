package efsbroker

import (
	"errors"
	"fmt"

	"time"

	"strings"

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
		&OperationState{InstanceID: instanceID},
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

	o.nextState = o.CreateFs
	for o.State() != nil {
		o.State()()
	}
}

func (o *ProvisionOperationStateMachine) State() func() {
	return o.nextState
}

func (o *ProvisionOperationStateMachine) CreateFs() {
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
	o.nextState = o.CheckFs
}

func (o *ProvisionOperationStateMachine) CheckFs() {
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

	logger.Debug(fmt.Sprintf("Setting fs state to '%s'", *output.FileSystems[0].LifeCycleState))
	o.state.FsState = *output.FileSystems[0].LifeCycleState

	switch o.state.FsState {
	case efs.LifeCycleStateAvailable:
		o.nextState = o.CreateMountTarget
		return
	default:
		o.stateAfterSleep = o.CheckFs
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

	//var mtOutput *efs.DescribeMountTargetsOutput
	//mtOutput, o.state.Err = o.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
	//	FileSystemId: aws.String(o.state.FsID),
	//})
	//if o.state.Err != nil {
	//	logger.Error("err-getting-mount-target-status", o.state.Err)
	//	o.nextState = o.Finish
	//	return
	//}
	//if len(mtOutput.MountTargets) != 1 {
	//	o.state.Err = fmt.Errorf("AWS returned an unexpected number of mount targets: %d", len(mtOutput.MountTargets))
	//	logger.Error("error-at-amazon", o.state.Err)
	//	o.nextState = o.Finish
	//	return
	//}
	//
	//o.state.MountTargetState = *mtOutput.MountTargets[0].LifeCycleState

	//switch o.state.MountTargetState {
	//case efs.LifeCycleStateAvailable:
	//	o.state.MountTargetID = *mtOutput.MountTargets[0].MountTargetId
	//	o.state.MountTargetIp = *mtOutput.MountTargets[0].IpAddress
	//	o.nextState = o.OpenPerms
	//	return
	//default:
	//	o.StateAfterSleep(o.CheckMountTarget)
	//	return
	//}

	//o.state.MountTargetID = *mtOutput.MountTargets[0].MountTargetId
	//o.state.MountTargetIp = *mtOutput.MountTargets[0].IpAddress
	//o.nextState = o.OpenPerms

	//o.StateAfterSleep(o.CheckMountTarget)

	err := checkMountTarget(o.logger, o.efsService, o.state.FsID, efs.LifeCycleStateAvailable, func(mount *efs.MountTargetDescription) {
		o.state.MountTargetID = *mount.MountTargetId
		o.state.MountTargetIp = *mount.IpAddress
		o.state.MountTargetState = efs.LifeCycleStateAvailable
		o.nextState = o.OpenPerms
		return
	}, func() {
		o.StateAfterSleep(o.CheckMountTarget)
		return
	})
	if err != nil {
		o.state.Err = err
		o.nextState = o.Finish
	}
}

func checkMountTarget(logger lager.Logger, efsService EFSService, fsID string, state string, success func(mount *efs.MountTargetDescription), failure func()) error {
	logger = logger.Session("check-mount-target")
	logger.Info("start")
	defer logger.Info("end")

	mtOutput, err := efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		logger.Error("err-getting-mount-target-status", err)
		return err
	}
	if len(mtOutput.MountTargets) != 1 {
		err = fmt.Errorf("AWS returned an unexpected number of mount targets: %d", len(mtOutput.MountTargets))
		logger.Error("error-at-amazon", err)
		return err
	}

	switch *mtOutput.MountTargets[0].LifeCycleState {
	case state:
		success(mtOutput.MountTargets[0])
		return nil
	default:
		failure()
		return nil
	}
}

func (o *ProvisionOperationStateMachine) OpenPerms() {
	logger := o.logger.Session("provision-state-open-perms")
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

type DeprovisionOperationSpec struct {
	InstanceID    string
	FsID          string
	MountTargetID string
}

func NewDeprovisionOperation(logger lager.Logger, efsService EFSService, clock Clock, spec DeprovisionOperationSpec, updateCb func(*OperationState)) Operation {
	return &DeprovisionOperation{logger, efsService, clock, spec, &OperationState{InstanceID: spec.InstanceID}, updateCb}
}

func NewTestDeprovisionOperation(logger lager.Logger, efsService EFSService, clock Clock,
	spec DeprovisionOperationSpec, updateCb func(*OperationState)) *DeprovisionOperation {
	return &DeprovisionOperation{logger, efsService, clock, spec, &OperationState{InstanceID: spec.InstanceID}, updateCb}
}

type DeprovisionOperation struct {
	logger   lager.Logger
	efs      EFSService
	clock    Clock
	spec     DeprovisionOperationSpec
	state    *OperationState
	updateCb func(*OperationState)
}

func (o *DeprovisionOperation) Execute() {
	logger := o.logger.Session("deprovision-execute")
	logger.Info("start")
	defer logger.Info("end")

	defer o.updateCb(o.state)

	err := o.DeleteMountTarget(o.spec.FsID)
	if err != nil {
		o.state.Err = err
		return
	}
	o.logger.Info("mount target deleted")

	err = o.CheckMountTarget(o.spec.FsID)
	if err != nil {
		o.state.Err = err
		return
	}

	err = o.DeleteFs(o.spec.FsID)
	if err != nil {
		o.state.Err = err
		return
	}

	err = o.CheckFs(o.spec.FsID)
	if err != nil {
		o.state.Err = err
		return
	}

}

func (o *DeprovisionOperation) DeleteMountTarget(fsID string) error {
	logger := o.logger.Session("delete-mount-target")
	logger.Info("start")
	defer logger.Info("end")
	//defer o.updateCb(o.state)

	out, err := o.efs.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		logger.Error("failed-describing-mount-targets", err)
		return err
	}
	if len(out.MountTargets) < 1 {
		logger.Info("no-mount-targets")
		return nil
	}
	if len(out.MountTargets) > 1 {
		err = fmt.Errorf("Too many mount targets found, Expected 1, got %d", len(out.MountTargets))
		logger.Error("err-at-amazon", err)
		return err
	}

	if *out.MountTargets[0].LifeCycleState != efs.LifeCycleStateAvailable {
		err = errors.New("invalid lifecycle transition, please wait until all mount targets are available")
		logger.Error("non-available-mount-targets", err)
		return err
	}

	logger.Info("deleting-mount-targets", lager.Data{"target-id": *out.MountTargets[0].MountTargetId})
	_, err = o.efs.DeleteMountTarget(&efs.DeleteMountTargetInput{
		MountTargetId: out.MountTargets[0].MountTargetId,
	})
	if err != nil {
		logger.Error("failed-deleting-mount-targets", err)
		return err
	}

	return nil
}

func (o *DeprovisionOperation) CheckMountTarget(fsID string) error {
	logger := o.logger.Session("check-mount-target-deleted")
	logger.Info("start")
	defer logger.Info("end")

	for true {
		mtOutput, err := o.efs.DescribeMountTargets(&efs.DescribeMountTargetsInput{
			FileSystemId: aws.String(fsID),
		})
		if err != nil {
			logger.Error("err-getting-mount-target-status", err)
			return err
		}
		if len(mtOutput.MountTargets) < 1 || *mtOutput.MountTargets[0].LifeCycleState == efs.LifeCycleStateDeleted {
			return nil
		}
		if len(mtOutput.MountTargets) > 1 {
			err = fmt.Errorf("amazon returned unexpected number of mount targets.  Expected 1, got %d", mtOutput.MountTargets)
			logger.Error("err-at-amazon", err)
			return err
		} else {
			o.clock.Sleep(time.Millisecond * 100)
		}
	}
	return nil
}

func (o *DeprovisionOperation) DeleteFs(fsID string) error {
	logger := o.logger.Session("delete-fs")
	logger.Info("start")
	defer logger.Info("end")

	_, err := o.efs.DeleteFileSystem(&efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		o.logger.Error("failed-deleting-fs", err)
	}
	return err
}

func (o *DeprovisionOperation) CheckFs(fsID string) error {
	logger := o.logger.Session("check-fs-deleted")
	logger.Info("start")
	defer logger.Info("end")

	for true {
		output, err := o.efs.DescribeFileSystems(&efs.DescribeFileSystemsInput{
			FileSystemId: aws.String(fsID),
		})
		if err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				return nil
			} else {
				logger.Error("err-getting-fs-status", err)
				return err
			}
		}
		if len(output.FileSystems) != 1 {
			return fmt.Errorf("AWS returned an unexpected number of filesystems: %d", len(output.FileSystems))
		}
		if output.FileSystems[0].LifeCycleState == nil {
			return errors.New("AWS returned an unexpected filesystem state")
		}

		if *output.FileSystems[0].LifeCycleState == efs.LifeCycleStateDeleted {
			return nil
		}
		o.clock.Sleep(PollingInterval)
	}
	return nil
}
