package efsbroker

import (
	"errors"
	"fmt"

	"time"

	"strings"

	"context"

	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/voldriver/driverhttp"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
)

//go:generate counterfeiter -o efsfakes/fake_operation.go . Operation

const (
	PollingInterval = 5 * time.Second
)

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
	MountPermsSet    bool
	MountTargetIp    string
	Err              error
}

func NewProvisionOperation(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnets []Subnet, encryption Encryption, clock Clock, updateCb func(*OperationState)) Operation {
	return NewProvisionStateMachine(logger, instanceID, planID, efsService, efsTools, subnets, encryption, clock, updateCb)
}

func NewProvisionStateMachine(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnets []Subnet, encryption Encryption, clock Clock, updateCb func(*OperationState)) *ProvisionOperationStateMachine {
	return &ProvisionOperationStateMachine{
		planID,
		efsService,
		efsTools,
		subnets,
		encryption,
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
	subnets         []Subnet
	encryption      Encryption
	logger          lager.Logger
	clock           Clock
	state           *OperationState
	updateCb        func(*OperationState)
	nextState       func()
	stateAfterSleep func()
}

func (o *ProvisionOperationStateMachine) Execute() {
	logger := o.logger.Session("provision-execute")
	logger.Info("start")
	defer logger.Info("end")

	err := o.CreateFs()
	if err != nil {
		return
	}

	err = o.CheckFs()
	if err != nil {
		return
	}

	err = o.CreateMountTarget()
	if err != nil {
		return
	}

	err = o.CheckMountTarget()
	if err != nil {
		return
	}

	err = o.OpenPerms()
	if err != nil {
		return
	}
}

func (o *ProvisionOperationStateMachine) CreateFs() error {
	logger := o.logger.Session("create-fs")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	var fsDescriptor *efs.FileSystemDescription
	input := efs.CreateFileSystemInput{
		CreationToken:   aws.String(o.state.InstanceID),
		PerformanceMode: planIDToPerformanceMode(o.planID),
		Encrypted:       &o.encryption.Enabled,
	}

	if o.encryption.KmsKeyId != "" {
		input.KmsKeyId = &o.encryption.KmsKeyId
	}

	fsDescriptor, o.state.Err = o.efsService.CreateFileSystem(&input)
	if o.state.Err != nil {
		logger.Error("provision-state-start-failed-to-create-fs", o.state.Err)
		return o.state.Err
	}
	o.state.FsID = *fsDescriptor.FileSystemId
	return nil
}

func (o *ProvisionOperationStateMachine) CheckFs() error {
	logger := o.logger.Session("check-fs")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	for true {
		var output *efs.DescribeFileSystemsOutput
		output, o.state.Err = o.efsService.DescribeFileSystems(&efs.DescribeFileSystemsInput{
			FileSystemId: aws.String(o.state.FsID),
		})
		if o.state.Err != nil {
			logger.Error("err-getting-fs-status", o.state.Err)
			//o.nextState = o.Finish
			return o.state.Err
		}
		if len(output.FileSystems) != 1 {
			o.state.Err = fmt.Errorf("AWS returned an unexpected number of filesystems: %d", len(output.FileSystems))
			logger.Error("err-at-amazon", o.state.Err)
			//o.nextState = o.Finish
			return o.state.Err
		}
		if output.FileSystems[0].LifeCycleState == nil {
			o.state.Err = errors.New("AWS returned an unexpected filesystem state")
			logger.Error("err-at-amazon", o.state.Err)
			//o.nextState = o.Finish
			return o.state.Err
		}

		o.state.FsState = *output.FileSystems[0].LifeCycleState

		switch o.state.FsState {
		case efs.LifeCycleStateAvailable:
			return nil
		case efs.LifeCycleStateCreating:
			o.clock.Sleep(PollingInterval)
		default:
			o.state.Err = fmt.Errorf("Unexpected lifecycle state.  Expected creating or available.  Got %s", o.state.FsState)
			return o.state.Err
		}
	}
	return nil
}

func (o *ProvisionOperationStateMachine) CreateMountTarget() error {
	logger := o.logger.Session("create-mount-target")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	_, o.state.Err = o.efsService.CreateMountTarget(&efs.CreateMountTargetInput{
		FileSystemId:   aws.String(o.state.FsID),
		SubnetId:       aws.String(o.subnets[0].ID),
		SecurityGroups: []*string{aws.String(o.subnets[0].SecurityGroup)},
	})

	if o.state.Err != nil {
		logger.Error("failed-to-create-mounts", o.state.Err)
		return o.state.Err
	}

	return nil
}

func (o *ProvisionOperationStateMachine) CheckMountTarget() error {
	logger := o.logger.Session("check-mount-target")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	for true {
		var mtOutput *efs.DescribeMountTargetsOutput
		mtOutput, o.state.Err = o.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
			FileSystemId: aws.String(o.state.FsID),
		})
		if o.state.Err != nil {
			logger.Error("err-getting-mount-target-status", o.state.Err)
			return o.state.Err
		}
		if len(mtOutput.MountTargets) != 1 {
			o.state.Err = fmt.Errorf("AWS returned an unexpected number of mount targets: %d", len(mtOutput.MountTargets))
			logger.Error("error-at-amazon", o.state.Err)
			return o.state.Err
		}

		o.state.MountTargetState = *mtOutput.MountTargets[0].LifeCycleState

		switch o.state.MountTargetState {
		case efs.LifeCycleStateAvailable:
			o.state.MountTargetID = *mtOutput.MountTargets[0].MountTargetId
			if mtOutput.MountTargets[0].IpAddress != nil {
				o.state.MountTargetIp = *mtOutput.MountTargets[0].IpAddress
			}
			return o.state.Err
		case efs.LifeCycleStateCreating:
			o.clock.Sleep(PollingInterval)
		default:
			o.state.Err = fmt.Errorf("Unexpected lifecycle state.  Expected creating or available, got %s", o.state.Err)
			return o.state.Err
		}
	}

	return nil
}

func (o *ProvisionOperationStateMachine) OpenPerms() error {
	logger := o.logger.Session("provision-state-open-perms")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	opts := map[string]interface{}{"ip": o.state.MountTargetIp}

	ctx := context.TODO()
	env := driverhttp.NewHttpDriverEnv(logger, ctx)

	resp := o.efsTools.OpenPerms(env, efsvoltools.OpenPermsRequest{Name: o.state.FsID, Opts: opts})
	if resp.Err != "" {
		o.state.Err = errors.New(resp.Err)
		logger.Error("failed-to-open-mount-permissions", o.state.Err)
		return o.state.Err
	}

	o.state.MountPermsSet = true

	return nil
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
			o.clock.Sleep(PollingInterval)
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
