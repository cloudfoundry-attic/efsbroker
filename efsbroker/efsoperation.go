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
	"github.com/pivotal-cf/brokerapi"
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
	InstanceID        string
	FsID              string
	FsState           string
	MountTargetIDs    []string
	MountTargetStates []string
	MountPermsSet     bool
	MountTargetIps    []string
	MountTargetAZs    []string
	Err               error
}

func NewProvisionOperation(logger lager.Logger, instanceID string, details brokerapi.ProvisionDetails, efsService EFSService, efsTools efsvoltools.VolTools, subnets []Subnet, clock Clock, updateCb func(*OperationState)) Operation {
	return NewProvisionStateMachine(logger, instanceID, details, efsService, efsTools, subnets, clock, updateCb)
}

func NewProvisionStateMachine(logger lager.Logger, instanceID string, details brokerapi.ProvisionDetails, efsService EFSService, efsTools efsvoltools.VolTools, subnets []Subnet, clock Clock, updateCb func(*OperationState)) *ProvisionOperationStateMachine {
	return &ProvisionOperationStateMachine{
		details,
		efsService,
		efsTools,
		subnets,
		logger,
		clock,
		&OperationState{
			InstanceID:        instanceID,
			MountTargetIDs:    make([]string, len(subnets)),
			MountTargetIps:    make([]string, len(subnets)),
			MountTargetStates: make([]string, len(subnets)),
			MountTargetAZs:    make([]string, len(subnets)),
		},
		updateCb,
		nil,
		nil,
		nil,
	}
}

type ProvisionOperationStateMachine struct {
	details         brokerapi.ProvisionDetails
	efsService      EFSService
	efsTools        efsvoltools.VolTools
	subnets         []Subnet
	logger          lager.Logger
	clock           Clock
	state           *OperationState
	updateCb        func(*OperationState)
	nextState       func()
	stateAfterSleep func()
	azs             []string
}

func (o *ProvisionOperationStateMachine) Execute() {
	logger := o.logger.Session("provision-execute")
	logger.Info("start")
	defer logger.Info("end")

	o.azs = []string{}
	for _, subnet := range o.subnets {
		o.azs = append(o.azs, subnet.AZ)
	}

	err := o.CreateFs()
	if err != nil {
		return
	}

	err = o.CheckFs()
	if err != nil {
		return
	}

	err = o.CreateMountTargets()
	if err != nil {
		return
	}

	err = o.CheckMountTargets()
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
	fsDescriptor, o.state.Err = o.efsService.CreateFileSystem(&efs.CreateFileSystemInput{
		CreationToken:   aws.String(o.state.InstanceID),
		PerformanceMode: planIDToPerformanceMode(o.details.PlanID),
	})
	if o.state.Err != nil {
		logger.Error("provision-state-start-failed-to-create-fs", o.state.Err)
		return o.state.Err
	}
	o.state.FsID = *fsDescriptor.FileSystemId

	_, o.state.Err = o.efsService.CreateTags(&efs.CreateTagsInput{
		FileSystemId: fsDescriptor.FileSystemId,
		Tags: []*efs.Tag{
			{Key: aws.String("organization_guid"), Value: aws.String(o.details.OrganizationGUID)},
			{Key: aws.String("space_guid"), Value: aws.String(o.details.SpaceGUID)},
			{Key: aws.String("service_id"), Value: aws.String(o.details.ServiceID)},
			{Key: aws.String("plan_id"), Value: aws.String(o.details.PlanID)},
			{Key: aws.String("instance"), Value: aws.String(o.state.InstanceID)},
		},
	})
	return o.state.Err
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

func (o *ProvisionOperationStateMachine) CreateMountTargets() error {
	logger := o.logger.Session("create-mount-targets")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	for i, subnet := range o.subnets {
		var target *efs.MountTargetDescription
		target, o.state.Err = o.efsService.CreateMountTarget(&efs.CreateMountTargetInput{
			FileSystemId:   aws.String(o.state.FsID),
			SubnetId:       aws.String(subnet.ID),
			SecurityGroups: []*string{aws.String(subnet.SecurityGroup)},
		})

		if o.state.Err != nil {
			logger.Error("failed-to-create-mounts", o.state.Err)
			return o.state.Err
		}

		o.state.MountTargetIDs[i] = *target.MountTargetId
		o.state.MountTargetAZs[i] = subnet.AZ
	}

	return nil
}

func (o *ProvisionOperationStateMachine) CheckMountTargets() error {
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
		if len(mtOutput.MountTargets) != len(o.subnets) {
			o.state.Err = fmt.Errorf("AWS returned an unexpected number of mount targets. got: %d expected %d", len(mtOutput.MountTargets), len(o.subnets))
			logger.Error("error-at-amazon", o.state.Err)
			return o.state.Err
		}

		working := false
		for _, target := range mtOutput.MountTargets {
			var (
				i  int
				id string
			)
			found := false
			for i, id = range o.state.MountTargetIDs {
				if id == *target.MountTargetId {
					found = true
					break
				}
			}
			if !found {
				o.state.Err = fmt.Errorf("Unknown Mount Target ID.  %s", *target.MountTargetId)
				return o.state.Err
			}
			o.state.MountTargetStates[i] = *target.LifeCycleState

			switch o.state.MountTargetStates[i] {
			case efs.LifeCycleStateAvailable:
				o.state.MountTargetIDs[i] = *target.MountTargetId
				if target.IpAddress != nil {
					o.state.MountTargetIps[i] = *target.IpAddress
				}
				continue
			case efs.LifeCycleStateCreating:
				o.clock.Sleep(PollingInterval)
				working = true
				break
			default:
				o.state.Err = fmt.Errorf("Unexpected lifecycle state.  Expected creating or available, got %s", o.state.Err)
				return o.state.Err
			}
		}
		if !working {
			break
		}
	}

	return nil
}

func (o *ProvisionOperationStateMachine) OpenPerms() error {
	logger := o.logger.Session("provision-state-open-perms")
	logger.Info("start")
	defer logger.Info("end")
	defer o.updateCb(o.state)

	opts := map[string]interface{}{"ip": o.state.MountTargetIps[0], "ips": o.state.MountTargetIps, "azs": o.azs}

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
	InstanceID     string
	FsID           string
	MountTargetIDs []string
}

func NewDeprovisionOperation(logger lager.Logger, efsService EFSService, clock Clock, spec DeprovisionOperationSpec, updateCb func(*OperationState)) Operation {
	return &DeprovisionOperation{logger, efsService, clock, spec, &OperationState{InstanceID: spec.InstanceID}, updateCb}
}

func NewTestDeprovisionOperation(logger lager.Logger, efsService EFSService, clock Clock, spec DeprovisionOperationSpec, updateCb func(*OperationState)) *DeprovisionOperation {
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
	if len(out.MountTargets) > len(o.spec.MountTargetIDs) {
		err = fmt.Errorf("Too many mount targets found, Expected %d, got %d", len(o.spec.MountTargetIDs), len(out.MountTargets))
		logger.Error("err-at-amazon", err)
		return err
	}

	for _, target := range out.MountTargets {
		if *target.LifeCycleState != efs.LifeCycleStateAvailable {
			err = errors.New("invalid lifecycle transition, please wait until all mount targets are available")
			logger.Error("non-available-mount-targets", err)
			return err
		}
	}

	logger.Info("deleting-mount-targets", lager.Data{"target-id": *out.MountTargets[0].MountTargetId})
	for _, target := range out.MountTargets {
		_, err = o.efs.DeleteMountTarget(&efs.DeleteMountTargetInput{
			MountTargetId: target.MountTargetId,
		})
		if err != nil {
			logger.Error("failed-deleting-mount-targets", err)
			return err
		}
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
		if len(mtOutput.MountTargets) < 1 {
			return nil
		}

		deleted := true
		for _, target := range mtOutput.MountTargets {
			if *target.LifeCycleState != efs.LifeCycleStateDeleted {
				deleted = false
				break
			}
		}
		if deleted {
			return nil
		}

		if len(mtOutput.MountTargets) > len(o.spec.MountTargetIDs) {
			err = fmt.Errorf("amazon returned unexpected number of mount targets.  Expected %d, got %d", len(o.spec.MountTargetIDs), len(mtOutput.MountTargets))
			logger.Error("err-at-amazon", err)
			return err
		}
		o.clock.Sleep(PollingInterval)
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
