package efsbroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"sync"

	"path"
	"strings"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/efsdriver/efsvoltools"
	ioutilshim "code.cloudfoundry.org/goshims/ioutil"
	osshim "code.cloudfoundry.org/goshims/os"
	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/pivotal-cf/brokerapi"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	DefaultContainerPath  = "/var/vcap/data"
	PollingInterval       = 3 * time.Second
)

var (
	ErrNoMountTargets         = errors.New("no mount targets found")
	ErrMountTargetUnavailable = errors.New("mount target not in available state")
)

type staticState struct {
	ServiceName string `json:"ServiceName"`
	ServiceId   string `json:"ServiceId"`
}

type EFSInstance struct {
	brokerapi.ProvisionDetails
	EfsId string `json:"EfsId"`
	err   bool
}

type dynamicState struct {
	InstanceMap map[string]EFSInstance
	BindingMap  map[string]brokerapi.BindDetails
}

type lock interface {
	Lock()
	Unlock()
}

type broker struct {
	logger             lager.Logger
	efsService         EFSService
	subnetIds          []string
	securityGroup      string
	dataDir            string
	os                 osshim.Os
	ioutil             ioutilshim.Ioutil
	mutex              lock
	clock              clock.Clock
	efsTools           efsvoltools.VolTools
	ProvisionOperation func(underlying interface{}, logger lager.Logger, fsID string) Operation

	static  staticState
	dynamic dynamicState
}

func New(
	logger lager.Logger,
	serviceName, serviceId, dataDir string,
	os osshim.Os,
	ioutil ioutilshim.Ioutil,
	clock clock.Clock,
	efsService EFSService, subnetIds []string, securityGroup string,
	efsTools efsvoltools.VolTools,
	provisionOperation func(underlying interface{}, logger lager.Logger, fsID string) Operation,
) *broker {

	theBroker := broker{
		logger:             logger,
		dataDir:            dataDir,
		os:                 os,
		ioutil:             ioutil,
		efsService:         efsService,
		subnetIds:          subnetIds,
		securityGroup:      securityGroup,
		mutex:              &sync.Mutex{},
		clock:              clock,
		efsTools:           efsTools,
		ProvisionOperation: provisionOperation,
		static: staticState{
			ServiceName: serviceName,
			ServiceId:   serviceId,
		},
		dynamic: dynamicState{
			InstanceMap: map[string]EFSInstance{},
			BindingMap:  map[string]brokerapi.BindDetails{},
		},
	}

	// theBroker.restoreDynamicState()

	return &theBroker
}

func (b *broker) Services() []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return []brokerapi.Service{{
		ID:            b.static.ServiceId,
		Name:          b.static.ServiceName,
		Description:   "Local service docs: https://code.cloudfoundry.org/efs-volume-release/",
		Bindable:      true,
		PlanUpdatable: false,
		Tags:          []string{"efs"},
		Requires:      []brokerapi.RequiredPermission{PermissionVolumeMount},

		Plans: []brokerapi.ServicePlan{
			{
				Name:        "generalPurpose",
				ID:          "generalPurpose",
				Description: "recommended for most file systems",
			}, {
				Name:        "maxIO",
				ID:          "maxIO",
				Description: "scales to higher levels of aggregate throughput and operations per second with a tradeoff of slightly higher latencies for most file operations",
			},
		},
	}}
}

func (b *broker) Provision(instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, error) {
	logger := b.logger.Session("provision").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	if !asyncAllowed {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	if b.instanceConflicts(details, instanceID) {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}

	logger.Info("creating-efs")
	fsDescriptor, err := b.efsService.CreateFileSystem(&efs.CreateFileSystemInput{
		CreationToken:   aws.String(instanceID),
		PerformanceMode: planIDToPerformanceMode(details.PlanID),
	})

	if err != nil {
		logger.Error("failed-to-create-fs", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	b.dynamic.InstanceMap[instanceID] = EFSInstance{details, *fsDescriptor.FileSystemId, false}

	operation := b.ProvisionOperation(b, logger, *fsDescriptor.FileSystemId)

	go operation.Execute()

	return brokerapi.ProvisionedServiceSpec{IsAsync: true, OperationData: "provision"}, nil
}

func (b *broker) createMountTargets(logger lager.Logger, fsID string) {
	logger = logger.Session("create-mount-targets")
	logger.Info("start")
	defer logger.Info("end")

	var err error

	// wait for fs to be available
	state, err := b.getFsStatus(logger, fsID)
	for state == efs.LifeCycleStateCreating {
		if err != nil {
			logger.Error("failed-to-get-fs-status", err)
			continue
		}

		b.clock.Sleep(PollingInterval)
		state, err = b.getFsStatus(logger, fsID)
	}

	// create mount target for that fs
	logger.Info("creating-mount-targets")
	_, err = b.efsService.CreateMountTarget(&efs.CreateMountTargetInput{
		FileSystemId:   aws.String(fsID),
		SubnetId:       aws.String(b.subnetIds[0]),
		SecurityGroups: []*string{aws.String(b.securityGroup)},
	})

	if err != nil {
		logger.Error("failed-to-create-mounts", err)
	}

	// wait for mount target to become available
	state, _ = b.getMountsStatus(logger, fsID)
	for state != efs.LifeCycleStateAvailable {
		b.clock.Sleep(PollingInterval)
		state, _ = b.getMountsStatus(logger, fsID)
	}
	logger.Info("created-mount-targets")

	// open up permissions on the new filesystem
	ip, err := b.getMountIp(fsID)
	if err != nil {
		logger.Error("failed-to-get-mount-address", err)
	}
	opts := map[string]interface{}{"ip": ip}

	resp := b.efsTools.OpenPerms(logger, efsvoltools.OpenPermsRequest{Name: fsID, Opts: opts})
	if resp.Err != "" {
		logger.Error("failed-to-open-mount-permissions", errors.New(resp.Err))
	}
}

func planIDToPerformanceMode(planID string) *string {
	if planID == "maxIO" {
		return aws.String(efs.PerformanceModeMaxIo)
	}
	return aws.String(efs.PerformanceModeGeneralPurpose)
}

func (b *broker) Deprovision(instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.DeprovisionServiceSpec, error) {
	logger := b.logger.Session("deprovision")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	instance, instanceExists := b.dynamic.InstanceMap[instanceID]
	if !instanceExists {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	go b.deprovision(logger, instance.EfsId, instanceID)

	return brokerapi.DeprovisionServiceSpec{IsAsync: true, OperationData: "deprovision"}, nil
}

func (b *broker) setErrorOnInstance(instanceId string, err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	instance, instanceExists := b.dynamic.InstanceMap[instanceId]
	if instanceExists {
		instance.err = true
		b.dynamic.InstanceMap[instanceId] = instance
	}
	return
}

func (b *broker) deprovision(logger lager.Logger, fsID string, instanceId string) {
	logger = logger.Session("deprovision-impl")
	logger.Info("start")
	defer logger.Info("end")

	err := b.deleteMountTargets(logger, fsID)
	if err != nil {
		b.setErrorOnInstance(instanceId, err)
		return
	}
	logger.Info("++++++++++++++++++++++++++mount target deleted++++++++++++++++++++++++")

	state, _ := b.getMountsStatus(logger, fsID)
	for state != efs.LifeCycleStateDeleted && state != "" {
		b.clock.Sleep(PollingInterval)
		state, _ = b.getMountsStatus(logger, fsID)
	}

	_, err = b.efsService.DeleteFileSystem(&efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		logger.Error("failed-deleting-fs", err)
		b.setErrorOnInstance(instanceId, err)
		return
	}
	logger.Info("++++++++++++++++++++++++++fs deleted++++++++++++++++++++++++")

	state, err = b.getFsStatus(logger, fsID)
	for state != efs.LifeCycleStateDeleted && err == nil {
		b.clock.Sleep(PollingInterval)
		state, err = b.getFsStatus(logger, fsID)
	}
	if err != nil && !strings.Contains(err.Error(), "does not exist") {
		logger.Info("error returned:")
		b.setErrorOnInstance(instanceId, err)
		return
	}
	logger.Info("++++++++++++++++++++++++++end++++++++++++++++++++++++")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)
	delete(b.dynamic.InstanceMap, instanceId)

	return
}

func (b *broker) deleteMountTargets(logger lager.Logger, fsId string) error {
	logger.Info("describing-mount-targets")
	out, err := b.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsId),
	})
	if err != nil {
		logger.Error("failed-describing-mount-targets", err)
		return err
	}

	if len(out.MountTargets) < 1 {
		logger.Info("no-mount-targets")
		return nil
	}

	if *out.MountTargets[0].LifeCycleState != efs.LifeCycleStateAvailable {
		logger.Info("non-available-mount-targets")
		return errors.New("invalid lifecycle transition, please wait until all mount targets are available")
	}

	logger.Info("deleting-mount-targets", lager.Data{"target-id": *out.MountTargets[0].MountTargetId})
	_, err = b.efsService.DeleteMountTarget(&efs.DeleteMountTargetInput{
		MountTargetId: out.MountTargets[0].MountTargetId,
	})
	if err != nil {
		logger.Error("failed-deleting-mount-targets", err)
		return err
	}

	return nil
}

func (b *broker) Bind(instanceID string, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	logger := b.logger.Session("bind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	fs, ok := b.dynamic.InstanceMap[instanceID]
	if !ok {
		return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
	}

	if details.AppGUID == "" {
		return brokerapi.Binding{}, brokerapi.ErrAppGuidNotProvided
	}

	mode, err := evaluateMode(details.Parameters)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	if b.bindingConflicts(bindingID, details) {
		return brokerapi.Binding{}, brokerapi.ErrBindingAlreadyExists
	}

	b.dynamic.BindingMap[bindingID] = details

	ip, err := b.getMountIp(fs.EfsId)
	if err != nil {
		return brokerapi.Binding{}, err
	}
	mountConfig := "{\"ip\": \"" + ip + "\"}"

	return brokerapi.Binding{
		Credentials: struct{}{}, // if nil, cloud controller chokes on response
		VolumeMounts: []brokerapi.VolumeMount{{
			ContainerDir: evaluateContainerPath(details.Parameters, instanceID),
			Mode:         mode,
			Driver:       "efsdriver",
			DeviceType:   "shared",
			Device: brokerapi.SharedDevice{
				VolumeId:    instanceID,
				MountConfig: mountConfig,
			},
		}},
	}, nil
}

func (b *broker) getMountIp(fsId string) (string, error) {
	// get mount point details from ews to return in bind response
	mtOutput, err := b.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsId),
	})
	if err != nil {
		b.logger.Error("err-getting-mount-target-status", err)
		return "", err
	}
	if len(mtOutput.MountTargets) < 1 {
		b.logger.Error("found-no-mount-targets", ErrNoMountTargets)
		return "", ErrNoMountTargets
	}

	if mtOutput.MountTargets[0].LifeCycleState == nil ||
		*mtOutput.MountTargets[0].LifeCycleState != efs.LifeCycleStateAvailable {
		b.logger.Error("mount-point-unavailable", ErrMountTargetUnavailable)
		return "", ErrMountTargetUnavailable
	}

	mountConfig := *mtOutput.MountTargets[0].IpAddress

	return mountConfig, nil
}

func (b *broker) Unbind(instanceID string, bindingID string, details brokerapi.UnbindDetails) error {
	logger := b.logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	if _, ok := b.dynamic.InstanceMap[instanceID]; !ok {
		return brokerapi.ErrInstanceDoesNotExist
	}

	if _, ok := b.dynamic.BindingMap[bindingID]; !ok {
		return brokerapi.ErrBindingDoesNotExist
	}

	delete(b.dynamic.BindingMap, bindingID)

	return nil
}

func (b *broker) Update(instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *broker) LastOperation(instanceID string, operationData string) (brokerapi.LastOperation, error) {
	logger := b.logger.Session("last-operation").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	switch operationData {
	case "provision":
		instance, instanceExists := b.dynamic.InstanceMap[instanceID]
		if !instanceExists {
			return brokerapi.LastOperation{}, brokerapi.ErrInstanceDoesNotExist
		}

		status, err := b.getStatus(logger, instance.EfsId)
		if err != nil {
			return brokerapi.LastOperation{}, err
		}

		return awsStateToLastOperation(status), nil
	case "deprovision":
		instance, instanceExists := b.dynamic.InstanceMap[instanceID]
		if !instanceExists {
			return brokerapi.LastOperation{State: brokerapi.Succeeded}, nil
		} else {
			if instance.err {
				return brokerapi.LastOperation{State: brokerapi.Failed}, nil
			} else {
				return brokerapi.LastOperation{State: brokerapi.InProgress}, nil
			}
		}
	default:
		return brokerapi.LastOperation{}, errors.New("unrecognized operationData")
	}
}

func (b *broker) getStatus(logger lager.Logger, fsId string) (string, error) {
	logger = logger.Session("getting-status", lager.Data{"fsId": fsId})
	logger.Info("start")
	defer logger.Info("end")

	fsStatus, err := b.getFsStatus(logger, fsId)
	if err != nil {
		return "", err
	}
	if fsStatus != efs.LifeCycleStateAvailable {
		return fsStatus, nil
	}

	mtStatus, err := b.getMountsStatus(logger, fsId)
	if err != nil {
		if err == ErrNoMountTargets {
			return efs.LifeCycleStateCreating, nil
		}
		return "", err
	}

	return mtStatus, nil
}

func (b *broker) getFsStatus(logger lager.Logger, fsId string) (string, error) {
	output, err := b.efsService.DescribeFileSystems(&efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(fsId),
	})
	if err != nil {
		logger.Error("err-getting-fs-status", err)
		return "", err
	}
	if len(output.FileSystems) != 1 {
		return "", fmt.Errorf("AWS returned an unexpected number of filesystems: %d", len(output.FileSystems))
	}
	if output.FileSystems[0].LifeCycleState == nil {
		return "", errors.New("AWS returned an unexpected filesystem state")
	}

	return *output.FileSystems[0].LifeCycleState, nil
}

func (b *broker) getMountsStatus(logger lager.Logger, fsId string) (string, error) {
	mtOutput, err := b.efsService.DescribeMountTargets(&efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsId),
	})
	if err != nil {
		logger.Error("err-getting-mount-target-status", err)
		return "", err
	}
	if len(mtOutput.MountTargets) < 1 {
		logger.Error("found-no-mount-targets", ErrNoMountTargets)
		return "", ErrNoMountTargets
	}

	logger.Info("getMountsStatus-returning: " + *mtOutput.MountTargets[0].LifeCycleState)
	return *mtOutput.MountTargets[0].LifeCycleState, nil
}

func awsStateToLastOperation(state string) brokerapi.LastOperation {
	switch state {
	case efs.LifeCycleStateCreating:
		return brokerapi.LastOperation{State: brokerapi.InProgress}
	case efs.LifeCycleStateAvailable:
		return brokerapi.LastOperation{State: brokerapi.Succeeded}
	default:
		return brokerapi.LastOperation{State: brokerapi.Failed}
	}
}

func (b *broker) instanceConflicts(details brokerapi.ProvisionDetails, instanceID string) bool {
	if existing, ok := b.dynamic.InstanceMap[instanceID]; ok {
		if !reflect.DeepEqual(details, existing) {
			return true
		}
	}
	return false
}

func evaluateContainerPath(parameters map[string]interface{}, volId string) string {
	if containerPath, ok := parameters["mount"]; ok && containerPath != "" {
		return containerPath.(string)
	}

	return path.Join(DefaultContainerPath, volId)
}

func evaluateMode(parameters map[string]interface{}) (string, error) {
	if ro, ok := parameters["readonly"]; ok {
		switch ro := ro.(type) {
		case bool:
			return readOnlyToMode(ro), nil
		default:
			return "", brokerapi.ErrRawParamsInvalid
		}
	}
	return "rw", nil
}

func readOnlyToMode(ro bool) string {
	if ro {
		return "r"
	}
	return "rw"
}

func (b *broker) bindingConflicts(bindingID string, details brokerapi.BindDetails) bool {
	if existing, ok := b.dynamic.BindingMap[bindingID]; ok {
		if !reflect.DeepEqual(details, existing) {
			return true
		}
	}
	return false
}

func (b *broker) persist(state interface{}) {
	logger := b.logger.Session("serialize-state")
	logger.Info("start")
	defer logger.Info("end")

	stateFile := filepath.Join(b.dataDir, fmt.Sprintf("%s-services.json", b.static.ServiceName))

	stateData, err := json.Marshal(state)
	if err != nil {
		b.logger.Error("failed-to-marshall-state", err)
		return
	}

	err = b.ioutil.WriteFile(stateFile, stateData, os.ModePerm)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-write-state-file: %s", stateFile), err)
		return
	}

	logger.Info("state-saved", lager.Data{"state-file": stateFile})
}

// func (b *broker) restoreDynamicState() {
//	logger := b.logger.Session("restore-services")
//	logger.Info("start")
//	defer logger.Info("end")

//stateFile := filepath.Join(b.dataDir, fmt.Sprintf("%s-services.json", b.static.ServiceName))
//
//serviceData, err := b.fs.ReadFile(stateFile)
//if err != nil {
//	b.logger.Error(fmt.Sprintf("failed-to-read-state-file: %s", stateFile), err)
//	return
//}

// dynamicState := dynamicState{}
//err = json.Unmarshal(serviceData, &dynamicState)
//if err != nil {
//	b.logger.Error(fmt.Sprintf("failed-to-unmarshall-state from state-file: %s", stateFile), err)
//	return
//}
//logger.Info("state-restored", lager.Data{"state-file": stateFile})
// b.dynamic = dynamicState
// }
