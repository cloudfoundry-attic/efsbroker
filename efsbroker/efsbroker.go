package efsbroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"sync"

	"path"

	"context"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/pivotal-cf/brokerapi"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	DefaultContainerPath  = "/var/vcap/data"

	RootPath = ":/"
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
	EfsId         string `json:"EfsId"`
	FsState       string `json:"FsState"`
	MountId       string `json:"MountId"`
	MountState    string `json:"MountState"`
	MountPermsSet bool   `json:"MountPermsSet"`
	MountIp       string `json:"MountIp"`
	Err           error  `json:"Err"`
}

type dynamicState struct {
	InstanceMap map[string]EFSInstance
	BindingMap  map[string]brokerapi.BindDetails
}

type lock interface {
	Lock()
	Unlock()
}

type Broker struct {
	logger               lager.Logger
	efsService           EFSService
	subnetIds            []string
	securityGroup        string
	dataDir              string
	os                   osshim.Os
	ioutil               ioutilshim.Ioutil
	mutex                lock
	clock                clock.Clock
	efsTools             efsvoltools.VolTools
	ProvisionOperation   func(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnetIds []string, securityGroup string, clock Clock, updateCb func(*OperationState)) Operation
	DeprovisionOperation func(logger lager.Logger, efsService EFSService, clock Clock, spec DeprovisionOperationSpec, updateCb func(*OperationState)) Operation

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
	provisionOperation func(logger lager.Logger, instanceID string, planID string, efsService EFSService, efsTools efsvoltools.VolTools, subnetIds []string, securityGroup string, clock Clock, updateCb func(*OperationState)) Operation,
	deprovisionOperation func(logger lager.Logger, efsService EFSService, clock Clock, spec DeprovisionOperationSpec, updateCb func(*OperationState)) Operation,
) *Broker {

	theBroker := Broker{
		logger:               logger,
		dataDir:              dataDir,
		os:                   os,
		ioutil:               ioutil,
		efsService:           efsService,
		subnetIds:            subnetIds,
		securityGroup:        securityGroup,
		mutex:                &sync.Mutex{},
		clock:                clock,
		efsTools:             efsTools,
		ProvisionOperation:   provisionOperation,
		DeprovisionOperation: deprovisionOperation,
		static: staticState{
			ServiceName: serviceName,
			ServiceId:   serviceId,
		},
		dynamic: dynamicState{
			InstanceMap: map[string]EFSInstance{},
			BindingMap:  map[string]brokerapi.BindDetails{},
		},
	}

	theBroker.restoreDynamicState()

	return &theBroker
}

func (b *Broker) Services(_ context.Context) []brokerapi.Service {
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

func (b *Broker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, error) {
	logger := b.logger.Session("provision").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if !asyncAllowed {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	if b.instanceConflicts(details, instanceID) {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}

	b.dynamic.InstanceMap[instanceID] = EFSInstance{details, "", "", "", "", false, "", nil}

	operation := b.ProvisionOperation(logger, instanceID, details.PlanID, b.efsService, b.efsTools, b.subnetIds, b.securityGroup, b.clock, b.ProvisionEvent)

	go operation.Execute()

	return brokerapi.ProvisionedServiceSpec{IsAsync: true, OperationData: "provision"}, nil
}

func (b *Broker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.DeprovisionServiceSpec, error) {
	logger := b.logger.Session("deprovision")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	instance, instanceExists := b.dynamic.InstanceMap[instanceID]
	if !instanceExists {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	spec := DeprovisionOperationSpec{
		InstanceID:    instanceID,
		FsID:          instance.EfsId,
		MountTargetID: instance.MountId,
	}
	operation := b.DeprovisionOperation(logger, b.efsService, b.clock, spec, b.DeprovisionEvent)

	go operation.Execute()

	return brokerapi.DeprovisionServiceSpec{IsAsync: true, OperationData: "deprovision"}, nil
}

func (b *Broker) setErrorOnInstance(instanceId string, err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	instance, instanceExists := b.dynamic.InstanceMap[instanceId]
	if instanceExists {
		instance.Err = err
		b.dynamic.InstanceMap[instanceId] = instance
	}
	return
}

func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
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

	source := ip + RootPath

	mountConfig := map[string]interface{}{"source": source}

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

func (b *Broker) getMountIp(fsId string) (string, error) {
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

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) error {
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

func (b *Broker) Update(context context.Context, instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *Broker) LastOperation(_ context.Context, instanceID string, operationData string) (brokerapi.LastOperation, error) {
	logger := b.logger.Session("last-operation").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	switch operationData {
	case "provision":
		instance, instanceExists := b.dynamic.InstanceMap[instanceID]
		if !instanceExists {
			logger.Info("instance-not-found")
			return brokerapi.LastOperation{}, brokerapi.ErrInstanceDoesNotExist
		}

		if instance.Err != nil {
			logger.Info(fmt.Sprintf("last-operation-error %#v", instance.Err))
			return brokerapi.LastOperation{State: brokerapi.Failed, Description: instance.Err.Error()}, nil
		}

		logger.Debug(fmt.Sprintf("Instance data %#v", instance))
		return stateToLastOperation(instance), nil
	case "deprovision":
		instance, instanceExists := b.dynamic.InstanceMap[instanceID]
		if !instanceExists {
			return brokerapi.LastOperation{State: brokerapi.Succeeded}, nil
		} else {
			if instance.Err != nil {
				return brokerapi.LastOperation{State: brokerapi.Failed}, nil
			} else {
				return brokerapi.LastOperation{State: brokerapi.InProgress}, nil
			}
		}
	default:
		return brokerapi.LastOperation{}, errors.New("unrecognized operationData")
	}
}

//callbacks
func (b *Broker) ProvisionEvent(opState *OperationState) {
	logger := b.logger.Session("provision-event").WithData(lager.Data{"state": opState})
	logger.Info("start")
	defer logger.Info("end")
	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	if opState.Err != nil {
		logger.Error("Last provision failed", opState.Err)
	}

	efsInstance, _ := b.dynamic.InstanceMap[opState.InstanceID]
	efsInstance.EfsId = opState.FsID
	efsInstance.FsState = opState.FsState
	efsInstance.MountId = opState.MountTargetID
	efsInstance.MountIp = opState.MountTargetIp
	efsInstance.MountState = opState.MountTargetState
	efsInstance.MountPermsSet = opState.MountPermsSet
	efsInstance.Err = opState.Err
	b.dynamic.InstanceMap[opState.InstanceID] = efsInstance
}

func (b *Broker) DeprovisionEvent(opState *OperationState) {
	logger := b.logger.Session("deprovision-event").WithData(lager.Data{"state": opState})
	logger.Info("start")
	defer logger.Info("end")
	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	if opState.Err == nil {
		delete(b.dynamic.InstanceMap, opState.InstanceID)
	} else {
		efsInstance := b.dynamic.InstanceMap[opState.InstanceID]
		efsInstance.Err = opState.Err
		b.dynamic.InstanceMap[opState.InstanceID] = efsInstance
	}
}

func stateToLastOperation(instance EFSInstance) brokerapi.LastOperation {
	desc := stateToDescription(instance)

	if instance.Err != nil {
		return brokerapi.LastOperation{State: brokerapi.Failed, Description: desc}
	}

	switch instance.FsState {
	case "":
		return brokerapi.LastOperation{State: brokerapi.InProgress, Description: desc}
	case efs.LifeCycleStateCreating:
		return brokerapi.LastOperation{State: brokerapi.InProgress, Description: desc}
	case efs.LifeCycleStateAvailable:

		switch instance.MountState {
		case "":
			return brokerapi.LastOperation{State: brokerapi.InProgress, Description: desc}
		case efs.LifeCycleStateCreating:
			return brokerapi.LastOperation{State: brokerapi.InProgress, Description: desc}
		case efs.LifeCycleStateAvailable:
			// TODO check if the permissions have been opened up.
			if instance.MountPermsSet {
				return brokerapi.LastOperation{State: brokerapi.Succeeded, Description: desc}
			} else {
				return brokerapi.LastOperation{State: brokerapi.InProgress, Description: desc}
			}
		default:
			return brokerapi.LastOperation{State: brokerapi.Failed, Description: desc}
		}

	default:
		return brokerapi.LastOperation{State: brokerapi.Failed, Description: desc}
	}
}

func stateToDescription(instance EFSInstance) string {
	desc := fmt.Sprintf("FsID: %s, FsState: %s, MountID: %s, MountState: %s, MountAddress: %s", instance.EfsId, instance.FsState, instance.MountId, instance.MountState, instance.MountIp)
	if instance.Err != nil {
		desc = fmt.Sprintf("%s, Error: %s", desc, instance.Err.Error())
	}
	return desc
}

func (b *Broker) instanceConflicts(details brokerapi.ProvisionDetails, instanceID string) bool {
	if existing, ok := b.dynamic.InstanceMap[instanceID]; ok {
		if !reflect.DeepEqual(details, existing) {
			return true
		}
	}
	return false
}

func (b *Broker) bindingConflicts(bindingID string, details brokerapi.BindDetails) bool {
	if existing, ok := b.dynamic.BindingMap[bindingID]; ok {
		if !reflect.DeepEqual(details, existing) {
			return true
		}
	}
	return false
}

func (b *Broker) persist(state interface{}) {
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

func planIDToPerformanceMode(planID string) *string {
	if planID == "maxIO" {
		return aws.String(efs.PerformanceModeMaxIo)
	}
	return aws.String(efs.PerformanceModeGeneralPurpose)
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

func (b *Broker) restoreDynamicState() {
	logger := b.logger.Session("restore-services")
	logger.Info("start")
	defer logger.Info("end")

	stateFile := filepath.Join(b.dataDir, fmt.Sprintf("%s-services.json", b.static.ServiceName))

	serviceData, err := b.ioutil.ReadFile(stateFile)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-read-state-file: %s", stateFile), err)
		return
	}

	dynamicState := dynamicState{}
	err = json.Unmarshal(serviceData, &dynamicState)
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed-to-unmarshall-state from state-file: %s", stateFile), err)
		return
	}
	logger.Info("state-restored", lager.Data{"state-file": stateFile})
	b.dynamic = dynamicState
}
