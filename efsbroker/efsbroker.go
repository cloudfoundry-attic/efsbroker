package efsbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sync"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/service-broker-store/brokerstore"
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
	EfsId         string   `json:"EfsId"`
	FsState       string   `json:"FsState"`
	MountId       string   `json:"MountId"`
	MountState    string   `json:"MountState"`
	MountPermsSet bool     `json:"MountPermsSet"`
	MountIp       string   `json:"MountIp"`
	MountIds      []string `json:"MountIds"`
	MountStates   []string `json:"MountStates"`
	MountIps      []string `json:"MountIps"`
	MountAZs      []string `json:"MountAZs"`
	Err           error    `json:"Err"`
}

type dynamicState struct {
	InstanceMap map[string]EFSInstance
	BindingMap  map[string]brokerapi.BindDetails
}

type lock interface {
	Lock()
	Unlock()
}

type Subnet struct {
	ID            string
	AZ            string
	SecurityGroup string
}

type Broker struct {
	logger               lager.Logger
	efsService           EFSService
	subnets              []Subnet
	dataDir              string
	os                   osshim.Os
	mutex                lock
	clock                clock.Clock
	efsTools             efsvoltools.VolTools
	ProvisionOperation   func(logger lager.Logger, instanceID string, details brokerapi.ProvisionDetails, efsService EFSService, efsTools efsvoltools.VolTools, subnets []Subnet, clock Clock, updateCb func(*OperationState)) Operation
	DeprovisionOperation func(logger lager.Logger, efsService EFSService, clock Clock, spec DeprovisionOperationSpec, updateCb func(*OperationState)) Operation

	static staticState
	store  brokerstore.Store
}

func New(
	logger lager.Logger,
	serviceName, serviceId, dataDir string,
	os osshim.Os,
	clock clock.Clock,
	store brokerstore.Store,
	efsService EFSService, subnets []Subnet,
	efsTools efsvoltools.VolTools,
	provisionOperation func(logger lager.Logger, instanceID string, details brokerapi.ProvisionDetails, efsService EFSService, efsTools efsvoltools.VolTools, subnets []Subnet, clock Clock, updateCb func(*OperationState)) Operation,
	deprovisionOperation func(logger lager.Logger, efsService EFSService, clock Clock, spec DeprovisionOperationSpec, updateCb func(*OperationState)) Operation,
) *Broker {

	theBroker := Broker{
		logger:               logger,
		dataDir:              dataDir,
		os:                   os,
		efsService:           efsService,
		subnets:              subnets,
		mutex:                &sync.Mutex{},
		clock:                clock,
		store:                store,
		efsTools:             efsTools,
		ProvisionOperation:   provisionOperation,
		DeprovisionOperation: deprovisionOperation,
		static: staticState{
			ServiceName: serviceName,
			ServiceId:   serviceId,
		},
	}

	theBroker.store.Restore(logger)

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

func (b *Broker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (_ brokerapi.ProvisionedServiceSpec, e error) {
	logger := b.logger.Session("provision").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	if !asyncAllowed {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	efsInstance := EFSInstance{details, "", "", "", "", false, "", []string{}, []string{}, []string{}, []string{}, nil}

	instanceDetails := brokerstore.ServiceInstance{
		details.ServiceID,
		details.PlanID,
		details.OrganizationGUID,
		details.SpaceGUID,
		efsInstance,
	}

	if b.instanceConflicts(instanceDetails, instanceID) {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}

	err := b.store.CreateInstanceDetails(instanceID, instanceDetails)
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("failed to store instance details %s", instanceID)
	}

	operation := b.ProvisionOperation(logger, instanceID, details, b.efsService, b.efsTools, b.subnets, b.clock, b.ProvisionEvent)

	go operation.Execute()

	return brokerapi.ProvisionedServiceSpec{IsAsync: true, OperationData: "provision"}, nil
}

func (b *Broker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (_ brokerapi.DeprovisionServiceSpec, e error) {
	logger := b.logger.Session("deprovision")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	instance, err := b.store.RetrieveInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	efsInstance, ok := instance.ServiceFingerPrint.(EFSInstance)
	if !ok {
		return brokerapi.DeprovisionServiceSpec{}, errors.New("failed to casting interface back to EFSInstance")
	}

	if efsInstance.MountIds == nil || len(efsInstance.MountIds) == 0 {
		efsInstance.MountIds = []string{efsInstance.MountId}
	}

	spec := DeprovisionOperationSpec{
		InstanceID:     instanceID,
		FsID:           efsInstance.EfsId,
		MountTargetIDs: efsInstance.MountIds,
	}
	operation := b.DeprovisionOperation(logger, b.efsService, b.clock, spec, b.DeprovisionEvent)

	go operation.Execute()

	return brokerapi.DeprovisionServiceSpec{IsAsync: true, OperationData: "deprovision"}, nil
}

func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, details brokerapi.BindDetails) (_ brokerapi.Binding, e error) {
	logger := b.logger.Session("bind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	instanceDetails, err := b.store.RetrieveInstanceDetails(instanceID)
	if err != nil {
		return brokerapi.Binding{}, brokerapi.ErrInstanceDoesNotExist
	}

	if details.AppGUID == "" {
		return brokerapi.Binding{}, brokerapi.ErrAppGuidNotProvided
	}

	var params map[string]interface{}
	if len(details.RawParameters) > 0 {
		if err := json.Unmarshal(details.RawParameters, &params); err != nil {
			return brokerapi.Binding{}, err
		}
	}

	mode, err := evaluateMode(params)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	if b.bindingConflicts(bindingID, details) {
		return brokerapi.Binding{}, brokerapi.ErrBindingAlreadyExists
	}

	err = b.store.CreateBindingDetails(bindingID, details)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	efsInstance, ok := instanceDetails.ServiceFingerPrint.(EFSInstance)
	if !ok {
		return brokerapi.Binding{}, errors.New("failed casting interface back to EFSInstance")
	}

	ip, err := b.getMountIp(efsInstance.EfsId)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	source := ip + RootPath

	azMap := map[string]interface{}{}
	for i, ip := range efsInstance.MountIps {
		azMap[efsInstance.MountAZs[i]] = ip + RootPath
	}

	mountConfig := map[string]interface{}{"source": source, "az-map": azMap}

	return brokerapi.Binding{
		Credentials: struct{}{}, // if nil, cloud controller chokes on response
		VolumeMounts: []brokerapi.VolumeMount{{
			ContainerDir: evaluateContainerPath(params, instanceID),
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

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) (e error) {
	logger := b.logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if e == nil {
			e = out
		}
	}()

	if _, err := b.store.RetrieveInstanceDetails(instanceID); err != nil {
		return brokerapi.ErrInstanceDoesNotExist
	}

	if _, err := b.store.RetrieveBindingDetails(bindingID); err != nil {
		return brokerapi.ErrBindingDoesNotExist
	}

	if err := b.store.DeleteBindingDetails(bindingID); err != nil {
		return err
	}
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
		instance, err := b.store.RetrieveInstanceDetails(instanceID)

		if err != nil {
			logger.Info("instance-not-found")
			return brokerapi.LastOperation{}, brokerapi.ErrInstanceDoesNotExist
		}

		efsInstance, ok := instance.ServiceFingerPrint.(EFSInstance)
		if !ok {
			return brokerapi.LastOperation{}, errors.New("failed to casting interface back to EFSInstance")
		}

		if efsInstance.Err != nil {
			logger.Info(fmt.Sprintf("last-operation-error %#v", efsInstance.Err))
			return brokerapi.LastOperation{State: brokerapi.Failed, Description: efsInstance.Err.Error()}, nil
		}

		logger.Debug(fmt.Sprintf("Instance data %#v", efsInstance))
		return stateToLastOperation(efsInstance), nil
	case "deprovision":
		instance, err := b.store.RetrieveInstanceDetails(instanceID)

		if err != nil {
			return brokerapi.LastOperation{State: brokerapi.Succeeded}, nil
		} else {
			efsInstance, ok := instance.ServiceFingerPrint.(EFSInstance)
			if !ok {
				return brokerapi.LastOperation{}, errors.New("failed to casting interface back to EFSInstance")
			}
			if efsInstance.Err != nil {
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
	defer func() {
		out := b.store.Save(logger)
		if out != nil {
			logger.Error("store save failed", out)
		}
	}()

	if opState.Err != nil {
		logger.Error("Last provision failed", opState.Err)
	}

	instance, err := b.store.RetrieveInstanceDetails(opState.InstanceID)
	if err != nil {
		logger.Error("instance-not-found", err)
	}

	var efsInstance EFSInstance

	efsInstance.EfsId = opState.FsID
	efsInstance.FsState = opState.FsState

	efsInstance.MountId = opState.MountTargetIDs[0]
	efsInstance.MountIp = opState.MountTargetIps[0]
	efsInstance.MountState = opState.MountTargetStates[0]

	efsInstance.MountIds = opState.MountTargetIDs
	efsInstance.MountIps = opState.MountTargetIps
	efsInstance.MountAZs = opState.MountTargetAZs
	efsInstance.MountStates = opState.MountTargetStates
	efsInstance.MountPermsSet = opState.MountPermsSet
	efsInstance.Err = opState.Err

	instance.ServiceFingerPrint = efsInstance

	err = b.store.DeleteInstanceDetails(opState.InstanceID)
	if err != nil {
		logger.Error("failed to delete instance", err)
		return
	}

	err = b.store.CreateInstanceDetails(opState.InstanceID, instance)
	if err != nil {
		logger.Error("failed to store instance details", err)
		return
	}
}

func (b *Broker) DeprovisionEvent(opState *OperationState) {
	logger := b.logger.Session("deprovision-event").WithData(lager.Data{"state": opState})
	logger.Info("start")
	defer logger.Info("end")
	b.mutex.Lock()
	defer b.mutex.Unlock()
	defer func() {
		out := b.store.Save(logger)
		if out != nil {
			logger.Error("store save failed", out)
		}
	}()

	var err error

	if opState.Err == nil {
		err = b.store.DeleteInstanceDetails(opState.InstanceID)
		if err != nil {
			logger.Error("failed to delete instance", err)
			return
		}
	} else {
		instance, err := b.store.RetrieveInstanceDetails(opState.InstanceID)
		if err != nil {
			logger.Error("instance-not-found", err)
			return
		}

		efsInstance, ok := instance.ServiceFingerPrint.(EFSInstance)
		if !ok {
			logger.Error("error", errors.New("failed to casting interface back to EFSInstance"))
			return
		}

		efsInstance.Err = opState.Err

		instance.ServiceFingerPrint = efsInstance

		err = b.store.DeleteInstanceDetails(opState.InstanceID)
		if err != nil {
			logger.Error("failed to delete instance", err)
			return
		}

		err = b.store.CreateInstanceDetails(opState.InstanceID, instance)
		if err != nil {
			logger.Error("failed to store instance details", err)
			return
		}
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

func (b *Broker) instanceConflicts(details brokerstore.ServiceInstance, instanceID string) bool {
	return b.store.IsInstanceConflict(instanceID, brokerstore.ServiceInstance(details))
}

func (b *Broker) bindingConflicts(bindingID string, details brokerapi.BindDetails) bool {
	return b.store.IsBindingConflict(bindingID, details)
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
