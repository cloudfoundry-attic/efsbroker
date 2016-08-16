package efsbroker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"sync"

	"code.cloudfoundry.org/goshims/os"
	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/pivotal-cf/brokerapi"
)

const (
	PermissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	//DefaultContainerPath  = "/var/vcap/data"
)

type staticState struct {
	ServiceName string `json:"ServiceName"`
	ServiceId   string `json:"ServiceId"`
	PlanName    string `json:"PlanName"`
	PlanId      string `json:"PlanId"`
	PlanDesc    string `json:"PlanDesc"`
}

type EFSInstance struct {
	brokerapi.ProvisionDetails
	efsID string
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
	logger      lager.Logger
	provisioner EFSService
	dataDir     string
	os          osshim.Os
	ioutil      ioutilshim.Ioutil
	mutex       lock

	static  staticState
	dynamic dynamicState
}

func New(
	logger lager.Logger,
	serviceName, serviceId, planName, planId, planDesc, dataDir string,
	os osshim.Os,
	ioutil ioutilshim.Ioutil,
	provisioner EFSService,
) *broker {

	theBroker := broker{
		logger:      logger,
		dataDir:     dataDir,
		os:          os,
		ioutil:      ioutil,
		provisioner: provisioner,
		mutex:       &sync.Mutex{},
		static: staticState{
			ServiceName: serviceName,
			ServiceId:   serviceId,
			PlanName:    planName,
			PlanId:      planId,
			PlanDesc:    planDesc,
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
	logger := b.logger.Session("provision")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	if !asyncAllowed {
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	if b.instanceConflicts(details, instanceID) {
		logger.Error("instance-already-exists", brokerapi.ErrInstanceAlreadyExists)
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrInstanceAlreadyExists
	}

	fsDescriptor, err := b.provisioner.CreateFileSystem(&efs.CreateFileSystemInput{
		CreationToken:   aws.String(instanceID),
		PerformanceMode: planIDToPerformanceMode(details.PlanID),
	})
	if err != nil {
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	b.dynamic.InstanceMap[instanceID] = EFSInstance{details, *fsDescriptor.FileSystemId}

	return brokerapi.ProvisionedServiceSpec{IsAsync: true}, nil
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

	defer b.persist(b.dynamic)

	if !asyncAllowed {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrAsyncRequired
	}

	instance, instanceExists := b.dynamic.InstanceMap[instanceID]
	if !instanceExists {
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	_, err := b.provisioner.DeleteFileSystem(&efs.DeleteFileSystemInput{
		FileSystemId: aws.String(instance.efsID),
	})
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	delete(b.dynamic.InstanceMap, instanceID)

	return brokerapi.DeprovisionServiceSpec{}, nil
}

func (b *broker) Bind(instanceID string, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	logger := b.logger.Session("bind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	return brokerapi.Binding{}, errors.New("unimplemented")
}

func (b *broker) Unbind(instanceID string, bindingID string, details brokerapi.UnbindDetails) error {
	logger := b.logger.Session("unbind")
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	defer b.persist(b.dynamic)

	return errors.New("unimplemented")
}

func (b *broker) Update(instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *broker) LastOperation(instanceID string, operationData string) (brokerapi.LastOperation, error) {
	instance, instanceExists := b.dynamic.InstanceMap[instanceID]
	if !instanceExists {
		return brokerapi.LastOperation{State: brokerapi.Failed}, brokerapi.ErrInstanceDoesNotExist
	}

	output, err := b.provisioner.DescribeFileSystems(&efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(instance.efsID),
	})
	if err != nil {
		return brokerapi.LastOperation{}, err
	}
	if len(output.FileSystems) != 1 {
		return brokerapi.LastOperation{State: brokerapi.Failed}, errors.New("invalid response from AWS")
	}

	return awsStateToLastOperation(output.FileSystems[0].LifeCycleState), nil
}

func awsStateToLastOperation(state *string) brokerapi.LastOperation {
	switch *state {
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

//func evaluateContainerPath(parameters map[string]interface{}, volId string) string {
//	if containerPath, ok := parameters["mount"]; ok && containerPath != "" {
//		return containerPath.(string)
//	}
//
//	return path.Join(DefaultContainerPath, volId)
//}
//
//func evaluateMode(parameters map[string]interface{}) (string, error) {
//	if ro, ok := parameters["readonly"]; ok {
//		switch ro := ro.(type) {
//		case bool:
//			return readOnlyToMode(ro), nil
//		default:
//			return "", brokerapi.ErrRawParamsInvalid
//		}
//	}
//	return "rw", nil
//}

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
