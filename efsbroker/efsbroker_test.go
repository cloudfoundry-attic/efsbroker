package efsbroker_test

import (
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/pivotal-cf/brokerapi"

	"time"

	"sync"

	"context"

	"encoding/json"

	"code.cloudfoundry.org/clock/fakeclock"
	"code.cloudfoundry.org/efsbroker/efsbroker"
	"code.cloudfoundry.org/efsbroker/efsbroker/efsfakes"
	"code.cloudfoundry.org/efsdriver/efsdriverfakes"
	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/goshims/ioutilshim/ioutil_fake"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type dynamicState struct {
	InstanceMap map[string]efsbroker.EFSInstance
	BindingMap  map[string]brokerapi.BindDetails
}

var _ = Describe("Broker", func() {
	var (
		broker                   *efsbroker.Broker
		fakeOs                   *os_fake.FakeOs
		fakeIoutil               *ioutil_fake.FakeIoutil
		fakeEFSService           *efsfakes.FakeEFSService
		fakeClock                *fakeclock.FakeClock
		fakeVolTools             *efsdriverfakes.FakeVolTools
		fakeProvisionOperation   *efsfakes.FakeOperation
		fakeDeprovisionOperation *efsfakes.FakeOperation
		logger                   lager.Logger
		ctx                      context.Context
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		ctx = context.TODO()
		fakeOs = &os_fake.FakeOs{}
		fakeIoutil = &ioutil_fake.FakeIoutil{}
		fakeClock = fakeclock.NewFakeClock(time.Unix(123, 456))
		fakeEFSService = &efsfakes.FakeEFSService{}
		fakeVolTools = &efsdriverfakes.FakeVolTools{}
		fakeProvisionOperation = &efsfakes.FakeOperation{}
		fakeDeprovisionOperation = &efsfakes.FakeOperation{}

		fakeEFSService.CreateFileSystemReturns(&efs.FileSystemDescription{
			FileSystemId: aws.String("fake-fs-id"),
		}, nil)
		fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
			FileSystems: []*efs.FileSystemDescription{{
				FileSystemId:   aws.String("fake-fs-id"),
				LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
			}},
		}, nil)
		fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
			MountTargets: []*efs.MountTargetDescription{{
				MountTargetId:  aws.String("fake-mt-id"),
				LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
				IpAddress:      aws.String("1.1.1.1"),
			}},
		}, nil)
		fakeEFSService.CreateMountTargetReturns(&efs.MountTargetDescription{
			MountTargetId: aws.String("fake-mt-id"),
		}, nil)
	})

	Context("when creating first time", func() {
		BeforeEach(func() {
			broker = efsbroker.New(
				logger,
				"service-name", "service-id", "/fake-dir",
				fakeOs,
				fakeIoutil,
				fakeClock,
				fakeEFSService,
				[]string{"fake-subnet-id"},
				"fake-security-group",
				fakeVolTools,
				func(lager.Logger, string, string, efsbroker.EFSService, efsvoltools.VolTools, []string, string, efsbroker.Clock, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeProvisionOperation
				},
				func(lager.Logger, efsbroker.EFSService, efsbroker.Clock, efsbroker.DeprovisionOperationSpec, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeDeprovisionOperation
				},
			)
		})

		Context(".Services", func() {
			It("returns the service catalog as appropriate", func() {
				result := broker.Services(ctx)[0]
				Expect(result.ID).To(Equal("service-id"))
				Expect(result.Name).To(Equal("service-name"))
				Expect(result.Description).To(Equal("Local service docs: https://code.cloudfoundry.org/efs-volume-release/"))
				Expect(result.Bindable).To(Equal(true))
				Expect(result.PlanUpdatable).To(Equal(false))
				Expect(result.Tags).To(ContainElement("efs"))
				Expect(result.Requires).To(ContainElement(brokerapi.RequiredPermission("volume_mount")))

				Expect(result.Plans[0].Name).To(Equal("generalPurpose"))
				Expect(result.Plans[0].ID).To(Equal("generalPurpose"))
				Expect(result.Plans[0].Description).To(Equal("recommended for most file systems"))
				Expect(result.Plans[1].Name).To(Equal("maxIO"))
				Expect(result.Plans[1].ID).To(Equal("maxIO"))
				Expect(result.Plans[1].Description).To(Equal("scales to higher levels of aggregate throughput and operations per second with a tradeoff of slightly higher latencies for most file operations"))
			})
		})

		Context(".Provision", func() {
			var (
				instanceID       string
				provisionDetails brokerapi.ProvisionDetails
				asyncAllowed     bool

				spec brokerapi.ProvisionedServiceSpec
				err  error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				provisionDetails = brokerapi.ProvisionDetails{PlanID: "generalPurpose"}
				asyncAllowed = true
			})

			JustBeforeEach(func() {
				spec, err = broker.Provision(ctx, instanceID, provisionDetails, asyncAllowed)
			})

			It("should not error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should provision the service instance asynchronously", func() {
				Expect(spec.IsAsync).To(Equal(true))
			})

			It("calls provision operation execute once", func() {
				Eventually(func() int {
					return fakeProvisionOperation.ExecuteCallCount()
				}, time.Second*1, time.Millisecond*100).Should(Equal(1))
			})

			Context("when the client doesnt support async", func() {
				BeforeEach(func() {
					asyncAllowed = false
				})

				It("errors", func() {
					Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
				})
			})

			Context("when the service instance already exists with different details", func() {
				// enclosing context creates initial instance
				JustBeforeEach(func() {
					provisionDetails.ServiceID = "different-service-id"
					_, err = broker.Provision(ctx, "some-instance-id", provisionDetails, true)
				})

				It("should error", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceAlreadyExists))
				})
			})
		})

		Context(".Deprovision", func() {
			var (
				instanceID       string
				asyncAllowed     bool
				provisionDetails brokerapi.ProvisionDetails

				opState efsbroker.OperationState

				err error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				provisionDetails = brokerapi.ProvisionDetails{PlanID: "generalPurpose"}
				asyncAllowed = true

				opState = efsbroker.OperationState{
					InstanceID:       instanceID,
					FsID:             "foo",
					FsState:          efs.LifeCycleStateAvailable,
					MountTargetID:    "bar",
					MountTargetState: efs.LifeCycleStateAvailable,
					MountTargetIp:    "1.2.3.4",
				}
			})

			BeforeEach(func() {
				broker.ProvisionEvent(&opState)
			})

			JustBeforeEach(func() {
				_, err = broker.Deprovision(ctx, instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
			})

			It("calls deprovision operation execute once", func() {
				Eventually(func() int {
					return fakeDeprovisionOperation.ExecuteCallCount()
				}, time.Second*1, time.Millisecond*100).Should(Equal(1))
			})

			Context("when the instance does not exist", func() {
				BeforeEach(func() {
					instanceID = "does-not-exist"
				})

				It("should fail", func() {
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})

			Context("when the client doesnt support async", func() {
				BeforeEach(func() {
					asyncAllowed = false
				})

				It("should not error", func() {
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context(".LastOperation", func() {
			var (
				instanceID string
				fsID       string

				mountID string

				op  brokerapi.LastOperation
				err error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				fsID = "12345"

				mountID = "some-mount-id"
			})

			JustBeforeEach(func() {
				op, err = broker.LastOperation(ctx, instanceID, "provision")
			})

			Context("while aws reports the fs is creating", func() {
				BeforeEach(func() {
					broker.ProvisionEvent(&efsbroker.OperationState{InstanceID: instanceID, FsID: fsID, FsState: "creating"})
				})

				It("returns in progress", func() {
					Expect(err).NotTo(HaveOccurred())
					Eventually(func() brokerapi.LastOperationState {
						return op.State
					}, time.Second*1, time.Millisecond*100).Should(Equal(brokerapi.InProgress))
				})
			})

			Context("while aws reports the fs is available", func() {
				Context("but aws reports that there are no mount targets", func() {
					BeforeEach(func() {
						broker.ProvisionEvent(&efsbroker.OperationState{InstanceID: instanceID, FsID: fsID, FsState: "available"})
					})

					It("returns in progress", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(op.State).To(Equal(brokerapi.InProgress))
					})

				})

				Context("but aws reports the mount target is still creating", func() {
					BeforeEach(func() {
						broker.ProvisionEvent(&efsbroker.OperationState{InstanceID: instanceID, FsID: fsID, FsState: "available", MountTargetID: mountID, MountTargetState: "creating"})
					})

					It("returns in progress", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(op.State).To(Equal(brokerapi.InProgress))
					})
				})

				Context("and aws reports the mount target is available", func() {
					BeforeEach(func() {
						broker.ProvisionEvent(&efsbroker.OperationState{InstanceID: instanceID, FsID: fsID, FsState: "available", MountTargetID: mountID, MountTargetState: "available", MountTargetIp: "1.2.3.4"})
					})

					It("returns in progress", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(op.State).To(Equal(brokerapi.InProgress))
					})

					Context("and permissions were set on the mount", func() {
						BeforeEach(func() {
							broker.ProvisionEvent(&efsbroker.OperationState{InstanceID: instanceID, FsID: fsID, FsState: "available", MountTargetID: mountID, MountTargetState: "available", MountPermsSet: true, MountTargetIp: "1.2.3.4"})
						})

						It("returns success", func() {
							Expect(err).NotTo(HaveOccurred())
							Expect(op.State).To(Equal(brokerapi.Succeeded))
						})
					})

				})
			})

			Context("when the instance doesn't exist", func() {
				It("errors", func() {
					op, err = broker.LastOperation(ctx, "non-existant", "provision")
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context(".Bind", func() {
			var (
				instanceID  string
				opState     efsbroker.OperationState
				bindDetails brokerapi.BindDetails
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				opState = efsbroker.OperationState{
					InstanceID:       instanceID,
					FsID:             "foo",
					FsState:          efs.LifeCycleStateAvailable,
					MountTargetID:    "bar",
					MountTargetState: efs.LifeCycleStateAvailable,
					MountPermsSet:    true,
					MountTargetIp:    "1.2.3.4",
				}

				broker.ProvisionEvent(&opState)

				bindDetails = brokerapi.BindDetails{AppGUID: "guid", Parameters: map[string]interface{}{}}
			})

			It("includes empty credentials to prevent CAPI crash", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.Credentials).NotTo(BeNil())
			})

			It("uses the instance id in the default container path", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/data/some-instance-id"))
			})

			It("flows container path through", func() {
				bindDetails.Parameters["mount"] = "/var/vcap/otherdir/something"
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/otherdir/something"))
			})

			It("uses rw as its default mode", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].Mode).To(Equal("rw"))
			})

			It("sets mode to `r` when readonly is true", func() {
				bindDetails.Parameters["readonly"] = true
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Mode).To(Equal("r"))
			})

			It("should write state", func() {
				_, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				_, data, _ := fakeIoutil.WriteFileArgsForCall(fakeIoutil.WriteFileCallCount() - 1)
				Expect(string(data)).To(Equal(`{"InstanceMap":{"some-instance-id":{"service_id":"","plan_id":"","organization_guid":"","space_guid":"","EfsId":"foo","FsState":"available","MountId":"bar","MountState":"available","MountPermsSet":true,"MountIp":"1.2.3.4","Err":null}},"BindingMap":{"binding-id":{"app_guid":"guid","plan_id":"","service_id":""}}}`))
			})

			It("errors if mode is not a boolean", func() {
				bindDetails.Parameters["readonly"] = ""
				_, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
			})

			It("fills in the driver name", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Driver).To(Equal("efsdriver"))
			})

			It("fills in the group id", func() {
				binding, err := broker.Bind(ctx, "some-instance-id", "binding-id", bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Device.VolumeId).To(Equal("some-instance-id"))
			})

			Context("when the binding already exists", func() {
				BeforeEach(func() {
					_, err := broker.Bind(ctx, "some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
					Expect(err).NotTo(HaveOccurred())
				})

				It("doesn't error when binding the same details", func() {
					_, err := broker.Bind(ctx, "some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
					Expect(err).NotTo(HaveOccurred())
				})

				It("errors when binding different details", func() {
					_, err := broker.Bind(ctx, "some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "different"})
					Expect(err).To(Equal(brokerapi.ErrBindingAlreadyExists))
				})
			})

			It("errors when the service instance does not exist", func() {
				_, err := broker.Bind(ctx, "nonexistent-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})

			It("errors when the app guid is not provided", func() {
				_, err := broker.Bind(ctx, "some-instance-id", "binding-id", brokerapi.BindDetails{})
				Expect(err).To(Equal(brokerapi.ErrAppGuidNotProvided))
			})
		})

		Context(".Unbind", func() {
			var (
				instanceID string
				opState    efsbroker.OperationState
				err        error
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				opState = efsbroker.OperationState{
					InstanceID:       instanceID,
					FsID:             "foo",
					FsState:          efs.LifeCycleStateAvailable,
					MountTargetID:    "bar",
					MountTargetState: efs.LifeCycleStateAvailable,
					MountPermsSet:    true,
					MountTargetIp:    "1.2.3.4",
				}

				broker.ProvisionEvent(&opState)

				_, err = broker.Bind(ctx, "some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
				Expect(err).NotTo(HaveOccurred())
			})

			It("unbinds a bound service instance from an app", func() {
				err := broker.Unbind(ctx, "some-instance-id", "binding-id", brokerapi.UnbindDetails{})
				Expect(err).NotTo(HaveOccurred())
			})

			It("fails when trying to unbind a instance that has not been provisioned", func() {
				err := broker.Unbind(ctx, "some-other-instance-id", "binding-id", brokerapi.UnbindDetails{})
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})

			It("fails when trying to unbind a binding that has not been bound", func() {
				err := broker.Unbind(ctx, "some-instance-id", "some-other-binding-id", brokerapi.UnbindDetails{})
				Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
			})
			It("should write state", func() {
				err := broker.Unbind(ctx, "some-instance-id", "binding-id", brokerapi.UnbindDetails{})
				Expect(err).NotTo(HaveOccurred())

				_, data, _ := fakeIoutil.WriteFileArgsForCall(fakeIoutil.WriteFileCallCount() - 1)
				Expect(string(data)).To(Equal(`{"InstanceMap":{"some-instance-id":{"service_id":"","plan_id":"","organization_guid":"","space_guid":"","EfsId":"foo","FsState":"available","MountId":"bar","MountState":"available","MountPermsSet":true,"MountIp":"1.2.3.4","Err":null}},"BindingMap":{}}`))
			})
		})

		Context(".ProvisionEvent", func() {
			var (
				opState efsbroker.OperationState
			)

			BeforeEach(func() {
				opState = efsbroker.OperationState{
					FsID:             "foo",
					FsState:          efs.LifeCycleStateAvailable,
					MountTargetID:    "bar",
					MountTargetState: efs.LifeCycleStateAvailable,
					MountPermsSet:    true,
					MountTargetIp:    "1.2.3.4",
				}
			})

			JustBeforeEach(func() {
				broker.ProvisionEvent(&opState)
			})

			It("should write state to disk", func() {
				Expect(fakeIoutil.WriteFileCallCount()).To(Equal(1))
				_, data, _ := fakeIoutil.WriteFileArgsForCall(0)
				Expect(string(data)).To(Equal(`{"InstanceMap":{"":{"service_id":"","plan_id":"","organization_guid":"","space_guid":"","EfsId":"foo","FsState":"available","MountId":"bar","MountState":"available","MountPermsSet":true,"MountIp":"1.2.3.4","Err":null}},"BindingMap":{}}`))
			})
		})

		Context(".DeprovisionEvent", func() {
			var (
				opState efsbroker.OperationState
			)

			BeforeEach(func() {
				opState = efsbroker.OperationState{
					InstanceID:       "instance1",
					FsID:             "foo",
					FsState:          efs.LifeCycleStateAvailable,
					MountTargetID:    "bar",
					MountTargetState: efs.LifeCycleStateAvailable,
					MountPermsSet:    true,
					MountTargetIp:    "1.2.3.4",
				}
				broker.ProvisionEvent(&opState)
				opState.InstanceID = "instance2"
				broker.ProvisionEvent(&opState)
				Expect(fakeIoutil.WriteFileCallCount()).To(Equal(2))
			})

			JustBeforeEach(func() {
				broker.DeprovisionEvent(&opState)
			})

			It("should write state to disk", func() {
				Expect(fakeIoutil.WriteFileCallCount()).To(Equal(3))
				_, data, _ := fakeIoutil.WriteFileArgsForCall(0)
				Expect(string(data)).To(Equal(`{"InstanceMap":{"instance1":{"service_id":"","plan_id":"","organization_guid":"","space_guid":"","EfsId":"foo","FsState":"available","MountId":"bar","MountState":"available","MountPermsSet":true,"MountIp":"1.2.3.4","Err":null}},"BindingMap":{}}`))
			})

		})

		Context("when multiple operations happen in parallel", func() {
			It("maintains consistency", func() {
				var wg sync.WaitGroup

				wg.Add(5)

				smash := func(uniqueName string) {
					defer GinkgoRecover()
					defer wg.Done()

					broker.Services(ctx)

					opState := efsbroker.OperationState{
						InstanceID:       uniqueName,
						FsID:             "foo",
						FsState:          efs.LifeCycleStateAvailable,
						MountTargetID:    "bar",
						MountTargetState: efs.LifeCycleStateAvailable,
						MountTargetIp:    "1.2.3.4",
					}

					broker.ProvisionEvent(&opState)

					_, err := broker.Bind(ctx, uniqueName, "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
					Expect(err).NotTo(HaveOccurred())

					err = broker.Unbind(ctx, uniqueName, "some-other-binding-id", brokerapi.UnbindDetails{})
					Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))

					broker.DeprovisionEvent(&opState)
				}

				// Note go race detection should kick in if access is unsynchronized
				go smash("some-instance-1")
				go smash("some-instance-2")
				go smash("some-instance-3")
				go smash("some-instance-4")
				go smash("some-instance-5")

				wg.Wait()
			})
		})
	})

	Context("when recreating", func() {
		It("should be able to bind to previously created service", func() {
			fileContents, err := json.Marshal(dynamicState{
				InstanceMap: map[string]efsbroker.EFSInstance{
					"service-name": {
						EfsId:         "service-id",
						FsState:       efs.LifeCycleStateAvailable,
						MountId:       "foo",
						MountState:    efs.LifeCycleStateAvailable,
						MountPermsSet: true,
						MountIp:       "0.0.0.0",
						Err:           nil,
					},
				},
				BindingMap: map[string]brokerapi.BindDetails{},
			})
			Expect(err).NotTo(HaveOccurred())
			fakeIoutil.ReadFileReturns(fileContents, nil)

			broker = efsbroker.New(
				logger,
				"service-name", "service-id", "/fake-dir",
				fakeOs,
				fakeIoutil,
				fakeClock,
				fakeEFSService,
				[]string{"fake-subnet-id"},
				"fake-security-group",
				fakeVolTools,
				func(lager.Logger, string, string, efsbroker.EFSService, efsvoltools.VolTools, []string, string, efsbroker.Clock, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeProvisionOperation
				},
				func(lager.Logger, efsbroker.EFSService, efsbroker.Clock, efsbroker.DeprovisionOperationSpec, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeDeprovisionOperation
				},
			)

			_, err = broker.Bind(ctx, "service-name", "whatever", brokerapi.BindDetails{AppGUID: "guid", Parameters: map[string]interface{}{}})
			Expect(err).NotTo(HaveOccurred())
		})

		It("shouldn't be able to bind to service from invalid state file", func() {
			filecontents := "{serviceName: [some invalid state]}"
			fakeIoutil.ReadFileReturns([]byte(filecontents[:]), nil)

			broker = efsbroker.New(
				logger,
				"service-name", "service-id", "/fake-dir",
				fakeOs,
				fakeIoutil,
				fakeClock,
				fakeEFSService,
				[]string{"fake-subnet-id"},
				"fake-security-group",
				fakeVolTools,
				func(lager.Logger, string, string, efsbroker.EFSService, efsvoltools.VolTools, []string, string, efsbroker.Clock, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeProvisionOperation
				},
				func(lager.Logger, efsbroker.EFSService, efsbroker.Clock, efsbroker.DeprovisionOperationSpec, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeDeprovisionOperation
				},
			)

			_, err := broker.Bind(ctx, "service-name", "whatever", brokerapi.BindDetails{AppGUID: "guid", Parameters: map[string]interface{}{}})
			Expect(err).To(HaveOccurred())
		})
	})

})
