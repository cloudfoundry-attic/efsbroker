package efsbroker_test

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/pivotal-cf/brokerapi"

	"code.cloudfoundry.org/clock/fakeclock"
	"code.cloudfoundry.org/efsbroker/efsbroker"
	"code.cloudfoundry.org/efsbroker/efsbroker/efsfakes"
	"code.cloudfoundry.org/efsdriver/efsdriverfakes"
	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"code.cloudfoundry.org/service-broker-store/brokerstore"
	"code.cloudfoundry.org/service-broker-store/brokerstore/brokerstorefakes"

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
		fakeEFSService           *efsfakes.FakeEFSService
		fakeClock                *fakeclock.FakeClock
		fakeVolTools             *efsdriverfakes.FakeVolTools
		fakeProvisionOperation   *efsfakes.FakeOperation
		fakeDeprovisionOperation *efsfakes.FakeOperation
		logger                   lager.Logger
		ctx                      context.Context
		fakeStore                *brokerstorefakes.FakeStore
	)

	const (
		instanceID     = "some-instance-id"
		bindingID      = "binding-id"
		fsID           = "fake-fs-id"
		mountID        = "fake-mt-id"
		serviceName    = "service-name"
		serviceID      = "service-id"
		nfsServerIP    = "1.1.1.1"
		nfsServerAZ    = "something-something-1"
		fakeTargetPath = "/fake-dir"
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		ctx = context.TODO()
		fakeOs = &os_fake.FakeOs{}
		fakeClock = fakeclock.NewFakeClock(time.Unix(123, 456))
		fakeEFSService = &efsfakes.FakeEFSService{}
		fakeVolTools = &efsdriverfakes.FakeVolTools{}
		fakeProvisionOperation = &efsfakes.FakeOperation{}
		fakeDeprovisionOperation = &efsfakes.FakeOperation{}
		fakeStore = &brokerstorefakes.FakeStore{}

		fakeEFSService.CreateFileSystemReturns(&efs.FileSystemDescription{
			FileSystemId: aws.String(fsID),
		}, nil)
		fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
			FileSystems: []*efs.FileSystemDescription{{
				FileSystemId:   aws.String(fsID),
				LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
			}},
		}, nil)
		fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
			MountTargets: []*efs.MountTargetDescription{{
				MountTargetId:  aws.String(mountID),
				LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
				IpAddress:      aws.String(nfsServerIP),
			}},
		}, nil)
		fakeEFSService.CreateMountTargetReturns(&efs.MountTargetDescription{
			MountTargetId: aws.String(mountID),
		}, nil)
	})

	Context("when creating first time", func() {
		BeforeEach(func() {
			broker = efsbroker.New(
				logger,
				serviceName, serviceID, fakeTargetPath,
				fakeOs,
				fakeClock,
				fakeStore,
				fakeEFSService,
				[]efsbroker.Subnet{{"fake-subnet-id", "fake-az", "fake-security-group"}},
				fakeVolTools,
				func(lager.Logger, string, brokerapi.ProvisionDetails, efsbroker.EFSService, efsvoltools.VolTools, []efsbroker.Subnet, efsbroker.Clock, func(*efsbroker.OperationState)) efsbroker.Operation {
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
				Expect(result.ID).To(Equal(serviceID))
				Expect(result.Name).To(Equal(serviceName))
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
				BeforeEach(func() {
					fakeStore.IsInstanceConflictReturns(true)
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
				fakeInstance     brokerstore.ServiceInstance

				err error
			)

			BeforeEach(func() {
				provisionDetails = brokerapi.ProvisionDetails{PlanID: "generalPurpose"}
				asyncAllowed = true

				fakeInstance = brokerstore.ServiceInstance{
					ServiceID:        instanceID,
					PlanID:           "fake-plan-id",
					OrganizationGUID: "fake-org-guid",
					SpaceGUID:        "fake-space-guid",
					ServiceFingerPrint: efsbroker.EFSInstance{
						EfsId:      "foo",
						FsState:    efs.LifeCycleStateAvailable,
						MountId:    "bar",
						MountIp:    "1.2.3.4",
						MountState: efs.LifeCycleStateAvailable,
					},
				}

			})

			BeforeEach(func() {
				fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)
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
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("Not Found"))
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
				instanceID   string
				fsID         string
				mountID      string
				err          error
				op           brokerapi.LastOperation
				fakeInstance brokerstore.ServiceInstance
			)

			BeforeEach(func() {
				fsID = "12345"
				mountID = "some-mount-id"

			})

			JustBeforeEach(func() {
				op, err = broker.LastOperation(ctx, instanceID, "provision")
			})

			Context("while aws reports the fs is creating", func() {
				BeforeEach(func() {

					fakeInstance = brokerstore.ServiceInstance{
						ServiceID:        instanceID,
						PlanID:           "fake-plan-id",
						OrganizationGUID: "fake-org-guid",
						SpaceGUID:        "fake-space-guid",
						ServiceFingerPrint: efsbroker.EFSInstance{
							EfsId:   fsID,
							FsState: "creating",
						},
					}

					fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)
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
						fakeInstance = brokerstore.ServiceInstance{
							ServiceID:        instanceID,
							PlanID:           "fake-plan-id",
							OrganizationGUID: "fake-org-guid",
							SpaceGUID:        "fake-space-guid",
							ServiceFingerPrint: efsbroker.EFSInstance{
								EfsId:   fsID,
								FsState: "available",
							},
						}
						fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)
					})

					It("returns in progress", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(op.State).To(Equal(brokerapi.InProgress))
					})

				})

				Context("but aws reports the mount target is still creating", func() {
					BeforeEach(func() {
						fakeInstance = brokerstore.ServiceInstance{
							ServiceID:        instanceID,
							PlanID:           "fake-plan-id",
							OrganizationGUID: "fake-org-guid",
							SpaceGUID:        "fake-space-guid",
							ServiceFingerPrint: efsbroker.EFSInstance{
								EfsId:      fsID,
								FsState:    "available",
								MountId:    mountID,
								MountState: "creating",
							},
						}
						fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)
					})

					It("returns in progress", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(op.State).To(Equal(brokerapi.InProgress))
					})
				})

				Context("and aws reports the mount target is available", func() {
					BeforeEach(func() {
						fakeInstance = brokerstore.ServiceInstance{
							ServiceID:        instanceID,
							PlanID:           "fake-plan-id",
							OrganizationGUID: "fake-org-guid",
							SpaceGUID:        "fake-space-guid",
							ServiceFingerPrint: efsbroker.EFSInstance{
								EfsId:      fsID,
								FsState:    "available",
								MountId:    mountID,
								MountState: "available",
								MountIp:    "1.2.3.4",
							},
						}
						fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)
					})

					It("returns in progress", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(op.State).To(Equal(brokerapi.InProgress))
					})

					Context("and permissions were set on the mount", func() {
						BeforeEach(func() {
							fakeInstance = brokerstore.ServiceInstance{
								ServiceID:        instanceID,
								PlanID:           "fake-plan-id",
								OrganizationGUID: "fake-org-guid",
								SpaceGUID:        "fake-space-guid",
								ServiceFingerPrint: efsbroker.EFSInstance{
									EfsId:         fsID,
									FsState:       "available",
									MountId:       mountID,
									MountState:    "available",
									MountIp:       "1.2.3.4",
									MountPermsSet: true,
								},
							}
							fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)
						})

						It("returns success", func() {
							Expect(err).NotTo(HaveOccurred())
							Expect(op.State).To(Equal(brokerapi.Succeeded))
						})
					})

				})
			})

			Context("when the instance doesn't exist", func() {
				BeforeEach(func() {
					fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("not found"))
				})

				It("errors", func() {
					op, err = broker.LastOperation(ctx, "non-existant", "provision")
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
				})
			})
		})

		Context(".Bind", func() {
			var (
				bindDetails        brokerapi.BindDetails
				fakeInstance       brokerstore.ServiceInstance
				fakeLegacyInstance brokerstore.ServiceInstance
			)

			BeforeEach(func() {
				fakeInstance = brokerstore.ServiceInstance{
					ServiceID:        instanceID,
					PlanID:           "fake-plan-id",
					OrganizationGUID: "fake-org-guid",
					SpaceGUID:        "fake-space-guid",
					ServiceFingerPrint: efsbroker.EFSInstance{
						EfsId:         "foo",
						FsState:       efs.LifeCycleStateAvailable,
						MountId:       "bar",
						MountState:    efs.LifeCycleStateAvailable,
						MountIp:       nfsServerIP,
						MountIps:      []string{nfsServerIP},
						MountAZs:      []string{nfsServerAZ},
						MountPermsSet: true,
					},
				}
				fakeLegacyInstance = brokerstore.ServiceInstance{
					ServiceID:        instanceID,
					PlanID:           "fake-plan-id",
					OrganizationGUID: "fake-org-guid",
					SpaceGUID:        "fake-space-guid",
					ServiceFingerPrint: efsbroker.EFSInstance{
						EfsId:         "foo",
						FsState:       efs.LifeCycleStateAvailable,
						MountId:       "bar",
						MountState:    efs.LifeCycleStateAvailable,
						MountIp:       nfsServerIP,
						MountPermsSet: true,
					},
				}
				fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)

				bindDetails = brokerapi.BindDetails{AppGUID: "guid"}
			})

			It("includes empty credentials to prevent CAPI crash", func() {
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.Credentials).NotTo(BeNil())
			})

			It("uses the instance id in the default container path", func() {
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/data/some-instance-id"))
			})

			It("flows container path through", func() {
				var err error
				params := map[string]interface{}{"mount": "/var/vcap/otherdir/something"}
				bindDetails.RawParameters, err = json.Marshal(params)
				Expect(err).NotTo(HaveOccurred())
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/otherdir/something"))
			})

			It("uses rw as its default mode", func() {
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())
				Expect(binding.VolumeMounts[0].Mode).To(Equal("rw"))
			})

			It("sets mode to `r` when readonly is true", func() {
				var err error
				params := map[string]interface{}{"readonly": true}
				bindDetails.RawParameters, err = json.Marshal(params)
				Expect(err).NotTo(HaveOccurred())
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Mode).To(Equal("r"))
			})

			It("should send the ip in the Opts", func() {
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(binding.VolumeMounts)).To(Equal(1))
				Expect(binding.VolumeMounts[0].Device.MountConfig["source"]).To(Equal(nfsServerIP + efsbroker.RootPath))
			})
			It("should still send the ip in the Opts if the service instance is old", func() {
				fakeStore.RetrieveInstanceDetailsReturns(fakeLegacyInstance, nil)
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(binding.VolumeMounts)).To(Equal(1))
				Expect(binding.VolumeMounts[0].Device.MountConfig["source"]).To(Equal(nfsServerIP + efsbroker.RootPath))
			})
			It("should map the ip to the az in the Opts", func() {
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(binding.VolumeMounts)).To(Equal(1))
				azMap := binding.VolumeMounts[0].Device.MountConfig["az-map"].(map[string]interface{})
				Expect(azMap[nfsServerAZ]).To(Equal(nfsServerIP + efsbroker.RootPath))
			})

			It("errors if mode is not a boolean", func() {
				var err error
				params := map[string]interface{}{"readonly": "true"}
				bindDetails.RawParameters, err = json.Marshal(params)
				Expect(err).NotTo(HaveOccurred())
				_, err = broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
			})

			It("fills in the driver name", func() {
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Driver).To(Equal("efsdriver"))
			})

			It("fills in the group id", func() {
				binding, err := broker.Bind(ctx, instanceID, bindingID, bindDetails)
				Expect(err).NotTo(HaveOccurred())

				Expect(binding.VolumeMounts[0].Device.VolumeId).To(Equal(instanceID))
			})

			Context("when the binding already exists", func() {
				It("doesn't error when binding the same details", func() {
					fakeStore.IsBindingConflictReturns(false)
					_, err := broker.Bind(ctx, instanceID, bindingID, brokerapi.BindDetails{AppGUID: "guid"})
					Expect(err).NotTo(HaveOccurred())
				})

				It("errors when binding different details", func() {
					fakeStore.IsBindingConflictReturns(true)
					_, err := broker.Bind(ctx, instanceID, bindingID, brokerapi.BindDetails{AppGUID: "different"})
					Expect(err).To(Equal(brokerapi.ErrBindingAlreadyExists))
				})
			})

			It("errors when the service instance does not exist", func() {
				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("not found"))
				_, err := broker.Bind(ctx, "nonexistent-instance-id", bindingID, brokerapi.BindDetails{AppGUID: "guid"})
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})

			It("errors when the app guid is not provided", func() {
				_, err := broker.Bind(ctx, instanceID, bindingID, brokerapi.BindDetails{})
				Expect(err).To(Equal(brokerapi.ErrAppGuidNotProvided))
			})
		})

		Context(".Unbind", func() {
			var (
				instanceID   string
				fakeInstance brokerstore.ServiceInstance
			)

			BeforeEach(func() {
				instanceID = "some-instance-id"
				fakeInstance = brokerstore.ServiceInstance{
					ServiceID:        instanceID,
					PlanID:           "fake-plan-id",
					OrganizationGUID: "fake-org-guid",
					SpaceGUID:        "fake-space-guid",
					ServiceFingerPrint: efsbroker.EFSInstance{
						EfsId:         "foo",
						FsState:       efs.LifeCycleStateAvailable,
						MountId:       "bar",
						MountState:    efs.LifeCycleStateAvailable,
						MountIp:       "1.2.3.4",
						MountPermsSet: true,
					},
				}
				fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)

			})

			It("unbinds a bound service instance from an app", func() {
				err := broker.Unbind(ctx, instanceID, bindingID, brokerapi.UnbindDetails{})
				Expect(err).NotTo(HaveOccurred())
			})

			It("fails when trying to unbind a instance that has not been provisioned", func() {
				fakeStore.RetrieveInstanceDetailsReturns(brokerstore.ServiceInstance{}, errors.New("not found"))
				err := broker.Unbind(ctx, "some-other-instance-id", bindingID, brokerapi.UnbindDetails{})
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})

			It("fails when trying to unbind a binding that has not been bound", func() {
				fakeStore.RetrieveBindingDetailsReturns(brokerapi.BindDetails{}, errors.New("not found"))
				err := broker.Unbind(ctx, instanceID, "some-other-binding-id", brokerapi.UnbindDetails{})
				Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
			})

			It("should delete binding in store", func() {
				err := broker.Unbind(ctx, instanceID, bindingID, brokerapi.UnbindDetails{})
				Expect(err).NotTo(HaveOccurred())
				Expect(fakeStore.DeleteBindingDetailsCallCount()).To(Equal(1))
			})
		})

		Context(".ProvisionEvent", func() {
			var (
				opState efsbroker.OperationState
			)

			BeforeEach(func() {
				opState = efsbroker.OperationState{
					FsID:              "foo",
					FsState:           efs.LifeCycleStateAvailable,
					MountTargetIDs:    []string{"bar"},
					MountTargetStates: []string{efs.LifeCycleStateAvailable},
					MountPermsSet:     true,
					MountTargetIps:    []string{"1.2.3.4"},
				}
			})

			JustBeforeEach(func() {
				broker.ProvisionEvent(&opState)
			})

			It("should update state", func() {
				Expect(fakeStore.DeleteInstanceDetailsCallCount()).To(Equal(1))
				Expect(fakeStore.CreateInstanceDetailsCallCount()).To(Equal(1))
			})
		})

		Context(".DeprovisionEvent", func() {
			var (
				opState efsbroker.OperationState
			)

			BeforeEach(func() {
				opState = efsbroker.OperationState{
					InstanceID:        "instance1",
					FsID:              "foo",
					FsState:           efs.LifeCycleStateAvailable,
					MountTargetIDs:    []string{"bar"},
					MountTargetStates: []string{efs.LifeCycleStateAvailable},
					MountPermsSet:     true,
					MountTargetIps:    []string{"1.2.3.4"},
				}
			})

			JustBeforeEach(func() {
				broker.DeprovisionEvent(&opState)
			})

			It("should delete instance state", func() {
				Expect(fakeStore.DeleteInstanceDetailsCallCount()).To(Equal(1))
			})

		})
	})

	Context("when recreating", func() {
		var (
			fakeInstance brokerstore.ServiceInstance
			err          error
		)
		It("should be able to bind to previously created service", func() {
			fakeInstance = brokerstore.ServiceInstance{
				ServiceID:        instanceID,
				PlanID:           "fake-plan-id",
				OrganizationGUID: "fake-org-guid",
				SpaceGUID:        "fake-space-guid",
				ServiceFingerPrint: efsbroker.EFSInstance{
					EfsId:         serviceID,
					FsState:       efs.LifeCycleStateAvailable,
					MountId:       "foo",
					MountState:    efs.LifeCycleStateAvailable,
					MountIp:       "0.0.0.0",
					MountPermsSet: true,
					Err:           nil,
				},
			}
			fakeStore.RetrieveInstanceDetailsReturns(fakeInstance, nil)

			broker = efsbroker.New(
				logger,
				serviceName, serviceID, fakeTargetPath,
				fakeOs,
				fakeClock,
				fakeStore,
				fakeEFSService,
				[]efsbroker.Subnet{{"fake-subnet-id", "fake-az", "fake-security-group"}},
				fakeVolTools,
				func(lager.Logger, string, brokerapi.ProvisionDetails, efsbroker.EFSService, efsvoltools.VolTools, []efsbroker.Subnet, efsbroker.Clock, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeProvisionOperation
				},
				func(lager.Logger, efsbroker.EFSService, efsbroker.Clock, efsbroker.DeprovisionOperationSpec, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeDeprovisionOperation
				},
			)

			_, err = broker.Bind(ctx, serviceName, "whatever", brokerapi.BindDetails{AppGUID: "guid"})
			Expect(err).NotTo(HaveOccurred())
		})

		It("shouldn't be able to bind to service from invalid state file", func() {
			broker = efsbroker.New(
				logger,
				serviceName, serviceID, fakeTargetPath,
				fakeOs,
				fakeClock,
				fakeStore,
				fakeEFSService,
				[]efsbroker.Subnet{{"fake-subnet-id", "fake-az", "fake-security-group"}},
				fakeVolTools,
				func(lager.Logger, string, brokerapi.ProvisionDetails, efsbroker.EFSService, efsvoltools.VolTools, []efsbroker.Subnet, efsbroker.Clock, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeProvisionOperation
				},
				func(lager.Logger, efsbroker.EFSService, efsbroker.Clock, efsbroker.DeprovisionOperationSpec, func(*efsbroker.OperationState)) efsbroker.Operation {
					return fakeDeprovisionOperation
				},
			)

			_, err := broker.Bind(ctx, serviceName, "whatever", brokerapi.BindDetails{AppGUID: "guid"})
			Expect(err).To(HaveOccurred())
		})
	})

})
