package efsbroker_test

import (
	"errors"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/pivotal-cf/brokerapi"

	"os"

	"code.cloudfoundry.org/efsbroker/efsbroker"
	"code.cloudfoundry.org/efsbroker/efsbroker/efsfakes"
	"code.cloudfoundry.org/goshims/ioutil/ioutil_fake"
	"code.cloudfoundry.org/goshims/os/os_fake"
	"code.cloudfoundry.org/lager"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type dynamicState struct {
	InstanceMap map[string]brokerapi.ProvisionDetails
	BindingMap  map[string]brokerapi.BindDetails
}

var _ = Describe("Broker", func() {
	var (
		broker             brokerapi.ServiceBroker
		fakeOs             *os_fake.FakeOs
		fakeIoutil         *ioutil_fake.FakeIoutil
		fakeEFSService     *efsfakes.FakeEFSService
		logger             lager.Logger
		WriteFileCallCount int
		WriteFileWrote     string
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		fakeOs = &os_fake.FakeOs{}
		fakeIoutil = &ioutil_fake.FakeIoutil{}
		fakeEFSService = &efsfakes.FakeEFSService{}
		fakeIoutil.WriteFileStub = func(filename string, data []byte, perm os.FileMode) error {
			WriteFileCallCount++
			WriteFileWrote = string(data)
			return nil
		}
	})

	//Context("when recreating", func() {
	//
	//	It("should be able to bind to previously created service", func() {
	//		filecontents, err := json.Marshal(dynamicState{
	//			InstanceMap: map[string]brokerapi.ProvisionDetails{
	//				"service-name": {
	//					ServiceID:        "service-id",
	//					PlanID:           "plan-id",
	//					OrganizationGUID: "o",
	//					SpaceGUID:        "s",
	//				},
	//			},
	//			BindingMap: map[string]brokerapi.BindDetails{},
	//		})
	//		Expect(err).NotTo(HaveOccurred())
	//		fakeFs.ReadFileReturns(filecontents, nil)
	//
	//		broker = efsbroker.New(
	//			logger,
	//			"service-name", "service-id",
	//			"plan-name", "plan-id", "plan-desc", "/fake-dir",
	//			fakeFs,
	//		)
	//
	//		_, err = broker.Bind("service-name", "whatever", brokerapi.BindDetails{AppGUID: "guid", Parameters: map[string]interface{}{}})
	//		Expect(err).NotTo(HaveOccurred())
	//	})
	//
	//	It("shouldn't be able to bind to service from invalid state file", func() {
	//		filecontents := "{serviceName: [some invalid state]}"
	//		fakeFs.ReadFileReturns([]byte(filecontents[:]), nil)
	//
	//		broker = efsbroker.New(
	//			logger,
	//			"service-name", "service-id",
	//			"plan-name", "plan-id", "plan-desc", "/fake-dir",
	//			fakeFs,
	//		)
	//
	//		_, err := broker.Bind("service-name", "whatever", brokerapi.BindDetails{AppGUID: "guid", Parameters: map[string]interface{}{}})
	//		Expect(err).To(HaveOccurred())
	//	})
	//})

	Context("when creating first time", func() {
		BeforeEach(func() {
			broker = efsbroker.New(
				logger,
				"service-name", "service-id",
				"plan-name", "plan-id", "plan-desc", "/fake-dir",
				fakeOs,
				fakeIoutil,
				fakeEFSService,
			)
			fakeEFSService.CreateFileSystemReturns(&efs.FileSystemDescription{FileSystemId: aws.String("fakeFS")}, nil)
		})

		Context(".Services", func() {
			It("returns the service catalog as appropriate", func() {
				result := broker.Services()[0]
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
			It("should provision the service instance", func() {
				spec, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{PlanID: "generalPurpose"}, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(fakeEFSService.CreateFileSystemCallCount()).To(Equal(1))
				Expect(spec.IsAsync).To(Equal(true))

				input := fakeEFSService.CreateFileSystemArgsForCall(0)
				Expect(*input.PerformanceMode).To(Equal("generalPurpose"))
				Expect(*input.CreationToken).To(Equal("some-instance-id"))
			})

			It("should provision the service instance with maxIO", func() {
				_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{PlanID: "maxIO"}, true)
				Expect(err).NotTo(HaveOccurred())
				input := fakeEFSService.CreateFileSystemArgsForCall(0)
				Expect(*input.PerformanceMode).To(Equal("maxIO"))
			})

			Context("when provisioning errors", func() {
				BeforeEach(func() {
					fakeEFSService.CreateFileSystemReturns(nil, errors.New("badness"))
				})

				It("errors", func() {
					_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the client doesnt support async", func() {
				It("errors", func() {
					_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, false)
					Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
				})
			})

			Context("when provisioning returns a nil FileSystemID?", func() {})

			Context("when the service instance already exists with different details", func() {
				var details brokerapi.ProvisionDetails
				BeforeEach(func() {
					details = brokerapi.ProvisionDetails{
						ServiceID:        "service-id",
						PlanID:           "plan-id",
						OrganizationGUID: "org-guid",
						SpaceGUID:        "space-guid",
					}
					_, err := broker.Provision("some-instance-id", details, true)
					Expect(err).NotTo(HaveOccurred())
				})

				It("should error", func() {
					details.ServiceID = "different-service-id"
					_, err := broker.Provision("some-instance-id", details, true)
					Expect(err).To(Equal(brokerapi.ErrInstanceAlreadyExists))
				})
			})

			It("should write state", func() {
				WriteFileCallCount = 0
				WriteFileWrote = ""
				_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(WriteFileCallCount).To(Equal(1))
				Expect(WriteFileWrote).To(Equal("{\"InstanceMap\":{\"some-instance-id\":{\"service_id\":\"\",\"plan_id\":\"\",\"organization_guid\":\"\",\"space_guid\":\"\"}},\"BindingMap\":{}}"))
			})
		})

		Context(".Deprovision", func() {
			BeforeEach(func() {
				_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should deprovision the service", func() {
				_, err := broker.Deprovision("some-instance-id", brokerapi.DeprovisionDetails{}, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(fakeEFSService.DeleteFileSystemCallCount()).To(Equal(1))

				By("checking that we can reprovision a slightly different service")
				_, err = broker.Provision("some-instance-id", brokerapi.ProvisionDetails{ServiceID: "different-service-id"}, true)
				Expect(err).NotTo(Equal(brokerapi.ErrInstanceAlreadyExists))
			})

			It("errors when the service instance does not exist", func() {
				_, err := broker.Deprovision("some-nonexistant-instance-id", brokerapi.DeprovisionDetails{}, true)
				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
			})

			Context("when the provisioner fails to remove", func() {
				BeforeEach(func() {
					fakeEFSService.DeleteFileSystemReturns(nil, errors.New("generic aws error"))
				})

				It("should error", func() {
					_, err := broker.Deprovision("some-instance-id", brokerapi.DeprovisionDetails{}, true)
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the client doesnt support async", func() {
				It("should error", func() {
					_, err := broker.Deprovision("some-instance-id", brokerapi.DeprovisionDetails{}, false)
					Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
				})
			})

			It("should write state", func() {
				WriteFileCallCount = 0
				WriteFileWrote = ""
				_, err := broker.Deprovision("some-instance-id", brokerapi.DeprovisionDetails{}, true)
				Expect(err).NotTo(HaveOccurred())

				Expect(WriteFileCallCount).To(Equal(1))
				Expect(WriteFileWrote).To(Equal("{\"InstanceMap\":{},\"BindingMap\":{}}"))
			})
		})

		Context(".LastOperation", func() {
			BeforeEach(func() {
				_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
				Expect(err).NotTo(HaveOccurred())
			})

			Context("while aws reports the fs is creating", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateCreating)}},
					}, nil)
				})

				It("returns in progress", func() {
					op, err := broker.LastOperation("some-instance-id", "")
					Expect(err).NotTo(HaveOccurred())
					Expect(op.State).To(Equal(brokerapi.InProgress))
				})
			})

			Context("while aws reports the fs is available", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateAvailable)}},
					}, nil)
				})

				It("returns successful", func() {
					op, err := broker.LastOperation("some-instance-id", "")
					Expect(err).NotTo(HaveOccurred())
					Expect(op.State).To(Equal(brokerapi.Succeeded))
				})
			})

			Context("when calling out to aws fails ", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{}, errors.New("badness"))
				})

				It("errors", func() {
					_, err := broker.LastOperation("some-instance-id", "")
					Expect(err).To(Equal(errors.New("badness")))
				})
			})

			Context("when calling out to aws returns too many file systems", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{
							{LifeCycleState: aws.String(efs.LifeCycleStateAvailable)},
							{LifeCycleState: aws.String(efs.LifeCycleStateAvailable)},
						},
					}, nil)
				})

				It("errors", func() {
					_, err := broker.LastOperation("some-instance-id", "")
					Expect(err).To(HaveOccurred())
				})
			})

			Context("when the instance doesn't exist", func() {
				It("errors", func() {
					state, err := broker.LastOperation("nonexistant", "")
					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
					Expect(state).To(Equal(brokerapi.LastOperation{State: brokerapi.Failed}))
				})
			})
		})
		//
		//Context(".Bind", func() {
		//	var bindDetails brokerapi.BindDetails
		//
		//	BeforeEach(func() {
		//		_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		bindDetails = brokerapi.BindDetails{AppGUID: "guid", Parameters: map[string]interface{}{}}
		//	})
		//
		//	It("includes empty credentials to prevent CAPI crash", func() {
		//		binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		Expect(binding.Credentials).NotTo(BeNil())
		//	})
		//
		//	It("uses the instance id in the default container path", func() {
		//		binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//		Expect(binding.VolumeMounts[0].ContainerPath).To(Equal("/var/vcap/data/some-instance-id"))
		//	})
		//
		//	It("flows container path through", func() {
		//		bindDetails.Parameters["mount"] = "/var/vcap/otherdir/something"
		//		binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//		Expect(binding.VolumeMounts[0].ContainerPath).To(Equal("/var/vcap/otherdir/something"))
		//	})
		//
		//	It("uses rw as its default mode", func() {
		//		binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//		Expect(binding.VolumeMounts[0].Mode).To(Equal("rw"))
		//	})
		//
		//	It("sets mode to `r` when readonly is true", func() {
		//		bindDetails.Parameters["readonly"] = true
		//		binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		Expect(binding.VolumeMounts[0].Mode).To(Equal("r"))
		//	})
		//
		//	It("should write state", func() {
		//		WriteFileCallCount = 0
		//		WriteFileWrote = ""
		//		_, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		Expect(WriteFileCallCount).To(Equal(1))
		//		Expect(WriteFileWrote).To(Equal("{\"InstanceMap\":{\"some-instance-id\":{\"service_id\":\"\",\"plan_id\":\"\",\"organization_guid\":\"\",\"space_guid\":\"\"}},\"BindingMap\":{\"binding-id\":{\"app_guid\":\"guid\",\"plan_id\":\"\",\"service_id\":\"\"}}}"))
		//	})
		//
		//	It("errors if mode is not a boolean", func() {
		//		bindDetails.Parameters["readonly"] = ""
		//		_, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
		//	})
		//
		//	It("fills in the driver name", func() {
		//		binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		Expect(binding.VolumeMounts[0].Private.Driver).To(Equal("localdriver"))
		//	})
		//
		//	It("fills in the group id", func() {
		//		binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		Expect(binding.VolumeMounts[0].Private.GroupId).To(Equal("some-instance-id"))
		//	})
		//
		//	Context("when the binding already exists", func() {
		//		BeforeEach(func() {
		//			_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
		//			Expect(err).NotTo(HaveOccurred())
		//		})
		//
		//		It("doesn't error when binding the same details", func() {
		//			_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
		//			Expect(err).NotTo(HaveOccurred())
		//		})
		//
		//		It("errors when binding different details", func() {
		//			_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "different"})
		//			Expect(err).To(Equal(brokerapi.ErrBindingAlreadyExists))
		//		})
		//	})
		//
		//	It("errors when the service instance does not exist", func() {
		//		_, err := broker.Bind("nonexistant-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
		//		Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
		//	})
		//
		//	It("errors when the app guid is not provided", func() {
		//		_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{})
		//		Expect(err).To(Equal(brokerapi.ErrAppGuidNotProvided))
		//	})
		//})
		//
		//Context(".Unbind", func() {
		//	BeforeEach(func() {
		//		_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		_, err = broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
		//		Expect(err).NotTo(HaveOccurred())
		//	})
		//
		//	It("unbinds a bound service instance from an app", func() {
		//		err := broker.Unbind("some-instance-id", "binding-id", brokerapi.UnbindDetails{})
		//		Expect(err).NotTo(HaveOccurred())
		//	})
		//
		//	It("fails when trying to unbind a instance that has not been provisioned", func() {
		//		err := broker.Unbind("some-other-instance-id", "binding-id", brokerapi.UnbindDetails{})
		//		Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
		//	})
		//
		//	It("fails when trying to unbind a binding that has not been bound", func() {
		//		err := broker.Unbind("some-instance-id", "some-other-binding-id", brokerapi.UnbindDetails{})
		//		Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
		//	})
		//	It("should write state", func() {
		//		WriteFileCallCount = 0
		//		WriteFileWrote = ""
		//		err := broker.Unbind("some-instance-id", "binding-id", brokerapi.UnbindDetails{})
		//		Expect(err).NotTo(HaveOccurred())
		//
		//		Expect(WriteFileCallCount).To(Equal(1))
		//		Expect(WriteFileWrote).To(Equal("{\"InstanceMap\":{\"some-instance-id\":{\"service_id\":\"\",\"plan_id\":\"\",\"organization_guid\":\"\",\"space_guid\":\"\"}},\"BindingMap\":{}}"))
		//	})
		//
		//})
		//Context("when multiple operations happen in parallel", func() {
		//	It("maintains consistency", func() {
		//		var wg sync.WaitGroup
		//
		//		wg.Add(2)
		//
		//		smash := func(uniqueName string) {
		//			defer GinkgoRecover()
		//			defer wg.Done()
		//
		//			broker.Services()
		//
		//			_, err := broker.Provision(uniqueName, brokerapi.ProvisionDetails{}, true)
		//			Expect(err).NotTo(HaveOccurred())
		//
		//			_, err = broker.Bind(uniqueName, "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
		//			Expect(err).NotTo(HaveOccurred())
		//
		//			err = broker.Unbind(uniqueName, "some-other-binding-id", brokerapi.UnbindDetails{})
		//			Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
		//
		//			_, err = broker.Deprovision(uniqueName, brokerapi.DeprovisionDetails{}, true)
		//			Expect(err).NotTo(HaveOccurred())
		//		}
		//
		//		// Note go race detection should kick in if access is unsynchronized
		//		go smash("some-instance-1")
		//		go smash("some-instance-2")
		//
		//		wg.Wait()
		//	})
		//})
	})
})
