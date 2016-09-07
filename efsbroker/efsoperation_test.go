package efsbroker_test

import (
	"os"

	"fmt"

	"errors"

	"code.cloudfoundry.org/efsbroker/efsbroker"
	"code.cloudfoundry.org/efsbroker/efsbroker/efsfakes"
	"code.cloudfoundry.org/efsdriver/efsdriverfakes"
	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/goshims/ioutil/ioutil_fake"
	"code.cloudfoundry.org/goshims/os/os_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

//type dynamicState struct {
//	InstanceMap map[string]brokerapi.ProvisionDetails
//	BindingMap  map[string]brokerapi.BindDetails
//}

func BeState(expected interface{}) types.GomegaMatcher {
	return &beStateMatcher{
		expected: expected,
	}
}

type beStateMatcher struct {
	expected interface{}
}

func (matcher *beStateMatcher) Match(actual interface{}) (success bool, err error) {
	if !(fmt.Sprintf("%#v", actual) == fmt.Sprintf("%#v", matcher.expected)) {
		return false, fmt.Errorf("Function pointers not aligned %#v != %#v", actual, matcher.expected)
	}
	return true, nil
}

func (matcher *beStateMatcher) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nto be \n\t%#v", actual, matcher.expected)
}

func (matcher *beStateMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nnot to be \n\t%#v", actual, matcher.expected)
}

var _ = Describe("Operation", func() {

	var (
		provisionOp        *efsbroker.ProvisionOperationStateMachine
		fakeOs             *os_fake.FakeOs
		fakeIoutil         *ioutil_fake.FakeIoutil
		fakeEFSService     *efsfakes.FakeEFSService
		fakeVolTools       *efsdriverfakes.FakeVolTools
		fakeClock          *efsfakes.FakeClock
		logger             lager.Logger
		WriteFileCallCount int
		WriteFileWrote     string
		filesystemID       *string
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		fakeOs = &os_fake.FakeOs{}
		fakeIoutil = &ioutil_fake.FakeIoutil{}
		fakeClock = &efsfakes.FakeClock{}
		fakeEFSService = &efsfakes.FakeEFSService{}
		fakeVolTools = &efsdriverfakes.FakeVolTools{}
		fakeIoutil.WriteFileStub = func(filename string, data []byte, perm os.FileMode) error {
			WriteFileCallCount++
			WriteFileWrote = string(data)
			return nil
		}
		provisionOp = efsbroker.NewProvisionStateMachine(logger, "instanceID", "planId", fakeEFSService, fakeVolTools, []string{"subnet-id-1", "subnet-id-2"}, "security-group-id", nil, fakeClock)
		filesystemID = aws.String("fake-fs-id")
	})

	Context(".Start", func() {
		JustBeforeEach(func() {
			provisionOp.Start()
		})
		Context("when amazon's create file system returns ok", func() {
			BeforeEach(func() {
				fakeEFSService.CreateFileSystemReturns(&efs.FileSystemDescription{
					FileSystemId: filesystemID,
				}, nil)
			})
			It("should move to check-for-fs state when amazon fails to acknowledge", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.CheckForFs))
			})
		})
		Context("when amazon's create file system returns error", func() {
			BeforeEach(func() {
				fakeEFSService.CreateFileSystemReturns(nil, fmt.Errorf("some err"))
			})
			It("should error when amazon fails to acknowledge", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
	})

	Context(".Sleep", func() {
		JustBeforeEach(func() {
			provisionOp.Sleep()
		})
		Context("when sleep is called and next state is Finish", func() {
			BeforeEach(func() {
				provisionOp.StateAfterSleep(provisionOp.Finish)
			})
			It("should sleep and then move to the finish state", func() {
				Expect(fakeClock.SleepCallCount()).To(Equal(1))
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
	})

	Context(".CheckFS", func() {
		JustBeforeEach(func() {
			provisionOp.CheckForFs()
		})
		Context("when amazon's describe file system returns creating", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
					FileSystems: []*efs.FileSystemDescription{{
						FileSystemId:   filesystemID,
						LifeCycleState: aws.String(efs.LifeCycleStateCreating),
					}},
				}, nil)
			})
			It("should sleep and remain in check-for-fs state", func() {
				provisionOp.Sleep()
				Expect(provisionOp.State()).To(BeState(provisionOp.CheckForFs))
			})
		})
		Context("when amazon's describe file system returns available", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
					FileSystems: []*efs.FileSystemDescription{{
						FileSystemId:   filesystemID,
						LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
					}},
				}, nil)
			})
			It("should move to create-mount-target state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.CreateMountTarget))
			})
		})
		Context("when amazon's describe file system returns an unexpected state", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
					FileSystems: []*efs.FileSystemDescription{{
						FileSystemId:   filesystemID,
						LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
					}},
				}, nil)
			})
			It("should sleep and remain in check fs state", func() {
				provisionOp.Sleep()
				Expect(provisionOp.State()).To(BeState(provisionOp.CheckForFs))
			})
		})
		Context("when amazon's describe file system returns no lifecycle at all", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
					FileSystems: []*efs.FileSystemDescription{{
						FileSystemId:   filesystemID,
						LifeCycleState: nil,
					}},
				}, nil)
			})
			It("should move to finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
		Context("when amazon's describe file system returns an error", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeFileSystemsReturns(nil, errors.New("badness"))
			})
			It("should move to finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
		Context("when amazon's describe file system returns no file systems", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
					FileSystems: []*efs.FileSystemDescription{{}},
				}, nil)
			})
			It("should move to finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
		Context("when amazon's describe file system returns lots of file systems", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
					FileSystems: []*efs.FileSystemDescription{{
						FileSystemId:   filesystemID,
						LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
					}, {
						FileSystemId:   aws.String("another-fs-id"),
						LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
					}},
				}, nil)
			})
			It("should move to finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
	})

	Context(".CreateMountTarget", func() {
		JustBeforeEach(func() {
			provisionOp.CreateMountTarget()
		})
		Context("when amazon's create mount target returns an error", func() {
			BeforeEach(func() {
				fakeEFSService.CreateMountTargetReturns(nil, errors.New("badness"))
			})
			It("should move to the finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
		Context("when amazon's create mount target return successfully", func() {
			BeforeEach(func() {
				fakeEFSService.CreateMountTargetReturns(nil, nil)
			})
			It("should move to the finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.CheckMountTarget))
			})
		})
	})

	Context(".CheckMountTarget", func() {
		JustBeforeEach(func() {
			provisionOp.CheckMountTarget()
		})
		Context("when amazon's describe mount target return successfully", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
					MountTargets: []*efs.MountTargetDescription{{
						MountTargetId:  aws.String("fake-mt-id"),
						LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
					}},
				}, nil)
			})
			It("should move to the get-mount-ip state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.OpenPerms))
			})
		})
		Context("when amazon's describe mount target returns an error", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeMountTargetsReturns(nil, errors.New("badness"))
			})
			It("should move to the finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
		Context("when amazon's describe mount target returns empty mount targets", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
					MountTargets: []*efs.MountTargetDescription{},
				}, nil)
			})
			It("should move to the finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
		Context("when amazon's describe mount target returns many mount targets", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
					MountTargets: []*efs.MountTargetDescription{{
						MountTargetId:  aws.String("fake-mt-id-1"),
						LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
					}, {
						MountTargetId:  aws.String("fake-mt-id-2"),
						LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
					}},
				}, nil)
			})
			It("should move to the finish state", func() {
				Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
			})
		})
		Context("when amazon's describe mount target returns an unexpected lifecycle state", func() {
			BeforeEach(func() {
				fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
					MountTargets: []*efs.MountTargetDescription{{
						MountTargetId:  aws.String("fake-mt-id"),
						LifeCycleState: aws.String(efs.LifeCycleStateCreating),
					}},
				}, nil)
			})
			It("should remain in the check mount target state", func() {
				provisionOp.Sleep()
				Expect(provisionOp.State()).To(BeState(provisionOp.CheckMountTarget))
			})
		})
	})

	Context(".OpenPerms", func() {
		JustBeforeEach(func() {
			provisionOp.OpenPerms()
		})
		Context("when amazon's describe mount target return successfully", func() {
			BeforeEach(func() {
			})

			Context("when mount has associated ip", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id"),
							IpAddress:      aws.String("1.2.3.4"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
						}},
					}, nil)

				})
				It("calls open perms", func() {
					Expect(fakeVolTools.OpenPermsCallCount()).To(Equal(1))
				})
				Context("when can open permissions", func() {
					BeforeEach(func() {
						fakeVolTools.OpenPermsReturns(efsvoltools.ErrorResponse{Err: ""})
					})
					It("moves to finish state", func() {
						Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					})
				})
				Context("when can not open permissions", func() {
					BeforeEach(func() {
						fakeVolTools.OpenPermsReturns(efsvoltools.ErrorResponse{Err: "An error occured"})
					})
					It("moves to finish state", func() {
						Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					})
				})
			})

			Context("when mount has no ip", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
						}},
					}, nil)

				})
			})
		})
	})

	//Context("when recreating", func() {
	//
	//	It("should be able to bind to previously created service", func() {
	//		filecontents, err := json.Marshal(dynamicState{
	//			InstanceMap: map[string]brokerapi.ProvisionDetails{
	//				"service-name": {
	//					ServiceID:    s"
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

	//Context("when creating first time", func() {
	//	BeforeEach(func() {
	//		//broker = efsbroker.New(
	//		//	logger,
	//		//	"service-name", "service-id", "/fake-dir",
	//		//	fakeOs,
	//		//	fakeIoutil,
	//		//	fakeClock,
	//		//	fakeEFSService,
	//		//	[]string{"fake-subnet-id"},
	//		//	"fake-security-group",
	//		//	fakeVolTools,
	//		//	NewOperation,
	//		//)
	//		provisionOp = efsbroker.NewProvisionOperation(nil, logger, "filesystem-id")
	//
	//		fakeEFSService.CreateFileSystemReturns(&efs.FileSystemDescription{
	//			FileSystemId: aws.String("fake-fs-id"),
	//		}, nil)
	//		fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//			FileSystems: []*efs.FileSystemDescription{{
	//				FileSystemId: aws.String("fake-fs-id"),
	//			}},
	//		}, nil)
	//		fakeEFSService.CreateMountTargetReturns(&efs.MountTargetDescription{
	//			MountTargetId: aws.String("fake-mt-id"),
	//		}, nil)
	//		fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//			MountTargets: []*efs.MountTargetDescription{{
	//				MountTargetId:  aws.String("fake-mt-id"),
	//				LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
	//				IpAddress:      aws.String("1.1.1.1"),
	//			}},
	//		}, nil)
	//	})
	//
	//	Context(".Provision", func() {
	//		var (
	//			instanceID       string
	//			provisionDetails brokerapi.ProvisionDetails
	//			asyncAllowed     bool
	//
	//			err error
	//		)
	//
	//		BeforeEach(func() {
	//			WriteFileCallCount = 0
	//			WriteFileWrote = ""
	//
	//			instanceID = "some-instance-id"
	//			provisionDetails = brokerapi.ProvisionDetails{PlanID: "generalPurpose"}
	//			asyncAllowed = true
	//		})
	//
	//		JustBeforeEach(func() {
	//			provisionOp.Execute()
	//		})
	//
	//		//It("should not error", func() {
	//		//	Expect(err).NotTo(HaveOccurred())
	//		//})
	//
	//		//It("should provision the service instance asynchronously", func() {
	//		//	Expect(spec.IsAsync).To(Equal(true))
	//		//})
	//
	//		It("creates new file system in efs", func() {
	//			Expect(fakeEFSService.CreateFileSystemCallCount()).To(Equal(1))
	//			input := fakeEFSService.CreateFileSystemArgsForCall(0)
	//			Expect(*input.PerformanceMode).To(Equal("generalPurpose"))
	//			Expect(*input.CreationToken).To(Equal("some-instance-id"))
	//		})
	//
	//		It("eventually creates a mount target in efs", func() {
	//			Eventually(fakeEFSService.CreateMountTargetCallCount).Should(Equal(1))
	//			input := fakeEFSService.CreateMountTargetArgsForCall(0)
	//			Expect(*input.FileSystemId).To(Equal("fake-fs-id"))
	//			Expect(*input.SubnetId).To(Equal("fake-subnet-id"))
	//		})
	//
	//		It("should write state", func() {
	//			Expect(WriteFileCallCount).To(Equal(1))
	//			Expect(WriteFileWrote).To(Equal(`{"InstanceMap":{"some-instance-id":{"service_id":"","plan_id":"generalPurpose","organization_guid":"","space_guid":"","EfsId":"fake-fs-id"}},"BindingMap":{}}`))
	//		})
	//
	//		Context("with maxIO", func() {
	//			BeforeEach(func() {
	//				provisionDetails = brokerapi.ProvisionDetails{PlanID: "maxIO"}
	//			})
	//
	//			It("should provision the service instance with maxIO", func() {
	//				input := fakeEFSService.CreateFileSystemArgsForCall(0)
	//				Expect(*input.PerformanceMode).To(Equal("maxIO"))
	//			})
	//		})
	//
	//		Context("when creating the efs errors", func() {
	//			BeforeEach(func() {
	//				fakeEFSService.CreateFileSystemReturns(nil, errors.New("bad create fs"))
	//			})
	//
	//			It("errors", func() {
	//				Expect(err).To(HaveOccurred())
	//			})
	//		})
	//
	//		Context("when creating the mounts errors", func() {
	//			BeforeEach(func() {
	//				// callIndex := 0
	//				// fakeEFSService.CreateMountTargetStub = func(*efs.CreateMountTargetInput) (*efs.MountTargetDescription, error) {
	//				// 	callIndex++
	//				// 	if callIndex == 1 {
	//				// 		return nil, errors.New("badness")
	//				// 	}
	//				// 	return &efs.MountTargetDescription{
	//				// 		MountTargetId: aws.String("fake-mt-id"),
	//				// 	}, nil
	//				fakeEFSService.CreateMountTargetReturns(nil, errors.New("badness"))
	//			})
	//
	//			It("does not error", func() {
	//				Expect(err).NotTo(HaveOccurred())
	//			})
	//		})
	//
	//		Context("when the client doesnt support async", func() {
	//			BeforeEach(func() {
	//				asyncAllowed = false
	//			})
	//
	//			It("errors", func() {
	//				Expect(err).To(Equal(brokerapi.ErrAsyncRequired))
	//			})
	//		})
	//
	//		//	Context("when the service instance already exists with different details", func() {
	//		//		// enclosing context creates initial instance
	//		//		JustBeforeEach(func() {
	//		//			provisionDetails.ServiceID = "different-service-id"
	//		//			_, err = broker.Provision("some-instance-id", provisionDetails, true)
	//		//		})
	//		//
	//		//		It("should error", func() {
	//		//			Expect(err).To(Equal(brokerapi.ErrInstanceAlreadyExists))
	//		//		})
	//		//	})
	//	})
	//
	//	//		Context(".Deprovision", func() {
	//	//			var (
	//	//				instanceID       string
	//	//				asyncAllowed     bool
	//	//				provisionDetails brokerapi.ProvisionDetails
	//	//
	//	//				err error
	//	//			)
	//	//
	//	//			BeforeEach(func() {
	//	//				WriteFileCallCount = 0
	//	//				WriteFileWrote = ""
	//	//				instanceID = "some-instance-id"
	//	//				provisionDetails = brokerapi.ProvisionDetails{PlanID: "generalPurpose"}
	//	//				asyncAllowed = true
	//	//			})
	//	//
	//	//			BeforeEach(func() {
	//	//				fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//	//					FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateAvailable)}},
	//	//				}, nil)
	//	//
	//	//				_, err = broker.Provision(instanceID, provisionDetails, asyncAllowed)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				// Wait for provisioning to finish
	//	//				Eventually(func() int {
	//	//					fakeClock.Increment(time.Second * 10)
	//	//					return fakeVolTools.OpenPermsCallCount()
	//	//				}, time.Second*1, time.Millisecond*100).Should(Equal(1))
	//	//			})
	//	//
	//	//			JustBeforeEach(func() {
	//	//
	//	//			})
	//	//
	//	//			Context("when deprovision is working", func() {
	//	//				JustBeforeEach(func() {
	//	//					WriteFileCallCount = 0
	//	//					WriteFileWrote = ""
	//	//
	//	//					count := 0
	//	//					fakeEFSService.DescribeMountTargetsStub = func(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
	//	//						logger.Info("fake-mount-target-info", lager.Data{"count": count})
	//	//						if count == 0 {
	//	//							count++
	//	//							return &efs.DescribeMountTargetsOutput{
	//	//								MountTargets: []*efs.MountTargetDescription{{
	//	//									MountTargetId:  aws.String("fake-mt-id"),
	//	//									LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
	//	//									IpAddress:      aws.String("1.1.1.1"),
	//	//								}},
	//	//							}, nil
	//	//						} else if count == 1 {
	//	//							count++
	//	//							return &efs.DescribeMountTargetsOutput{
	//	//								MountTargets: []*efs.MountTargetDescription{{
	//	//									MountTargetId:  aws.String("fake-mt-id"),
	//	//									LifeCycleState: aws.String(efs.LifeCycleStateDeleting),
	//	//									IpAddress:      aws.String("1.1.1.1"),
	//	//								}},
	//	//							}, nil
	//	//						} else {
	//	//							count++
	//	//							return &efs.DescribeMountTargetsOutput{
	//	//								MountTargets: []*efs.MountTargetDescription{{
	//	//									MountTargetId:  aws.String("fake-mt-id"),
	//	//									LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
	//	//								}},
	//	//							}, nil
	//	//						}
	//	//					}
	//	//
	//	//					fakeEFSService.DescribeFileSystemsReturns(nil, errors.New("blah blah blah does not exist."))
	//	//
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//
	//	//					fakeClock.WaitForWatcherAndIncrement(2 * efsbroker.PollingInterval)
	//	//
	//	//					Eventually(func() brokerapi.LastOperationState {
	//	//						retval, _ := broker.LastOperation(instanceID, "deprovision")
	//	//						return retval.State
	//	//					}, time.Second*1, time.Millisecond*100).Should(Equal(brokerapi.Succeeded))
	//	//				})
	//	//
	//	//				It("should deprovision the service", func() {
	//	//					Expect(err).NotTo(HaveOccurred())
	//	//
	//	//					By("checking that we can reprovision a slightly different service")
	//	//					_, err = broker.Provision(instanceID, brokerapi.ProvisionDetails{ServiceID: "different-service-id"}, true)
	//	//					Expect(err).NotTo(Equal(brokerapi.ErrInstanceAlreadyExists))
	//	//				})
	//	//
	//	//				It("should delete the efs", func() {
	//	//					Expect(fakeEFSService.DeleteFileSystemCallCount()).To(Equal(1))
	//	//					Expect(*fakeEFSService.DeleteFileSystemArgsForCall(0).FileSystemId).To(Equal("fake-fs-id"))
	//	//				})
	//	//
	//	//				It("should write state", func() {
	//	//					Expect(WriteFileCallCount).To(Equal(1))
	//	//					Expect(WriteFileWrote).To(Equal("{\"InstanceMap\":{},\"BindingMap\":{}}"))
	//	//				})
	//	//
	//	//				It("should delete the mount targets", func() {
	//	//					Eventually(fakeEFSService.DeleteMountTargetCallCount, time.Second, time.Millisecond*10).Should(Equal(1))
	//	//					Expect(*fakeEFSService.DeleteMountTargetArgsForCall(0).MountTargetId).To(Equal("fake-mt-id"))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when the instance is not available", func() {
	//	//				BeforeEach(func() {
	//	//					//fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//					//	MountTargets: []*efs.MountTargetDescription{{
	//	//					//		MountTargetId:  aws.String("fake-mt-id"),
	//	//					//		LifeCycleState: aws.String(efs.LifeCycleStateCreating),
	//	//					//	}},
	//	//					//}, nil)
	//	//				})
	//	//
	//	//				JustBeforeEach(func() {
	//	//					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//						MountTargets: []*efs.MountTargetDescription{{
	//	//							MountTargetId:  aws.String("fake-mt-id"),
	//	//							LifeCycleState: aws.String(efs.LifeCycleStateCreating),
	//	//						}},
	//	//					}, nil)
	//	//
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("should fail", func() {
	//	//					Eventually(func() brokerapi.LastOperationState {
	//	//						retval, _ := broker.LastOperation(instanceID, "deprovision")
	//	//						return retval.State
	//	//					}, time.Second*1, time.Millisecond*100).Should(Equal(brokerapi.Failed))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when describe mount targets fails", func() {
	//	//				JustBeforeEach(func() {
	//	//					fakeEFSService.DescribeMountTargetsReturns(nil, errors.New("badness"))
	//	//
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("should fail", func() {
	//	//					Eventually(func() brokerapi.LastOperationState {
	//	//						retval, _ := broker.LastOperation(instanceID, "deprovision")
	//	//						return retval.State
	//	//					}, time.Second*1, time.Millisecond*100).Should(Equal(brokerapi.Failed))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when describe mount targets returns no mounts", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//						MountTargets: nil,
	//	//					}, nil)
	//	//
	//	//					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//	//						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateDeleted)}},
	//	//					}, nil)
	//	//				})
	//	//
	//	//				JustBeforeEach(func() {
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("should succeed", func() {
	//	//					Eventually(func() brokerapi.LastOperationState {
	//	//						retval, _ := broker.LastOperation(instanceID, "deprovision")
	//	//						return retval.State
	//	//					}, time.Second*1, time.Millisecond*100).Should(Equal(brokerapi.Succeeded))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when the service instance does not exist", func() {
	//	//				BeforeEach(func() {
	//	//					instanceID = "nonexistent"
	//	//				})
	//	//
	//	//				JustBeforeEach(func() {
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("errors", func() {
	//	//					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when we immediately fail to delete the file system", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//						MountTargets: []*efs.MountTargetDescription{{
	//	//							MountTargetId:  aws.String("fake-mt-id"),
	//	//							LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
	//	//						}},
	//	//					}, nil)
	//	//
	//	//					fakeEFSService.DeleteFileSystemReturns(nil, errors.New("generic aws error"))
	//	//				})
	//	//
	//	//				JustBeforeEach(func() {
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("should fail", func() {
	//	//					Eventually(func() brokerapi.LastOperationState {
	//	//						retval, _ := broker.LastOperation(instanceID, "deprovision")
	//	//						return retval.State
	//	//					}, time.Second*1, time.Millisecond*100).Should(Equal(brokerapi.Failed))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when we eventually fail to delete the file system", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//						MountTargets: []*efs.MountTargetDescription{{
	//	//							MountTargetId:  aws.String("fake-mt-id"),
	//	//							LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
	//	//						}},
	//	//					}, nil)
	//	//
	//	//					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//	//						FileSystems: []*efs.FileSystemDescription{},
	//	//					}, errors.New("some error"))
	//	//				})
	//	//
	//	//				JustBeforeEach(func() {
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("should fail", func() {
	//	//					Eventually(func() brokerapi.LastOperationState {
	//	//						retval, _ := broker.LastOperation(instanceID, "deprovision")
	//	//						return retval.State
	//	//					}, time.Second*1, time.Millisecond*100).Should(Equal(brokerapi.Failed))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when we fail to delete mount target", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DeleteMountTargetReturns(nil, errors.New("generic aws error"))
	//	//				})
	//	//
	//	//				JustBeforeEach(func() {
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("should not error yet", func() {
	//	//					Expect(err).NotTo(HaveOccurred())
	//	//				})
	//	//			})
	//	//
	//	//			Context("when the client doesnt support async", func() {
	//	//				BeforeEach(func() {
	//	//					asyncAllowed = false
	//	//				})
	//	//
	//	//				JustBeforeEach(func() {
	//	//					_, err = broker.Deprovision(instanceID, brokerapi.DeprovisionDetails{}, asyncAllowed)
	//	//				})
	//	//
	//	//				It("should not error", func() {
	//	//					Expect(err).NotTo(HaveOccurred())
	//	//				})
	//	//			})
	//	//		})
	//	//
	//	//		Context(".LastOperation", func() {
	//	//			var (
	//	//				instanceID string
	//	//
	//	//				op  brokerapi.LastOperation
	//	//				err error
	//	//			)
	//	//
	//	//			BeforeEach(func() {
	//	//				instanceID = "some-instance-id"
	//	//			})
	//	//
	//	//			JustBeforeEach(func() {
	//	//				_, err = broker.Provision(instanceID, brokerapi.ProvisionDetails{}, true)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				op, err = broker.LastOperation(instanceID, "provision")
	//	//			})
	//	//
	//	//			Context("while aws reports the fs is creating", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//	//						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateCreating)}},
	//	//					}, nil)
	//	//				})
	//	//
	//	//				It("returns in progress", func() {
	//	//					Expect(err).NotTo(HaveOccurred())
	//	//					Expect(op.State).To(Equal(brokerapi.InProgress))
	//	//				})
	//	//			})
	//	//
	//	//			Context("while aws reports the fs is available", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//	//						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateAvailable)}},
	//	//					}, nil)
	//	//				})
	//	//
	//	//				Context("but aws reports that there are no mount targets", func() {
	//	//					BeforeEach(func() {
	//	//						fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//							MountTargets: nil,
	//	//						}, nil)
	//	//					})
	//	//
	//	//					It("returns in progress", func() {
	//	//						Expect(err).NotTo(HaveOccurred())
	//	//						Expect(op.State).To(Equal(brokerapi.InProgress))
	//	//					})
	//	//
	//	//				})
	//	//
	//	//				Context("but aws reports the mount target is still creating", func() {
	//	//					BeforeEach(func() {
	//	//						fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//							MountTargets: []*efs.MountTargetDescription{{
	//	//								LifeCycleState: aws.String(efs.LifeCycleStateCreating),
	//	//							}},
	//	//						}, nil)
	//	//					})
	//	//
	//	//					It("returns in progress", func() {
	//	//						Expect(err).NotTo(HaveOccurred())
	//	//						Expect(op.State).To(Equal(brokerapi.InProgress))
	//	//					})
	//	//				})
	//	//
	//	//				Context("and aws reports the mount target is available", func() {
	//	//					BeforeEach(func() {
	//	//						fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
	//	//							MountTargets: []*efs.MountTargetDescription{{
	//	//								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
	//	//								IpAddress:      aws.String("1.1.1.1"),
	//	//							}},
	//	//						}, nil)
	//	//					})
	//	//
	//	//					It("returns successful", func() {
	//	//						Expect(err).NotTo(HaveOccurred())
	//	//						Expect(op.State).To(Equal(brokerapi.Succeeded))
	//	//					})
	//	//				})
	//	//
	//	//				Context("when describing mount targets fails ", func() {
	//	//					BeforeEach(func() {
	//	//						fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{}, errors.New("badness"))
	//	//					})
	//	//
	//	//					It("errors", func() {
	//	//						Expect(err).To(Equal(errors.New("badness")))
	//	//					})
	//	//				})
	//	//			})
	//	//
	//	//			Context("while aws reports a nil lifecycle state", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//	//						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: nil}},
	//	//					}, nil)
	//	//				})
	//	//
	//	//				It("fails", func() {
	//	//					Expect(err).To(HaveOccurred())
	//	//				})
	//	//			})
	//	//
	//	//			Context("when calling out to aws fails ", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{}, errors.New("badness"))
	//	//				})
	//	//
	//	//				It("errors", func() {
	//	//					Expect(err).To(Equal(errors.New("badness")))
	//	//				})
	//	//			})
	//	//
	//	//			Context("when calling out to aws returns too many file systems", func() {
	//	//				BeforeEach(func() {
	//	//					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
	//	//						FileSystems: []*efs.FileSystemDescription{
	//	//							{LifeCycleState: aws.String(efs.LifeCycleStateAvailable)},
	//	//							{LifeCycleState: aws.String(efs.LifeCycleStateAvailable)},
	//	//						},
	//	//					}, nil)
	//	//				})
	//	//
	//	//				It("errors", func() {
	//	//					Expect(err).To(HaveOccurred())
	//	//				})
	//	//			})
	//	//
	//	//			Context("when the instance doesn't exist", func() {
	//	//				It("errors", func() {
	//	//					op, err = broker.LastOperation("non-existant", "provision")
	//	//					Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
	//	//				})
	//	//			})
	//	//		})
	//	//
	//	//		Context(".Bind", func() {
	//	//			var bindDetails brokerapi.BindDetails
	//	//
	//	//			BeforeEach(func() {
	//	//				_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				bindDetails = brokerapi.BindDetails{AppGUID: "guid", Parameters: map[string]interface{}{}}
	//	//			})
	//	//
	//	//			It("includes empty credentials to prevent CAPI crash", func() {
	//	//				binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				Expect(binding.Credentials).NotTo(BeNil())
	//	//			})
	//	//
	//	//			It("uses the instance id in the default container path", func() {
	//	//				binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/data/some-instance-id"))
	//	//			})
	//	//
	//	//			It("flows container path through", func() {
	//	//				bindDetails.Parameters["mount"] = "/var/vcap/otherdir/something"
	//	//				binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//				Expect(binding.VolumeMounts[0].ContainerDir).To(Equal("/var/vcap/otherdir/something"))
	//	//			})
	//	//
	//	//			It("uses rw as its default mode", func() {
	//	//				binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//				Expect(binding.VolumeMounts[0].Mode).To(Equal("rw"))
	//	//			})
	//	//
	//	//			It("sets mode to `r` when readonly is true", func() {
	//	//				bindDetails.Parameters["readonly"] = true
	//	//				binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				Expect(binding.VolumeMounts[0].Mode).To(Equal("r"))
	//	//			})
	//	//
	//	//			It("should write state", func() {
	//	//				WriteFileCallCount = 0
	//	//				WriteFileWrote = ""
	//	//				_, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				Expect(WriteFileCallCount).To(Equal(1))
	//	//				Expect(WriteFileWrote).To(Equal(`{"InstanceMap":{"some-instance-id":{"service_id":"","plan_id":"","organization_guid":"","space_guid":"","EfsId":"fake-fs-id"}},"BindingMap":{"binding-id":{"app_guid":"guid","plan_id":"","service_id":""}}}`))
	//	//			})
	//	//
	//	//			It("errors if mode is not a boolean", func() {
	//	//				bindDetails.Parameters["readonly"] = ""
	//	//				_, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).To(Equal(brokerapi.ErrRawParamsInvalid))
	//	//			})
	//	//
	//	//			It("fills in the driver name", func() {
	//	//				binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				Expect(binding.VolumeMounts[0].Driver).To(Equal("efsdriver"))
	//	//			})
	//	//
	//	//			It("fills in the group id", func() {
	//	//				binding, err := broker.Bind("some-instance-id", "binding-id", bindDetails)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				Expect(binding.VolumeMounts[0].Device.VolumeId).To(Equal("some-instance-id"))
	//	//			})
	//	//
	//	//			Context("when the binding already exists", func() {
	//	//				BeforeEach(func() {
	//	//					_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
	//	//					Expect(err).NotTo(HaveOccurred())
	//	//				})
	//	//
	//	//				It("doesn't error when binding the same details", func() {
	//	//					_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
	//	//					Expect(err).NotTo(HaveOccurred())
	//	//				})
	//	//
	//	//				It("errors when binding different details", func() {
	//	//					_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "different"})
	//	//					Expect(err).To(Equal(brokerapi.ErrBindingAlreadyExists))
	//	//				})
	//	//			})
	//	//
	//	//			It("errors when the service instance does not exist", func() {
	//	//				_, err := broker.Bind("nonexistent-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
	//	//				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
	//	//			})
	//	//
	//	//			It("errors when the app guid is not provided", func() {
	//	//				_, err := broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{})
	//	//				Expect(err).To(Equal(brokerapi.ErrAppGuidNotProvided))
	//	//			})
	//	//		})
	//	//
	//	//		Context(".Unbind", func() {
	//	//			BeforeEach(func() {
	//	//				_, err := broker.Provision("some-instance-id", brokerapi.ProvisionDetails{}, true)
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				_, err = broker.Bind("some-instance-id", "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//			})
	//	//
	//	//			It("unbinds a bound service instance from an app", func() {
	//	//				err := broker.Unbind("some-instance-id", "binding-id", brokerapi.UnbindDetails{})
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//			})
	//	//
	//	//			It("fails when trying to unbind a instance that has not been provisioned", func() {
	//	//				err := broker.Unbind("some-other-instance-id", "binding-id", brokerapi.UnbindDetails{})
	//	//				Expect(err).To(Equal(brokerapi.ErrInstanceDoesNotExist))
	//	//			})
	//	//
	//	//			It("fails when trying to unbind a binding that has not been bound", func() {
	//	//				err := broker.Unbind("some-instance-id", "some-other-binding-id", brokerapi.UnbindDetails{})
	//	//				Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
	//	//			})
	//	//			It("should write state", func() {
	//	//				WriteFileCallCount = 0
	//	//				WriteFileWrote = ""
	//	//				err := broker.Unbind("some-instance-id", "binding-id", brokerapi.UnbindDetails{})
	//	//				Expect(err).NotTo(HaveOccurred())
	//	//
	//	//				Expect(WriteFileCallCount).To(Equal(1))
	//	//				Expect(WriteFileWrote).To(Equal(`{"InstanceMap":{"some-instance-id":{"service_id":"","plan_id":"","organization_guid":"","space_guid":"","EfsId":"fake-fs-id"}},"BindingMap":{}}`))
	//	//			})
	//	//
	//	//		})
	//	//
	//	//		//Context("when multiple operations happen in parallel", func() {
	//	//		//	It("maintains consistency", func() {
	//	//		//		var wg sync.WaitGroup
	//	//		//
	//	//		//		wg.Add(2)
	//	//		//
	//	//		//		smash := func(uniqueName string) {
	//	//		//			defer GinkgoRecover()
	//	//		//			defer wg.Done()
	//	//		//
	//	//		//			broker.Services()
	//	//		//
	//	//		//			_, err := broker.Provision(uniqueName, brokerapi.ProvisionDetails{}, true)
	//	//		//			Expect(err).NotTo(HaveOccurred())
	//	//		//
	//	//		//			_, err = broker.Bind(uniqueName, "binding-id", brokerapi.BindDetails{AppGUID: "guid"})
	//	//		//			Expect(err).NotTo(HaveOccurred())
	//	//		//
	//	//		//			err = broker.Unbind(uniqueName, "some-other-binding-id", brokerapi.UnbindDetails{})
	//	//		//			Expect(err).To(Equal(brokerapi.ErrBindingDoesNotExist))
	//	//		//
	//	//		//			_, err = broker.Deprovision(uniqueName, brokerapi.DeprovisionDetails{}, true)
	//	//		//			Expect(err).NotTo(HaveOccurred())
	//	//		//		}
	//	//		//
	//	//		//		// Note go race detection should kick in if access is unsynchronized
	//	//		//		go smash("some-instance-1")
	//	//		//		go smash("some-instance-2")
	//	//		//
	//	//		//		wg.Wait()
	//	//		//	})
	//	//		//})
	//})
})
