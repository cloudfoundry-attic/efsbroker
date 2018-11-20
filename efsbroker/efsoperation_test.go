package efsbroker_test

import (
	"errors"
	"fmt"
	"os"

	"code.cloudfoundry.org/efsbroker/efsbroker"
	"code.cloudfoundry.org/efsbroker/efsbroker/efsfakes"
	"code.cloudfoundry.org/efsdriver/efsdriverfakes"
	"code.cloudfoundry.org/efsdriver/efsvoltools"
	"code.cloudfoundry.org/goshims/ioutilshim/ioutil_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/efs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/pivotal-cf/brokerapi"
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
		logger         lager.Logger
		fakeEFSService *efsfakes.FakeEFSService
		fakeVolTools   *efsdriverfakes.FakeVolTools
		fakeClock      *efsfakes.FakeClock
	)

	BeforeEach(func() {
		logger = lagertest.NewTestLogger("test-broker")
		fakeEFSService = &efsfakes.FakeEFSService{}
		fakeClock = &efsfakes.FakeClock{}
	})

	Context("ProvisionOperation", func() {

		var (
			err                error
			filesystemID       *string
			provisionOp        *efsbroker.ProvisionOperationStateMachine
			fakeIoutil         *ioutil_fake.FakeIoutil
			WriteFileCallCount int
			operationState     *efsbroker.OperationState
		)

		var update = func(opstate *efsbroker.OperationState) {
			operationState = opstate
		}

		BeforeEach(func() {
			fakeIoutil = &ioutil_fake.FakeIoutil{}
			fakeVolTools = &efsdriverfakes.FakeVolTools{}
			fakeIoutil.WriteFileStub = func(filename string, data []byte, perm os.FileMode) error {
				WriteFileCallCount++
				return nil
			}
			provisionOp = efsbroker.NewProvisionStateMachine(
				logger,
				"instanceID",
				brokerapi.ProvisionDetails{PlanID: "planId"},
				fakeEFSService,
				fakeVolTools,
				[]efsbroker.Subnet{{"fake-subnet-id", "fake-az", "fake-security-group"}},
				fakeClock,
				update)
			filesystemID = aws.String("fake-fs-id")
		})

		Context(".CreateFs", func() {
			JustBeforeEach(func() {
				err = provisionOp.CreateFs()
			})
			Context("when amazon's create file system returns ok", func() {
				BeforeEach(func() {
					fakeEFSService.CreateFileSystemReturns(&efs.FileSystemDescription{
						FileSystemId: filesystemID,
					}, nil)
				})
				It("should succeed and update fsid state", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.FsID).To(ContainSubstring(*filesystemID))
				})
			})
			Context("when amazon's create file system returns error", func() {
				BeforeEach(func() {
					fakeEFSService.CreateFileSystemReturns(nil, fmt.Errorf("some-error"))
				})

				It("should error", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).To(Equal(efsbroker.NewOperationStateErr("some-error")))
				})
			})
		})

		Context(".CheckFS", func() {
			JustBeforeEach(func() {
				err = provisionOp.CheckFs()
			})
			Context("when amazon's describe file system returns creating", func() {
				BeforeEach(func() {
					count := 0
					fakeEFSService.DescribeFileSystemsStub = func(*efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error) {
						if count < 2 {
							count++
							return &efs.DescribeFileSystemsOutput{
								FileSystems: []*efs.FileSystemDescription{{
									FileSystemId:   filesystemID,
									LifeCycleState: aws.String(efs.LifeCycleStateCreating),
								}},
							}, nil
						}
						return &efs.DescribeFileSystemsOutput{
							FileSystems: []*efs.FileSystemDescription{{
								FileSystemId:   filesystemID,
								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							}},
						}, nil
					}
				})
				It("should sleep and try again", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.FsState).To(ContainSubstring(efs.LifeCycleStateAvailable))
					Expect(operationState.Err).To(BeNil())
					Expect(fakeEFSService.DescribeFileSystemsCallCount()).To(Equal(3))
				})
				It("should sleep with exponential backoff", func() {
					Expect(fakeClock.SleepCallCount()).To(Equal(2))
					Expect(fakeClock.SleepArgsForCall(1)).To(Equal(fakeClock.SleepArgsForCall(0) * 2))
				})
			})
			Context("when amazon's describe file system returns a throttling exception", func() {
				BeforeEach(func() {
					count := 0
					fakeEFSService.DescribeFileSystemsStub = func(*efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error) {
						if count < 2 {
							count++
							return nil, errors.New("ThrottlingException: Rate exceeded\n\tstatus code: 400, request id: badcab")
						}
						return &efs.DescribeFileSystemsOutput{
							FileSystems: []*efs.FileSystemDescription{{
								FileSystemId:   filesystemID,
								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							}},
						}, nil
					}
				})
				It("should sleep and try again", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.FsState).To(ContainSubstring(efs.LifeCycleStateAvailable))
					Expect(operationState.Err).To(BeNil())
					Expect(fakeEFSService.DescribeFileSystemsCallCount()).To(Equal(3))
				})
				It("should sleep with exponential backoff", func() {
					Expect(fakeClock.SleepCallCount()).To(Equal(2))
					Expect(fakeClock.SleepArgsForCall(1)).To(Equal(fakeClock.SleepArgsForCall(0) * 2))
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
				It("should error and set Err state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.FsState).To(ContainSubstring(efs.LifeCycleStateDeleted))
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe file system returns no file system lifecycle state at all", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{{
							FileSystemId:   filesystemID,
							LifeCycleState: nil,
						}},
					}, nil)
				})
				It("should err and set Err state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe file system returns an error", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(nil, errors.New("badness"))
				})
				It("should error and set Err state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe file system returns no file systems", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{{}},
					}, nil)
				})
				It("should error and set Err state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
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
				It("should error and set Err state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
		})

		Context(".CreateMountTarget", func() {
			JustBeforeEach(func() {
				err = provisionOp.CreateMountTargets()
			})
			Context("when amazon's create mount target return successfully", func() {
				BeforeEach(func() {
					fakeEFSService.CreateMountTargetReturns(&efs.MountTargetDescription{
						MountTargetId:  aws.String("fake-mt-id"),
						LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
					}, nil)
				})
				It("should succeed and set state", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.Err).To(BeNil())
					Expect(operationState.MountTargetIDs).To(ContainElement("fake-mt-id"))
					Expect(operationState.MountTargetAZs).To(ContainElement("fake-az"))
				})
			})
			Context("when amazon's create mount target returns an error", func() {
				BeforeEach(func() {
					fakeEFSService.CreateMountTargetReturns(nil, errors.New("badness"))
				})
				It("should move to the finish state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
		})

		Context(".CheckMountTarget", func() {
			BeforeEach(func() {
				fakeEFSService.CreateMountTargetReturns(&efs.MountTargetDescription{
					MountTargetId:  aws.String("fake-mt-id"),
					LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
				}, nil)
			})
			JustBeforeEach(func() {
				provisionOp.CreateMountTargets()
				err = provisionOp.CheckMountTargets()
			})
			Context("when amazon's describe mount target return successfully", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id"),
							IpAddress:      aws.String("1.2.3.4"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
						}},
					}, nil)
				})
				It("should succeed and set op state", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.Err).To(BeNil())
					Expect(operationState.MountTargetIDs[0]).To(Equal("fake-mt-id"))
					Expect(operationState.MountTargetStates[0]).To(Equal("available"))
					Expect(operationState.MountTargetIps[0]).To(Equal("1.2.3.4"))
					Expect(operationState.MountTargetAZs[0]).To(Equal("fake-az"))
				})
			})
			Context("when amazon's describe mount target returns an error", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(nil, errors.New("badness"))
				})
				It("should error and set err on op state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe mount target returns empty mount targets", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{},
					}, nil)
				})
				It("should error and set err on op state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
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
				It("should error and set err on op state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe mount target returns creating then available", func() {
				BeforeEach(func() {
					count := 0
					fakeEFSService.DescribeMountTargetsStub = func(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
						if count == 0 {
							count++
							return &efs.DescribeMountTargetsOutput{
								MountTargets: []*efs.MountTargetDescription{{
									MountTargetId:  aws.String("fake-mt-id"),
									LifeCycleState: aws.String(efs.LifeCycleStateCreating),
								}},
							}, nil
						}
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []*efs.MountTargetDescription{{
								MountTargetId:  aws.String("fake-mt-id"),
								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							}},
						}, nil
					}
				})
				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.MountTargetStates[0]).To(Equal(efs.LifeCycleStateAvailable))
					Expect(fakeEFSService.DescribeMountTargetsCallCount()).To(Equal(2))
				})
			})
			Context("when amazon's describe mount target returns throttling exception then available", func() {
				BeforeEach(func() {
					count := 0
					fakeEFSService.DescribeMountTargetsStub = func(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
						if count == 0 {
							count++
							return nil, errors.New("ThrottlingException: Rate exceeded\n\tstatus code: 400, request id: 0afb00")
						}
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []*efs.MountTargetDescription{{
								MountTargetId:  aws.String("fake-mt-id"),
								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							}},
						}, nil
					}
				})
				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.MountTargetStates[0]).To(Equal(efs.LifeCycleStateAvailable))
					Expect(fakeEFSService.DescribeMountTargetsCallCount()).To(Equal(2))
				})
			})
			Context("when amazon's describe mount target returns an unexpected state", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id"),
							LifeCycleState: aws.String(efs.LifeCycleStateDeleting),
						}},
					}, nil)
				})
				It("should error and set err on op state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when there are multiple subnets", func() {
				BeforeEach(func() {
					provisionOp = efsbroker.NewProvisionStateMachine(
						logger,
						"instanceID",
						brokerapi.ProvisionDetails{PlanID: "planId"},
						fakeEFSService,
						fakeVolTools,
						[]efsbroker.Subnet{
							{"fake-subnet-id", "fake-az", "fake-security-group"},
							{"fake-subnet-id-2", "fake-az-2", "fake-security-group-2"},
						},
						fakeClock,
						update)
					filesystemID = aws.String("fake-fs-id")

					count := 0
					fakeEFSService.CreateMountTargetStub = func(*efs.CreateMountTargetInput) (*efs.MountTargetDescription, error) {
						if count == 0 {
							count++
							return &efs.MountTargetDescription{
								MountTargetId:  aws.String("fake-mt-id-1"),
								LifeCycleState: aws.String(efs.LifeCycleStateCreating),
							}, nil
						}
						return &efs.MountTargetDescription{
							MountTargetId:  aws.String("fake-mt-id-2"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
						}, nil
					}

				})
				Context("when amazon's describe mount target returns one mount target", func() {
					BeforeEach(func() {
						fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
							MountTargets: []*efs.MountTargetDescription{{
								MountTargetId:  aws.String("fake-mt-id-1"),
								IpAddress:      aws.String("1.2.3.4"),
								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							}},
						}, nil)
					})
					It("should error and set err on op state", func() {
						Expect(err).To(HaveOccurred())
						Expect(operationState.Err).NotTo(BeNil())
					})
				})
				Context("when there are many mount targets, returned out of order", func() {
					BeforeEach(func() {

						fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
							MountTargets: []*efs.MountTargetDescription{{
								MountTargetId:  aws.String("fake-mt-id-2"),
								IpAddress:      aws.String("1.2.3.5"),
								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							}, {
								MountTargetId:  aws.String("fake-mt-id-1"),
								IpAddress:      aws.String("1.2.3.4"),
								LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							}},
						}, nil)
					})
					It("should succeed and set op state in original order with correct AZs", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(operationState.Err).To(BeNil())
						Expect(operationState.MountTargetIDs[0]).To(Equal("fake-mt-id-1"))
						Expect(operationState.MountTargetStates[0]).To(Equal("available"))
						Expect(operationState.MountTargetIps[0]).To(Equal("1.2.3.4"))
						Expect(operationState.MountTargetAZs[0]).To(Equal("fake-az"))
						Expect(operationState.MountTargetIDs[1]).To(Equal("fake-mt-id-2"))
						Expect(operationState.MountTargetStates[1]).To(Equal("available"))
						Expect(operationState.MountTargetIps[1]).To(Equal("1.2.3.5"))
						Expect(operationState.MountTargetAZs[1]).To(Equal("fake-az-2"))
					})
				})
				Context("when amazon's describe mount target returns creating then available", func() {
					BeforeEach(func() {
						count := 0
						fakeEFSService.DescribeMountTargetsStub = func(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
							if count == 0 {
								count++
								return &efs.DescribeMountTargetsOutput{
									MountTargets: []*efs.MountTargetDescription{
										{
											MountTargetId:  aws.String("fake-mt-id-1"),
											LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
										},
										{
											MountTargetId:  aws.String("fake-mt-id-2"),
											LifeCycleState: aws.String(efs.LifeCycleStateCreating),
										},
									},
								}, nil
							}
							return &efs.DescribeMountTargetsOutput{
								MountTargets: []*efs.MountTargetDescription{
									{
										MountTargetId:  aws.String("fake-mt-id-1"),
										LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
									},
									{
										MountTargetId:  aws.String("fake-mt-id-2"),
										LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
									},
								},
							}, nil
						}
					})
					It("should succeed", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(operationState.MountTargetStates[1]).To(Equal(efs.LifeCycleStateAvailable))
						Expect(fakeEFSService.DescribeMountTargetsCallCount()).To(Equal(2))
					})
				})

			})
		})

		Context(".OpenPerms", func() {
			JustBeforeEach(func() {
				err = provisionOp.OpenPerms()
			})
			Context("when open permissions succeeds", func() {
				BeforeEach(func() {
					fakeVolTools.OpenPermsReturns(efsvoltools.ErrorResponse{Err: ""})
				})
				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(operationState.Err).To(BeNil())
				})
			})
			Context("when open permissions fails", func() {
				BeforeEach(func() {
					fakeVolTools.OpenPermsReturns(efsvoltools.ErrorResponse{Err: "An error occured"})
				})
				It("fails and sets err on op state", func() {
					Expect(err).To(HaveOccurred())
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
		})
	})

	Context("DeprovisionOperation", func() {
		var (
			deprovisionOp *efsbroker.DeprovisionOperation
			spec          efsbroker.DeprovisionOperationSpec

			instanceID string
			fsId       string
			mountId    string

			err error
		)

		BeforeEach(func() {
			instanceID = "some-instance-id"
			fsId = "fake-fs-id"
			mountId = "fake-mount-id"

			spec = efsbroker.DeprovisionOperationSpec{
				InstanceID:     instanceID,
				FsID:           fsId,
				MountTargetIDs: []string{mountId},
			}
			deprovisionOp = efsbroker.NewTestDeprovisionOperation(logger, fakeEFSService, fakeClock, spec, nil)
		})

		Context("#DeleteMountTarget", func() {

			JustBeforeEach(func() {
				err = deprovisionOp.DeleteMountTarget(fsId)
			})

			Context("when describe mount targets returns a single available mount target", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							IpAddress:      aws.String("1.1.1.1"),
						}},
					}, nil)
				})
				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
				})

				Context("when delete mount target returns returns error", func() {
					BeforeEach(func() {
						fakeEFSService.DeleteMountTargetReturns(nil, errors.New("badness"))
					})
					It("should error", func() {
						Expect(err).To(HaveOccurred())
					})
				})

				Context("when delete mount target succeeds", func() {
					BeforeEach(func() {
						fakeEFSService.DeleteMountTargetReturns(nil, nil)
					})
					It("should succeed", func() {
						Expect(err).NotTo(HaveOccurred())
					})
				})
			})

			Context("when describe mount targets returns nil mount targets", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: nil,
					}, nil)
				})
				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("when describe mount targets returns empty mount targets", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{},
					}, nil)
				})
				It("should succeed", func() {
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("when describe mount targets returns many mount targets", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id1"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							IpAddress:      aws.String("1.1.1.1"),
						}, {
							MountTargetId:  aws.String("fake-mt-id2"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
							IpAddress:      aws.String("1.1.1.2"),
						}},
					}, nil)
				})
				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
				Context("when there are also many subnets", func() {
					BeforeEach(func() {
						spec = efsbroker.DeprovisionOperationSpec{
							InstanceID:     instanceID,
							FsID:           fsId,
							MountTargetIDs: []string{"fake-mount-1", "fake-mount-2"},
						}
						deprovisionOp = efsbroker.NewTestDeprovisionOperation(logger, fakeEFSService, fakeClock, spec, nil)
					})
					It("should succeed", func() {
						Expect(err).NotTo(HaveOccurred())
						Expect(fakeEFSService.DeleteMountTargetCallCount()).To(Equal(2))
					})
					Context("when some of the targets are unavailable", func() {
						BeforeEach(func() {
							fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
								MountTargets: []*efs.MountTargetDescription{{
									MountTargetId:  aws.String("fake-mt-id1"),
									LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
									IpAddress:      aws.String("1.1.1.1"),
								}, {
									MountTargetId:  aws.String("fake-mt-id2"),
									LifeCycleState: aws.String(efs.LifeCycleStateCreating),
									IpAddress:      aws.String("1.1.1.2"),
								}},
							}, nil)
						})
						It("should fail", func() {
							Expect(err).To(HaveOccurred())
						})
					})

				})
			})

			Context("when describe mount targets returns unavailable mount target", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id1"),
							LifeCycleState: aws.String(efs.LifeCycleStateCreating),
							IpAddress:      aws.String("1.1.1.1"),
						}},
					}, nil)
				})
				It("should error", func() {
					Expect(err).To(HaveOccurred())
				})
			})

		})

		Context("#CheckMountTarget", func() {
			JustBeforeEach(func() {
				err = deprovisionOp.CheckMountTarget(fsId)
			})

			Context("when amazon's describe mount target returns empty mount targets", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{},
					}, nil)
				})
				It("should return an error", func() {
					Expect(err).To(BeNil())
				})
			})

			Context("when amazon's describe mount target return successfully", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(&efs.DescribeMountTargetsOutput{
						MountTargets: []*efs.MountTargetDescription{{
							MountTargetId:  aws.String("fake-mt-id"),
							IpAddress:      aws.String("1.2.3.4"),
							LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
						}},
					}, nil)
				})
				It("should succeed", func() {
					Expect(err).To(BeNil())
				})
			})

			Context("when amazon's describe mount target returns an error", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(nil, errors.New("badness"))
				})
				It("should return error", func() {
					Expect(err).NotTo(BeNil())
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
				It("should error", func() {
					Expect(err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe mount target returns a deleting lifecycle state", func() {
				BeforeEach(func() {
					count := 0
					fakeEFSService.DescribeMountTargetsStub = func(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
						if count == 0 {
							count++
							return &efs.DescribeMountTargetsOutput{
								MountTargets: []*efs.MountTargetDescription{{
									MountTargetId:  aws.String("fake-mt-id"),
									LifeCycleState: aws.String(efs.LifeCycleStateDeleting),
								}},
							}, nil
						}
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []*efs.MountTargetDescription{{
								MountTargetId:  aws.String("fake-mt-id"),
								LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
							}},
						}, nil
					}
				})
				It("should remain in the check mount target state", func() {
					Expect(fakeClock.SleepCallCount()).To(Equal(1))
				})
			})
			Context("when amazon's describe mount target returns a throttling exception", func() {
				BeforeEach(func() {
					count := 0
					fakeEFSService.DescribeMountTargetsStub = func(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
						if count == 0 {
							count++
							return nil, errors.New("ThrottlingException: Rate exceeded\n\tstatus code: 400, request id: d00fad00f")
						}
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []*efs.MountTargetDescription{{
								MountTargetId:  aws.String("fake-mt-id"),
								LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
							}},
						}, nil
					}
				})
				It("should remain in the check mount target state", func() {
					Expect(fakeClock.SleepCallCount()).To(Equal(1))
				})
			})
			Context("when there are multiple mount points and not all are deleted", func() {
				BeforeEach(func() {
					spec = efsbroker.DeprovisionOperationSpec{
						InstanceID:     instanceID,
						FsID:           fsId,
						MountTargetIDs: []string{"fake-mount-1", "fake-mount-2"},
					}
					deprovisionOp = efsbroker.NewTestDeprovisionOperation(logger, fakeEFSService, fakeClock, spec, nil)

					count := 0
					fakeEFSService.DescribeMountTargetsStub = func(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
						if count == 0 {
							count++
							return &efs.DescribeMountTargetsOutput{
								MountTargets: []*efs.MountTargetDescription{{
									MountTargetId:  aws.String("fake-mt-id-1"),
									LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
								}, {
									MountTargetId:  aws.String("fake-mt-id-2"),
									LifeCycleState: aws.String(efs.LifeCycleStateDeleting),
								}},
							}, nil
						}
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []*efs.MountTargetDescription{{
								MountTargetId:  aws.String("fake-mt-id-1"),
								LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
							}, {
								MountTargetId:  aws.String("fake-mt-id-2"),
								LifeCycleState: aws.String(efs.LifeCycleStateDeleted),
							}},
						}, nil
					}
				})
				It("should remain in the check mount target state", func() {
					Expect(fakeClock.SleepCallCount()).To(Equal(1))
				})
			})
		})

		Context("#DeleteFs", func() {
			JustBeforeEach(func() {
				err = deprovisionOp.DeleteFs(fsId)
			})

			Context("when amazon's describe file system returns successfully", func() {
				BeforeEach(func() {
				})
				It("should succeed", func() {
					Expect(err).To(BeNil())
				})
			})

			Context("when amazon's describe file system returns error", func() {
				BeforeEach(func() {
					fakeEFSService.DeleteFileSystemReturns(nil, errors.New("badness"))
				})
				It("should error", func() {
					Expect(err).NotTo(BeNil())
				})
			})
		})

		Context("#CheckFs", func() {
			JustBeforeEach(func() {
				err = deprovisionOp.CheckFs(fsId)
			})

			Context("when amazon's describe file system returns 'it does not exist' error", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(nil, errors.New("fake-fs-id does not exist"))
				})
				It("should error", func() {
					Expect(err).To(BeNil())
				})
			})

			Context("when amazon's describe file system returns deleted state", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateDeleted)}},
					}, nil)
				})
				It("should succeed", func() {
					Expect(err).To(BeNil())
				})
			})

			Context("when amazon's describe file system returns a throttling exception", func() {
				BeforeEach(func() {
					count := 0
					fakeEFSService.DescribeFileSystemsStub = func(*efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error) {
						if count < 2 {
							count++
							return nil, errors.New("ThrottlingException: Rate exceeded\n\tstatus code: 400, request id: d00d00")
						}
						return nil, errors.New("fake-fs-id does not exist")
					}
				})
				It("should sleep and try again", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeEFSService.DescribeFileSystemsCallCount()).To(Equal(3))
				})
				It("should sleep with exponential backoff", func() {
					Expect(fakeClock.SleepCallCount()).To(Equal(2))
					Expect(fakeClock.SleepArgsForCall(1)).To(Equal(fakeClock.SleepArgsForCall(0) * 2))
				})
			})

			Context("when amazon's describe file system returns any other error", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(nil, errors.New("badness"))
				})
				It("should error", func() {
					Expect(err).NotTo(BeNil())
				})
			})

			Context("when amazon's describe file system returns no file systems", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{},
					}, nil)
				})
				It("should error", func() {
					Expect(err).NotTo(BeNil())
				})
			})

			Context("when amazon's describe file system returns many file systems", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: aws.String(efs.LifeCycleStateDeleted)}, {LifeCycleState: aws.String(efs.LifeCycleStateDeleted)}},
					}, nil)
				})
				It("should error", func() {
					Expect(err).NotTo(BeNil())
				})
			})

			Context("when amazon's describe file system returns file system with a nil state", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(&efs.DescribeFileSystemsOutput{
						FileSystems: []*efs.FileSystemDescription{{LifeCycleState: nil}},
					}, nil)
				})
				It("should error", func() {
					Expect(err).NotTo(BeNil())
				})
			})
		})
	})
})
