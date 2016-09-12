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
			provisionOp        *efsbroker.ProvisionOperationStateMachine
			fakeOs             *os_fake.FakeOs
			fakeIoutil         *ioutil_fake.FakeIoutil
			WriteFileCallCount int
			WriteFileWrote     string
			filesystemID       *string
			operationState     *efsbroker.OperationState
		)

		var update = func(opstate *efsbroker.OperationState) {
			operationState = opstate
		}

		BeforeEach(func() {
			fakeOs = &os_fake.FakeOs{}
			fakeIoutil = &ioutil_fake.FakeIoutil{}
			fakeVolTools = &efsdriverfakes.FakeVolTools{}
			fakeIoutil.WriteFileStub = func(filename string, data []byte, perm os.FileMode) error {
				WriteFileCallCount++
				WriteFileWrote = string(data)
				return nil
			}
			provisionOp = efsbroker.NewProvisionStateMachine(logger, "instanceID", "planId", fakeEFSService, fakeVolTools, []string{"subnet-id-1", "subnet-id-2"}, "security-group-id", fakeClock, update)
			filesystemID = aws.String("fake-fs-id")
		})

		Context(".Start", func() {
			JustBeforeEach(func() {
				provisionOp.CreateFs()
			})
			Context("when amazon's create file system returns ok", func() {
				BeforeEach(func() {
					fakeEFSService.CreateFileSystemReturns(&efs.FileSystemDescription{
						FileSystemId: filesystemID,
					}, nil)
				})
				It("should move to check-for-fs state when amazon fails to acknowledge", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.CheckFs))
					Expect(operationState.FsID).To(ContainSubstring(*filesystemID))
				})
			})
			Context("when amazon's create file system returns error", func() {
				var SomeErr = fmt.Errorf("some err")
				BeforeEach(func() {
					fakeEFSService.CreateFileSystemReturns(nil, SomeErr)
				})
				It("should error when amazon fails to acknowledge", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).To(Equal(SomeErr))
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
				provisionOp.CheckFs()
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
					Expect(provisionOp.State()).To(BeState(provisionOp.CheckFs))
					Expect(operationState.FsState).To(ContainSubstring(efs.LifeCycleStateCreating))
					Expect(operationState.Err).To(BeNil())
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
					Expect(operationState.FsState).To(ContainSubstring(efs.LifeCycleStateAvailable))
					Expect(operationState.Err).To(BeNil())
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
					Expect(provisionOp.State()).To(BeState(provisionOp.CheckFs))
					Expect(operationState.FsState).To(ContainSubstring(efs.LifeCycleStateDeleted))
					Expect(operationState.Err).To(BeNil())
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
				It("should move to finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe file system returns an error", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeFileSystemsReturns(nil, errors.New("badness"))
				})
				It("should move to finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).NotTo(BeNil())
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
				It("should move to finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
		})

		Context(".CreateMountTarget", func() {
			JustBeforeEach(func() {
				provisionOp.CreateMountTarget()
			})
			Context("when amazon's create mount target return successfully", func() {
				BeforeEach(func() {
					fakeEFSService.CreateMountTargetReturns(nil, nil)
				})
				It("should move to the finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.CheckMountTarget))
					Expect(operationState.Err).To(BeNil())
				})
			})
			Context("when amazon's create mount target returns an error", func() {
				BeforeEach(func() {
					fakeEFSService.CreateMountTargetReturns(nil, errors.New("badness"))
				})
				It("should move to the finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).NotTo(BeNil())
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
							IpAddress:      aws.String("1.2.3.4"),
							LifeCycleState: aws.String(efs.LifeCycleStateAvailable),
						}},
					}, nil)
				})
				It("should move to the open perms state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.OpenPerms))
					Expect(operationState.Err).To(BeNil())
					Expect(operationState.MountTargetID).To(Equal("fake-mt-id"))
					Expect(operationState.MountTargetIp).To(Equal("1.2.3.4"))
				})
			})
			Context("when amazon's describe mount target returns an error", func() {
				BeforeEach(func() {
					fakeEFSService.DescribeMountTargetsReturns(nil, errors.New("badness"))
				})
				It("should move to the finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).NotTo(BeNil())
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
				It("should move to the finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).NotTo(BeNil())
				})
			})
			Context("when amazon's describe mount target returns creating", func() {
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
					Expect(operationState.MountTargetState).To(Equal(efs.LifeCycleStateCreating))
					Expect(provisionOp.State()).To(BeState(provisionOp.CheckMountTarget))
				})
			})
		})

		Context(".OpenPerms", func() {
			//Expect(operationState.Err).NotTo(BeNil())

			JustBeforeEach(func() {
				provisionOp.OpenPerms()
			})
			Context("when can open permissions", func() {
				BeforeEach(func() {
					fakeVolTools.OpenPermsReturns(efsvoltools.ErrorResponse{Err: ""})
				})
				It("moves to finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
					Expect(operationState.Err).To(BeNil())
				})
			})
			Context("when can not open permissions", func() {
				BeforeEach(func() {
					fakeVolTools.OpenPermsReturns(efsvoltools.ErrorResponse{Err: "An error occured"})
				})
				It("moves to finish state", func() {
					Expect(provisionOp.State()).To(BeState(provisionOp.Finish))
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
				InstanceID:    instanceID,
				FsID:          fsId,
				MountTargetID: mountId,
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
			Context("when amazon's describe mount target returns an unexpected lifecycle state", func() {
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
