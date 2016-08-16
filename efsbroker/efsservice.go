package efsbroker

import "github.com/aws/aws-sdk-go/service/efs"

//go:generate counterfeiter -o efsfakes/fake_service.go . EFSService

type EFSService interface {
	CreateFileSystem(*efs.CreateFileSystemInput) (*efs.FileSystemDescription, error)

	DeleteFileSystem(*efs.DeleteFileSystemInput) (*efs.DeleteFileSystemOutput, error)

	DescribeFileSystems(*efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error)

	CreateMountTarget(*efs.CreateMountTargetInput) (*efs.MountTargetDescription, error)

	DeleteMountTarget(*efs.DeleteMountTargetInput) (*efs.DeleteMountTargetOutput, error)

	DescribeMountTargets(*efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error)
}
