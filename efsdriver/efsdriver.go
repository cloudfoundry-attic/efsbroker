package efsdriver

import "github.com/aws/aws-sdk-go/service/efs"

//go:generate counterfeiter -o efsdriverfakes/fake_service.go . EFSService

type EFSService interface {
	CreateFileSystem(*efs.CreateFileSystemInput) (*efs.FileSystemDescription, error)

	DeleteFileSystem(*efs.DeleteFileSystemInput) (*efs.DeleteFileSystemOutput, error)

	DescribeFileSystems(*efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error)
}
