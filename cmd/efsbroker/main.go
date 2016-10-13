package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"code.cloudfoundry.org/cflager"
	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/efsbroker/efsbroker"
	"code.cloudfoundry.org/efsbroker/utils"
	"code.cloudfoundry.org/efsdriver/efsvoltools/voltoolshttp"
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
)

var dataDir = flag.String(
	"dataDir",
	"",
	"[REQUIRED] - Broker's state will be stored here to persist across reboots",
)

var atAddress = flag.String(
	"listenAddr",
	"0.0.0.0:8999",
	"host:port to serve service broker API",
)
var efsToolsAddress = flag.String(
	"efsToolsAddress",
	"127.0.0.1:7090",
	"host:port to reach the efsdriver when creating volumes",
)
var serviceName = flag.String(
	"serviceName",
	"efsvolume",
	"name of the service to register with cloud controller",
)
var serviceId = flag.String(
	"serviceId",
	"service-guid",
	"ID of the service to register with cloud controller",
)
var username = flag.String(
	"username",
	"admin",
	"basic auth username to verify on incoming requests",
)
var password = flag.String(
	"password",
	"admin",
	"basic auth password to verify on incoming requests",
)
var awsSubnetIds = flag.String(
	"awsSubnetIds",
	"",
	"list of comma-seperated aws subnet ids where mount targets will be created for each efs",
)
var awsSecurityGroup = flag.String(
	"awsSecurityGroup",
	"",
	"aws security group to assign to the mount point",
)

func main() {
	parseCommandLine()

	checkParams()

	logger, logSink := cflager.New("efsbroker")
	logger.Info("starting")
	defer logger.Info("ends")

	server := createServer(logger)

	if dbgAddr := debugserver.DebugAddress(flag.CommandLine); dbgAddr != "" {
		server = utils.ProcessRunnerFor(grouper.Members{
			{"debug-server", debugserver.Runner(dbgAddr, logSink)},
			{"broker-api", server},
		})
	}

	process := ifrit.Invoke(server)
	logger.Info("started")
	utils.UntilTerminated(logger, process)
}

func parseCommandLine() {
	cflager.AddFlags(flag.CommandLine)
	debugserver.AddFlags(flag.CommandLine)
	flag.Parse()
}

func checkParams() {
	if *dataDir == "" {
		fmt.Fprint(os.Stderr, "\nERROR: Required parameter dataDir not defined.\n\n")
		flag.Usage()
		os.Exit(1)
	}

	if *awsSubnetIds == "" {
		fmt.Fprint(os.Stderr, "\nERROR: Required parameter awsSubnetIds not defined.\n\n")
		flag.Usage()
		os.Exit(1)
	}
}

func parseSubnets(subnetsFlag string) []string {
	return strings.Split(subnetsFlag, ",")
}

func createServer(logger lager.Logger) ifrit.Runner {
	session, err := session.NewSession()
	if err != nil {
		panic(err)
	}

	config := aws.NewConfig()

	efsClient := efs.New(session, config)

	efsTools, err := voltoolshttp.NewRemoteClient("http://" + *efsToolsAddress)
	if err != nil {
		panic(err)
	}

	serviceBrokerWithContext := efsbroker.New(logger,
		*serviceName, *serviceId,
		*dataDir, &osshim.OsShim{}, &ioutilshim.IoutilShim{}, clock.NewClock(),
		efsClient, parseSubnets(*awsSubnetIds), *awsSecurityGroup, efsTools, efsbroker.NewProvisionOperation, efsbroker.NewDeprovisionOperation)

	credentials := brokerapi.BrokerCredentials{Username: *username, Password: *password}
	handler := brokerapi.NewWithContext(serviceBrokerWithContext, logger.Session("broker-api"), credentials)

	return http_server.New(*atAddress, handler)
}
