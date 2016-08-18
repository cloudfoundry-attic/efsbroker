package main

import (
	"flag"
	"strings"

	"code.cloudfoundry.org/cflager"
	"code.cloudfoundry.org/debugserver"

	"fmt"

	"os"

	"code.cloudfoundry.org/efsbroker/efsbroker"
	"code.cloudfoundry.org/efsbroker/utils"
	"code.cloudfoundry.org/goshims/ioutil"
	"code.cloudfoundry.org/goshims/os"
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
var planName = flag.String(
	"planName",
	"free",
	"name of the service plan to register with cloud controller",
)
var planId = flag.String(
	"planId",
	"free-plan-guid",
	"ID of the service plan to register with cloud controller",
)
var planDesc = flag.String(
	"planDesc",
	"free efs filesystem",
	"description of the service plan to register with cloud controller",
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

	serviceBroker := efsbroker.New(logger,
		*serviceName, *serviceId,
		*planName, *planId, *planDesc,
		*dataDir, &osshim.OsShim{}, &ioutilshim.IoutilShim{},
		efsClient, parseSubnets(*awsSubnetIds))

	credentials := brokerapi.BrokerCredentials{Username: *username, Password: *password}
	handler := brokerapi.New(serviceBroker, logger.Session("broker-api"), credentials)

	return http_server.New(*atAddress, handler)
}
