package main

import (
	"io"
	"net/http"
	"os/exec"
	"strconv"

	"code.cloudfoundry.org/goshims/osshim/os_fake"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"

	"encoding/json"
	"io/ioutil"

	"fmt"

	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
)

type failRunner struct {
	Command           *exec.Cmd
	Name              string
	AnsiColorCode     string
	StartCheck        string
	StartCheckTimeout time.Duration
	Cleanup           func()
	session           *gexec.Session
	sessionReady      chan struct{}
}

func (r failRunner) Run(sigChan <-chan os.Signal, ready chan<- struct{}) error {
	defer GinkgoRecover()

	allOutput := gbytes.NewBuffer()

	debugWriter := gexec.NewPrefixedWriter(
		fmt.Sprintf("\x1b[32m[d]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
		GinkgoWriter,
	)

	session, err := gexec.Start(
		r.Command,
		gexec.NewPrefixedWriter(
			fmt.Sprintf("\x1b[32m[o]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
			io.MultiWriter(allOutput, GinkgoWriter),
		),
		gexec.NewPrefixedWriter(
			fmt.Sprintf("\x1b[91m[e]\x1b[%s[%s]\x1b[0m ", r.AnsiColorCode, r.Name),
			io.MultiWriter(allOutput, GinkgoWriter),
		),
	)

	Ω(err).ShouldNot(HaveOccurred())

	fmt.Fprintf(debugWriter, "spawned %s (pid: %d)\n", r.Command.Path, r.Command.Process.Pid)

	r.session = session
	if r.sessionReady != nil {
		close(r.sessionReady)
	}

	startCheckDuration := r.StartCheckTimeout
	if startCheckDuration == 0 {
		startCheckDuration = 5 * time.Second
	}

	var startCheckTimeout <-chan time.Time
	if r.StartCheck != "" {
		startCheckTimeout = time.After(startCheckDuration)
	}

	detectStartCheck := allOutput.Detect(r.StartCheck)

	for {
		select {
		case <-detectStartCheck: // works even with empty string
			allOutput.CancelDetects()
			startCheckTimeout = nil
			detectStartCheck = nil
			close(ready)

		case <-startCheckTimeout:
			// clean up hanging process
			session.Kill().Wait()

			// fail to start
			return fmt.Errorf(
				"did not see %s in command's output within %s. full output:\n\n%s",
				r.StartCheck,
				startCheckDuration,
				string(allOutput.Contents()),
			)

		case signal := <-sigChan:
			session.Signal(signal)

		case <-session.Exited:
			if r.Cleanup != nil {
				r.Cleanup()
			}

			Expect(string(allOutput.Contents())).To(ContainSubstring(r.StartCheck))
			Expect(session.ExitCode()).NotTo(Equal(0), fmt.Sprintf("Expected process not to exit with 0, got: %d", session.ExitCode()))
			return nil
		}
	}
}

var _ = Describe("Efsbroker Main", func() {
	Context("Parse VCAP_SERVICES tests", func() {
		var (
			port   string
			fakeOs os_fake.FakeOs = os_fake.FakeOs{}
			logger lager.Logger
		)

		BeforeEach(func() {
			*dbDriver = "postgres"
			*cfServiceName = "postgresql"
			logger = lagertest.NewTestLogger("test-broker-main")
		})

		JustBeforeEach(func() {
			env := fmt.Sprintf(`
				{
					"postgresql":[
						{
							"credentials":{
								"dbType":"postgresql",
								"hostname":"8.8.8.8",
								"name":"foo",
								"password":"foo",
								"port":%s,
								"uri":"postgresql://foo:foo@8.8.8.8:9999/foo",
								"username":"foo"
							},
							"label":"postgresql",
							"name":"foobroker",
							"plan":"amanaplanacanalpanama",
							"provider":null,
							"syslog_drain_url":null,
							"tags":[
								"postgresql",
								"cache"
							],
							"volume_mounts":[]
						}
					]
				}`, port)
			fakeOs.LookupEnvReturns(env, true)
		})

		Context("when port is a string", func() {
			BeforeEach(func() {
				port = `"9999"`
			})

			It("should succeed", func() {
				Expect(func() { parseVcapServices(logger, &fakeOs) }).NotTo(Panic())
				Expect(*dbPort).To(Equal("9999"))
			})
		})
		Context("when port is a number", func() {
			BeforeEach(func() {
				port = `9999`
			})

			It("should succeed", func() {
				Expect(func() { parseVcapServices(logger, &fakeOs) }).NotTo(Panic())
				Expect(*dbPort).To(Equal("9999"))
			})
		})
		Context("when port is an array", func() {
			BeforeEach(func() {
				port = `[9999]`
			})

			It("should panic", func() {
				Expect(func() { parseVcapServices(logger, &fakeOs) }).To(Panic())
			})
		})
	})

	Context("Missing required args", func() {
		It("shows usage", func() {
			var args []string
			volmanRunner := failRunner{
				Name:       "efsbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "Either dataDir or db parameters must be provided.",
			}
			process := ifrit.Invoke(volmanRunner)
			ginkgomon.Kill(process) // this is only if incorrect implementation leaves process running
		})
	})

	Context("Missing required args", func() {
		It("fails with an error", func() {
			var args []string
			args = append(args, "-listenAddr", "0.0.0.0:" + strconv.Itoa(8999+GinkgoParallelNode()))
			args = append(args, "-username", "admin")
			args = append(args, "-password", "password")
			args = append(args, "-dataDir", os.TempDir())
			args = append(args, "-awsSubnetIds", "subnet-ajwe8912,subnet-olj23e")
			args = append(args, "-awsAZs", "foo-foo-2a,foo-foo-2b,BAD-EXTRA-AZ")
			args = append(args, "-awsSecurityGroups", "sg-foo,sg-bar")

			volmanRunner := failRunner{
				Name:       "efsbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "arguments awsSubnetIds, awsAZs, and awsSecurityGroups must have the same number of entries",
			}
			process := ifrit.Invoke(volmanRunner)
			ginkgomon.Kill(process) // this is only if incorrect implementation leaves process running
		})
	})

	Context("Has required args", func() {
		var (
			args                                    []string
			listenAddr                              string
			tempDir                                 string
			username, password                      string
			awsSubnetIds, awsAZs, awsSecurityGroups string

			process ifrit.Process
		)

		BeforeEach(func() {
			listenAddr = "0.0.0.0:" + strconv.Itoa(8999+GinkgoParallelNode())
			username = "admin"
			password = "password"
			tempDir = os.TempDir()
			awsSubnetIds = "subnet-ajwe8912,subnet-olj23e"
			awsAZs = "foo-foo-2a,foo-foo-2b"
			awsSecurityGroups = "sg-foo,sg-bar"

			args = append(args, "-listenAddr", listenAddr)
			args = append(args, "-username", username)
			args = append(args, "-password", password)
			args = append(args, "-dataDir", tempDir)
			args = append(args, "-awsSubnetIds", awsSubnetIds)
			args = append(args, "-awsAZs", awsAZs)
			args = append(args, "-awsSecurityGroups", awsSecurityGroups)

		})

		JustBeforeEach(func() {
			volmanRunner := ginkgomon.New(ginkgomon.Config{
				Name:       "efsbroker",
				Command:    exec.Command(binaryPath, args...),
				StartCheck: "started",
			})
			process = ginkgomon.Invoke(volmanRunner)
		})

		AfterEach(func() {
			ginkgomon.Kill(process)
		})

		httpDoWithAuth := func(method, endpoint string, body io.ReadCloser) (*http.Response, error) {
			req, err := http.NewRequest(method, "http://"+listenAddr+endpoint, body)
			Expect(err).NotTo(HaveOccurred())

			req.SetBasicAuth(username, password)
			return http.DefaultClient.Do(req)
		}

		It("should listen on the given address", func() {
			resp, err := httpDoWithAuth("GET", "/v2/catalog", nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(resp.StatusCode).To(Equal(200))
		})

		Context("given arguments", func() {
			BeforeEach(func() {
				args = append(args, "-serviceName", "something")
				args = append(args, "-serviceId", "someguid")
			})

			It("should pass arguments though to catalog", func() {
				resp, err := httpDoWithAuth("GET", "/v2/catalog", nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(200))

				bytes, err := ioutil.ReadAll(resp.Body)
				Expect(err).NotTo(HaveOccurred())

				var catalog brokerapi.CatalogResponse
				err = json.Unmarshal(bytes, &catalog)
				Expect(err).NotTo(HaveOccurred())

				Expect(catalog.Services[0].Name).To(Equal("something"))
				Expect(catalog.Services[0].ID).To(Equal("someguid"))
				Expect(catalog.Services[0].Plans[0].ID).To(Equal("generalPurpose"))
				Expect(catalog.Services[0].Plans[0].Name).To(Equal("generalPurpose"))
				Expect(catalog.Services[0].Plans[0].Description).To(Equal("recommended for most file systems"))
			})
		})
	})
})
