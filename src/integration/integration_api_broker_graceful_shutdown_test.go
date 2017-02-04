package integration_test

import (
	"fmt"
	. "integration"
	"net/http"
	"path/filepath"

	"code.cloudfoundry.org/cfhttp"

	"net"

	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/ghttp"
	"github.com/tedsuo/ifrit/ginkgomon"
)

var _ = Describe("Integration_Api_Broker_Graceful_Shutdown", func() {

	const (
		MessageForServer = "message_for_server"
	)

	var (
		runner *ginkgomon.Runner
		buffer *gbytes.Buffer
	)

	BeforeEach(func() {
		fakeApiServer := ghttp.NewServer()

		serviceBrokerConfPath = components.PrepareServiceBrokerConfig(components.Ports[ServiceBroker], brokerUserName, brokerPassword, dbUrl, fakeApiServer.URL(), brokerApiHttpRequestTimeout, tmpDir)
	})

	AfterEach(func() {
		stopAll()
	})

	Describe("Shutdown", func() {
		Context("ApiServer", func() {
			BeforeEach(func() {
				fakeScheduler = ghttp.NewServer()
				apiServerConfPath = components.PrepareApiServerConfig(components.Ports[APIServer], dbUrl, fakeScheduler.URL(), tmpDir)
				apiTLSConfig, err := cfhttp.NewTLSConfig(
					filepath.Join(testCertDir, "api.crt"),
					filepath.Join(testCertDir, "api.key"),
					filepath.Join(testCertDir, "autoscaler-ca.crt"),
				)
				Expect(err).NotTo(HaveOccurred())
				httpClient.Transport.(*http.Transport).TLSClientConfig = apiTLSConfig
				httpClient.Timeout = apiSchedulerHttpRequestTimeout
				runner = startApiServer()
				buffer = runner.Buffer()
			})

			AfterEach(func() {
				fakeScheduler.Close()
				stopAll()
			})

			Context("with a SIGUSR2 signal", func() {
				It("stops receiving new connections", func() {
					fakeScheduler.AppendHandlers(ghttp.RespondWith(http.StatusOK, "successful"))

					policyStr := readPolicyFromFile("fakePolicyWithSchedule.json")
					resp, err := attachPolicy(getRandomId(), policyStr)
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).To(Equal(http.StatusCreated))
					resp.Body.Close()

					sendSigusr2Signal(APIServer)
					Eventually(buffer, 5*time.Second).Should(gbytes.Say(`Received SIGUSR2 signal`))

					Eventually(processMap[APIServer].Wait()).Should(Receive())
					Expect(runner.ExitCode()).Should(Equal(0))
				})

				It("waits for in-flight request to finish", func() {
					ch := make(chan struct{})
					fakeScheduler.AppendHandlers(func(w http.ResponseWriter, r *http.Request) {
						defer GinkgoRecover()
						ch <- struct{}{}

						Eventually(ch).Should(BeClosed())
						w.WriteHeader(http.StatusOK)
					})

					done := make(chan struct{})
					go func() {
						defer GinkgoRecover()
						policyStr := readPolicyFromFile("fakePolicyWithSchedule.json")
						resp, err := attachPolicy(getRandomId(), policyStr)
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusCreated))
						resp.Body.Close()
						close(done)
					}()

					Eventually(ch).Should(Receive())
					sendSigusr2Signal(APIServer)

					Eventually(buffer, 5*time.Second).Should(gbytes.Say(`Received SIGUSR2 signal`))
					Consistently(processMap[APIServer].Wait()).ShouldNot(Receive())
					close(ch)
					Eventually(done).Should(BeClosed())

					Eventually(processMap[APIServer].Wait()).Should(Receive())
					Expect(runner.ExitCode()).Should(Equal(0))
				})
			})
		})

		Context("Service Broker", func() {

			BeforeEach(func() {
				runner = startServiceBroker()
				buffer = runner.Buffer()
			})

			It("stops receiving new connections on receiving SIGUSR2 signal", func() {
				conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[ServiceBroker]))
				Expect(err).NotTo(HaveOccurred())

				_, err = conn.Write([]byte(MessageForServer))
				Expect(err).NotTo(HaveOccurred())

				newConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[ServiceBroker]))
				Expect(err).NotTo(HaveOccurred())

				conn.Close()
				newConn.Close()

				sendSigusr2Signal(ServiceBroker)
				Eventually(buffer, 5*time.Second).Should(gbytes.Say(`Received SIGUSR2 signal`))

				Eventually(processMap[ServiceBroker].Wait()).Should(Receive())
				Expect(runner.ExitCode()).Should(Equal(0))

				_, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[ServiceBroker]))
				Expect(err).To(HaveOccurred())

			})

			It("waits for in-flight request to finish before shutting down server on receiving SIGUSR2 signal", func() {
				conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[ServiceBroker]))
				Expect(err).NotTo(HaveOccurred())

				sendSigusr2Signal(ServiceBroker)
				Eventually(buffer, 5*time.Second).Should(gbytes.Say(`Received SIGUSR2 signal`))

				Expect(runner.ExitCode()).Should(Equal(-1))

				_, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[ServiceBroker]))
				Expect(err).To(HaveOccurred())

				_, err = conn.Write([]byte(MessageForServer))
				Expect(err).NotTo(HaveOccurred())

				conn.Close()

				Eventually(processMap[ServiceBroker].Wait()).Should(Receive())
				Expect(runner.ExitCode()).Should(Equal(0))
			})

		})

	})

	Describe("Non Graceful Shutdown", func() {

		Context("ApiServer", func() {

			BeforeEach(func() {
				startApiServer()
			})

			It("kills server immediately without serving the in-flight requests", func() {
				conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[APIServer]))
				Expect(err).NotTo(HaveOccurred())

				_, err = conn.Write([]byte(MessageForServer))
				Expect(err).NotTo(HaveOccurred())

				sendKillSignal(APIServer)

				_, err = conn.Write([]byte(MessageForServer))
				Expect(err).To(HaveOccurred())

				_, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[APIServer]))
				Expect(err).To(HaveOccurred())

			})
		})

		Context("Service Broker", func() {

			BeforeEach(func() {
				startServiceBroker()
			})

			It("kills server immediately without serving the in-flight requests", func() {
				conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[ServiceBroker]))
				Expect(err).NotTo(HaveOccurred())

				_, err = conn.Write([]byte(MessageForServer))
				Expect(err).NotTo(HaveOccurred())

				sendKillSignal(ServiceBroker)

				_, err = conn.Write([]byte(MessageForServer))
				Expect(err).To(HaveOccurred())

				_, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", components.Ports[ServiceBroker]))
				Expect(err).To(HaveOccurred())

			})
		})

	})

})
