// Package pge2e_test will run integration tests for using etcd as backend with v3 api
package pge2e_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/docker/go-connections/nat"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

var _ = Describe("Smoke", Ordered, func() {
	const (
		autoRemove = true

		pgUser     = "postgres"
		pgDatabase = "postgres"
		pgPassword = "test123"
		pgAlias    = "postgres"
		pgPort     = 5432
	)

	var (
		ctx context.Context
		nw  *testcontainers.DockerNetwork

		pgCnt         testcontainers.Container
		allContainers []testcontainers.Container
		pgConn        = pgConnParams{
			"host":     "localhost",
			"user":     pgUser,
			"password": pgPassword,
			"dbname":   pgDatabase,
		}
		pgEnv = map[string]string{
			"PGHOST":     pgAlias,
			"PGPASSWORD": pgPassword,
		}
	)
	BeforeAll(func() {
		// RYUK requires permissions we don't need and don't want to implement
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

		ctx = context.Background()

		var nwErr error
		nw, nwErr = network.New(ctx)
		Ω(nwErr).NotTo(HaveOccurred())

		// start postgres
		var pgErr error
		pgCnt, pgErr = runPostgres(ctx, nw,
			map[string][]string{nw.Name: {pgAlias}},
			map[string]string{"POSTGRES_PASSWORD": pgPassword},
		)
		Ω(pgErr).NotTo(HaveOccurred())
		allContainers = append(allContainers, pgCnt)
	})
	AfterAll(func() {
		if !autoRemove {
			return
		}
		if CurrentSpecReport().Failed() {
			GinkgoWriter.Printf("Test failed! not cleaning containers")
			return
		}
		for _, cnt := range allContainers {
			Ω(cnt.Terminate(ctx)).NotTo(HaveOccurred())
		}
		Ω(nw.Remove(ctx)).NotTo(HaveOccurred())
	})
	Context("when running consistency check", func() {
		It("should work properly", func() {
			// run dbtwool consistency check
			for _, jobType := range []string{"ru-performance", "lob-performance"} {
				for _, phase := range []string{"stage", "gen", "test"} {
					dbtwoolCnt, initErr := runDbwTool(
						ctx,
						nw,
						pgEnv,
						jobType, phase)
					Ω(initErr).NotTo(HaveOccurred())
					allContainers = append(allContainers, dbtwoolCnt)
					dbtwoolLogs, logErr := containerLogs(ctx, dbtwoolCnt)
					Ω(logErr).NotTo(HaveOccurred())
					Ω(dbtwoolLogs).To(MatchRegexp(".*info.*finished.*"))
				}
			}
		})
	})
	Context("when connecting to Postgres", func() {
		It("should work properly", func() {
			natPort, err := pgCnt.MappedPort(ctx,
				nat.Port(fmt.Sprintf("%d/tcp", pgPort)))
			Ω(err).NotTo(HaveOccurred())
			pgConnSettings := pgConn.setParam("port", natPort.Port())
			// This does not work directly after starting the container but does after 5s.
			// So, we will try this for 10 seconds
			isReadyCtx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second*10))
			defer cancelFunc()
			// every 100 miliseconds
			isReadyErr := isReady(isReadyCtx, pgConnSettings, time.Millisecond*100)
			Ω(isReadyErr).NotTo(HaveOccurred())
		})
	})
})
