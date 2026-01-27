// Package db2e2e_test will run integration tests for using etcd as backend with v3 api
package db2e2e_test

import (
	"context"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

var _ = Describe("Smoke", Ordered, func() {
	const (
		autoRemove = true

		db2Password = "test456"
		db2Alias    = "db2"
		db2DB       = "testdb"
	)

	var (
		ctx context.Context
		nw  *testcontainers.DockerNetwork

		db2Cnt testcontainers.Container

		allContainers []testcontainers.Container
		db2Env        = map[string]string{
			"DB2_HOST":     db2Alias,
			"DB2_PORT":     "50000",
			"DB2_DATABASE": db2DB,
			"DB2_USER":     "db2inst1",
			"DB2_PASSWORD": db2Password,
			"DB2_PROTOCOL": "TCPIP",
		}
	)
	BeforeAll(func() {
		// RYUK requires permissions we don't need and don't want to implement
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

		ctx = context.Background()

		var nwErr error
		nw, nwErr = network.New(ctx)
		Ω(nwErr).NotTo(HaveOccurred())

		// start db2
		var db2Err error
		db2Cnt, db2Err = runDB2(ctx, nw,
			map[string][]string{nw.Name: {db2Alias}},
			map[string]string{
				"DB2INST1_PASSWORD": db2Password,
				"DBNAME":            db2DB,
			},
		)
		Ω(db2Err).NotTo(HaveOccurred())
		allContainers = append(allContainers, db2Cnt)
		/*
			db2LogErr := db2Cnt.StartLogProducer(ctx)
			if db2LogErr != nil {
				log.Printf("Kon log producer niet starten: %v", db2LogErr)
			}
			defer db2Cnt.StopLogProducer()
			db2Cnt.FollowOutput(&testcontainers.StdoutLogConsumer{})
		*/
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
	/*
		Context("when running consistency check", func() {
			It("should work properly", func() {
				// run dbtwool consistency check
				dbtwoolCnt, initErr := runDbwTool(
					ctx,
					nw,
					db2Env,
					"consistency")
				Ω(initErr).NotTo(HaveOccurred())
				allContainers = append(allContainers, dbtwoolCnt)
				dbtwoolLogs, logErr := containerLogs(ctx, dbtwoolCnt)
				Ω(logErr).NotTo(HaveOccurred())
				Ω(dbtwoolLogs).To(MatchRegexp("info.*finished"))
			})
		})
	*/
	Context("when running consistency check", func() {
		It("should work properly", func() {
			// run dbtwool consistency check
			for _, jobType := range []string{"ru-performance", "lob-performance"} {
				for _, phase := range []string{"stage", "gen", "test"} {
					dbtwoolCnt, initErr := runDbwTool(
						ctx,
						nw,
						db2Env,
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
})
