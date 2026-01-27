package dbinterface

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

// ConsistencyTest runs a consistency test against DB2
func ConsistencyTest(
	ctx context.Context,
	cl Client,
	olapQuery string,
	isolationLevel IsolationLevel,
	oltpLockQuery string,
	oltpUpdateQuery string,
) {
	var logger = log.With().Logger()
	pool, poolErr := cl.Pool(ctx)
	if poolErr != nil {
		logger.Fatal().Msgf("Failed to connect: %v", poolErr)
	}

	conn1, connectErr1 := pool.Connect(ctx) //
	if connectErr1 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %v", connectErr1)
	}
	defer conn1.Close(ctx)

	conn2, connectErr2 := pool.Connect(ctx)
	if connectErr2 != nil {
		logger.Fatal().Msgf("connect error for connection 1: %v", connectErr2)
	}
	defer conn2.Close(ctx)

	start := time.Now()

	logSinceElapsed := func(formatted string, args ...any) {
		elapsed := time.Since(start).Milliseconds()
		logger.Info().Int64("elapsed (ms)", elapsed).Msgf(formatted, args...)
	}

	if err := conn2.SetIsolationLevel(ctx, isolationLevel); err != nil {
		logger.Fatal().Msgf("error while setting iso level on connection 1: %v", err)
	}

	logSinceElapsed("T1: BEGIN;")
	if err := conn1.Begin(ctx); err != nil {
		logger.Fatal().Msgf("error during begin transaction on connection 1: %v", err)
	}

	if row, err := conn1.QueryOneRow(ctx, olapQuery); err != nil {
		logger.Fatal().Msgf("error during fetch of olap query: %v", err)
	} else {
		logger.Info().Msgf("T1: result: %v", row)
	}

	logSinceElapsed("T1: %s", oltpLockQuery)
	if _, err := conn1.Execute(ctx, oltpLockQuery); err != nil {
		logger.Fatal().Err(err)
	}

	go func() {
		logSinceElapsed("T2: BEGIN;")
		if err := conn2.Begin(ctx); err != nil {
			logger.Fatal().Msgf("error during begin transaction: %v", err)
		}

		logSinceElapsed("T2: %s", olapQuery)
		if row, err := conn2.QueryOneRow(ctx, olapQuery); err != nil {
			logger.Fatal().Msgf("error during fetch of olap query: %v", err)
		} else {
			logger.Info().Msgf("T2: result: %v", row)
		}
		conn2.Commit(ctx)
	}()

	logSinceElapsed("T1: %s", oltpUpdateQuery)
	if _, err := conn1.Execute(ctx, oltpUpdateQuery); err != nil {
		logger.Fatal().Err(err)
	}

	logSinceElapsed("T1: sleeping 10s');")
	time.Sleep(10 * time.Second)

	logSinceElapsed("T1: COMMIT;")
	if err := conn1.Commit(ctx); err != nil {
		logger.Fatal().Err(err)
	}
}
