package arguments

// CLI argument keys used throughout the application.
const (
	ArgCfgFile        = "cfgFile"
	ArgIsolationLevel = "isolationLevel"
	ArgSpread         = "spread"
	ArgByteSize       = "byteSize"
	ArgBatchSize      = "batchSize"
	ArgLobType        = "lobType"
	ArgEmptyLobs      = "emptyLobs"
	ArgRandomizerSeed = "randomizerSeed"
	ArgTable          = "table"
	ArgParallel       = "parallel"
	ArgWarmupTime     = "warmupTime"
	ArgExecutionTime  = "executionTime"
	ArgReadMode       = "readMode"
	ArgNumOfRows      = "numOfRows"
)

var (
	// AllArgs defines all arguments that can be used in DBTwool
	AllArgs = Args{
		ArgCfgFile: {short: "c", defValue: "config.yaml", argType: typePath,
			desc: `config file`},
		ArgIsolationLevel: {short: "i", defValue: "1", argType: typeString,
			desc: `Transaction isolation level`},
		ArgSpread: {short: "s", defValue: []string{"100%:8b"}, argType: typeStringArray,
			desc: `spread. By default everything is 8 bytes`},
		ArgByteSize: {short: "b", defValue: "1kb", argType: typeString,
			desc: `What the size of the datasource should be in b, kb, gb, etc.`},
		ArgBatchSize: {short: "B", defValue: uint(50), argType: typeUInt,
			desc: `Number of inserts in one batch transactions`},
		ArgLobType: {short: "l", defValue: "blob", argType: typeString,
			desc: `What type of large object. (BLOB, CLOB, JSONB, etc.)`},
		ArgEmptyLobs: {short: "e", defValue: uint(0), argType: typeUInt,
			desc: `How many rows of empty lobs to generate`},
		ArgRandomizerSeed: {short: "r", argType: typeString,
			desc: `seed to use for reproducability of the tests. Leave empty for random seed.`},
		ArgTable: {short: "t", defValue: "dbtwooltests.lobtable", argType: typeString,
			desc: `What the schema + table name should be`},
		ArgParallel: {short: "p", defValue: uint(1), argType: typeUInt,
			desc: `The degree of parallel execution`},
		ArgWarmupTime: {short: "w", defValue: uint(1), argType: typeUInt,
			desc: `The test warmup time in seconds`},
		ArgExecutionTime: {short: "x", defValue: uint(1), argType: typeUInt,
			desc: `The test execution time in seconds`},
		ArgReadMode: {short: "m", defValue: "scattered", argType: typeString,
			desc: `How the reading of LOBs is distributed. 'scattered' or 'sequential'. leave empty for scattered.`},
		ArgNumOfRows: {short: "n", defValue: uint(10000000), argType: typeUInt,
			desc: `How many rows to generate`},
	}
)
