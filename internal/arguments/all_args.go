package arguments

var (
	// AllArgs defines all arguments that can be used in DBTwool
	AllArgs = Args{
		"cfgFile":        {short: "c", defValue: "config.yaml", argType: typePath, desc: `config file`},
		"isolationLevel": {short: "i", defValue: "1", argType: typeString, desc: `Transaction isolation level`},
		"spread":         {short: "s", defValue: []string{"100%:8b"}, argType: typeStringArray, desc: `spread. By default everything is 8 bytes`},
		"byteSize": {short: "b", defValue: "1kb", argType: typeString,
			desc: `What the size of the datasource should be in b, kb, gb, etc.`},
		"lobType": {short: "l", defValue: "blob", argType: typeString,
			desc: `What type of large object. (BLOB, CLOB, JSONB, etc.)`},
		"emptyLobs": {short: "e", defValue: uint(0), argType: typeUInt,
			desc: `How many rows of empty lobs to generate`},
		"randomizerSeed": {short: "r", defValue: "0", argType: typeString,
			desc: `seed to use for reproducability of the tests. Leave empty for random seed.`},
		"table": {short: "t", defValue: "twooltests.lobtable", argType: typeString,
			desc: `What the schema + table name should be`},
		"parallel":      {short: "p", defValue: uint(1), argType: typeUInt, desc: `The degree of parallel execution`},
		"warmupTime":    {short: "w", defValue: uint(1), argType: typeUInt, desc: `The test warmup time in seconds`},
		"executionTime": {short: "x", defValue: uint(1), argType: typeUInt, desc: `The test execution time in seconds`},
		"readMode": {short: "m", defValue: "scattered", argType: typeString,
			desc: `How the reading of LOBs is distributed. 'scattered' or 'sequential'. leave empty for scattered.`},
	}
)
