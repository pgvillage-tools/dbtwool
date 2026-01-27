package arguments

var (
	// AllArgs defines all arguments that can be used in DBTwool
	AllArgs = Args{
		"cfgFile":        {short: "c", defValue: "config.yaml", argType: typePath, desc: `config file`},
		"isolationLevel": {short: "i", defValue: "1", argType: typeString, desc: `Transaction isolation level`},
		"spread":         {short: "s", argType: typeStringArray, desc: `spread`},
		"byteSize": {short: "b", argType: typeString,
			desc: `What the size of the datasource should be in b, kb, gb, etc.`},
		"lobType": {short: "l", defValue: "LOB", argType: typeString,
			desc: `What type of large object. (LOB, CLOB, JSONB, etc.)`},
		"emptyLobs": {short: "e", defValue: uint(0), argType: typeUInt,
			desc: `How many rows of empty lobs to generate`},
		"randomizerSeed": {short: "r", argType: typeString,
			desc: `seed to use for reproducability of the tests. Leave empty for random seed.`},
		"table": {short: "t", defValue: "twooltests.lobtable", argType: typeString,
			desc: `What the schema + table name should be`},
		"parallel": {short: "p", defValue: uint(1), argType: typeUInt, desc: `The degree of parallel execution`},
	}
)
