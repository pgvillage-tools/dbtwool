package arguments

var (
	// AllArgs defines all arguments that can be used in DBTwool
	AllArgs = Args{
		"cfgFile":        {short: "c", defValue: "config.yaml", argType: typePath, desc: `config file`},
		"isolationLevel": {short: "i", defValue: "1", argType: typeString, desc: `Transaction isolation level`},
		"spread":         {short: "s", argType: typeStringArray, desc: `spread`},
		"bytesize": {short: "b", argType: typeString,
			desc: `What the size of the datasource should be in b, kb, gb, etc.`},
		"table": {short: "t", defValue: "dbtwooltests.lobtable", argType: typeString,
			desc: `What the schema + table name should be`},
		"parallel": {short: "p", defValue: uint(1), argType: typeUInt, desc: `The degree of parallel execution`},
	}
)
