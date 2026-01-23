package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/pgvillage-tools/dbtwool/pkg/utils"
	"github.com/spf13/cobra"
)

type aType int

const (
	typeCount aType = iota
	typeUInt
	typeBool
	typeString
	typeStringArray
	typePath
	typeUnknown
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

var (
	typeToString = map[aType]string{
		typeCount:       "typeCount",
		typeUInt:        "typeUInt",
		typeBool:        "typeBool",
		typeString:      "typeString",
		typeStringArray: "typeStringArray",
		typePath:        "typePath",
		typeUnknown:     "typeUnknown",
	}
)

func (at aType) String() string {
	value, exists := typeToString[at]
	if !exists {
		return typeUnknown.String()
	}
	return value
}

const (
	confDir = "~/.dbtwool"
)

type arg struct {
	short            string
	desc             string
	extraEnvVars     []string
	defValue         any
	argType          aType
	stringValue      *string
	stringArrayValue *[]string
	uIntValue        *uint
	intValue         *int
	boolValue        *bool
}

func fromEnv(keys []string) string {
	for _, key := range keys {
		fromEnv := os.Getenv(key)
		if fromEnv != "" {
			return fromEnv
		}
	}
	return ""
}

type args map[string]arg

var (
	allArgs = args{
		"cfgFile":        {short: "c", defValue: "config.yaml", argType: typePath, desc: `config file`},
		"isolationLevel": {short: "i", defValue: "1", argType: typeString, desc: `Transaction isolation level`},
		"datasource":     {short: "d", defValue: "pg", argType: typeString, desc: `Datasource`},
		"spread":         {short: "s", argType: typeStringArray, desc: `spread`},
		"bytesize": {short: "b", argType: typeString,
			desc: `What the size of the datasource should be in b, kb, gb, etc.`},
		"table": {short: "t", defValue: "dbtwooltests.lobtable", argType: typeString,
			desc: `What the schema + table name should be`},
		"parallel": {short: "p", defValue: uint(1), argType: typeUInt, desc: `The degree of parallel execution`},
	}
)

func handleUintCommandArg(key string, argConfig *arg) (uint, error) {
	envVars := append(argConfig.extraEnvVars, "PGC_"+strings.ToUpper(toSnakeCase(key)))
	defaultFromEnv := fromEnv(envVars)
	if defaultFromEnv != "" {
		var (
			err    error
			defVal uint64
		)
		const (
			baseTen   = 10
			fourBytes = 32
		)
		defVal, err = strconv.ParseUint(defaultFromEnv, baseTen, fourBytes)
		if err != nil {
			return 0, fmt.Errorf("default from environment (%v) is invalid as int", defaultFromEnv)
		}
		argConfig.defValue = uint(defVal)
	} else if argConfig.defValue == nil {
		argConfig.defValue = uint(0)
	}
	defaultValue, ok := argConfig.defValue.(uint)
	if !ok {
		return 0,
			fmt.Errorf(
				"requested argument %s is %s, but %v (%T) cannot be parsed to %T",
				key,
				argConfig.argType.String(),
				argConfig.defValue,
				argConfig.defValue,
				defaultValue,
			)
	}
	return defaultValue, nil
}

func handleStringCommandArg(key string, argConfig *arg) (string, error) {
	envVars := append(argConfig.extraEnvVars, "PGC_"+strings.ToUpper(toSnakeCase(key)))
	defaultFromEnv := fromEnv(envVars)
	fromEnvOverride := defaultFromEnv != ""
	if fromEnvOverride {
		argConfig.defValue = defaultFromEnv
	} else if argConfig.defValue == nil {
		argConfig.defValue = ""
	}
	defaultValue, ok := argConfig.defValue.(string)
	if !ok {
		return "", fmt.Errorf(
			"requested argument %s is %s, but %v (%T) cannot be parsed to %T",
			key,
			argConfig.argType.String(),
			argConfig.defValue,
			argConfig.defValue,
			defaultValue,
		)
	}
	if argConfig.argType == typePath && !fromEnvOverride {
		defaultValue = filepath.Join(confDir, defaultValue)
	}
	return defaultValue, nil
}

func handleStringArrayCommandArg(key string, argConfig *arg) ([]string, error) {
	envVars := append(argConfig.extraEnvVars, "PGC_"+strings.ToUpper(toSnakeCase(key)))
	defaultFromEnv := fromEnv(envVars)
	if defaultFromEnv != "" {
		argConfig.defValue = strings.Split(defaultFromEnv, ",")
	} else if argConfig.defValue == nil {
		argConfig.defValue = []string{}
	}
	defaultValue, ok := argConfig.defValue.([]string)
	if !ok {
		return nil,
			fmt.Errorf(
				"requested argument %s is %s, but %v (%T) cannot be parsed to %T",
				key,
				argConfig.argType.String(),
				argConfig.defValue,
				argConfig.defValue,
				defaultValue,
			)
	}
	return defaultValue, nil
}

func handleBoolCommandArg(key string, argConfig *arg) (bool, error) {
	envVars := append(argConfig.extraEnvVars, "PGC_"+strings.ToUpper(toSnakeCase(key)))
	defaultFromEnv := fromEnv(envVars)
	if defaultFromEnv != "" {
		var err error
		argConfig.defValue, err = strconv.ParseBool(defaultFromEnv)
		if err != nil {
			return false, fmt.Errorf("default %s from environment is not a valid bool", defaultFromEnv)
		}
	} else if argConfig.defValue == nil {
		argConfig.defValue = false
	}
	defaultValue, ok := argConfig.defValue.(bool)
	if !ok {
		return false,
			fmt.Errorf(
				"requested argument %s is %s, but %v (%T) cannot be parsed to %T",
				key,
				argConfig.argType.String(),
				argConfig.defValue,
				argConfig.defValue,
				defaultValue,
			)
	}
	return defaultValue, nil
}

func (as args) commandArgs(command *cobra.Command, enabledArguments []string) (myArgs args) {
	myArgs = args{}
	for _, key := range enabledArguments {
		if _, exists := myArgs[key]; exists {
			continue
		}
		argConfig, exists := as[key]
		if !exists {
			panic(fmt.Sprintf("requested argument %s does not seem to exist", key))
		}
		switch argConfig.argType {
		case typeCount:
			argConfig.intValue = command.PersistentFlags().CountP(key, argConfig.short, argConfig.desc)
		case typeUInt:
			defaultValue, err := handleUintCommandArg(key, &argConfig)
			if err != nil {
				panic(err)
			}
			argConfig.uIntValue = command.PersistentFlags().UintP(key, argConfig.short, defaultValue,
				argConfig.desc)
		case typePath, typeString:
			defaultValue, err := handleStringCommandArg(key, &argConfig)
			if err != nil {
				panic(err)
			}
			argConfig.stringValue = command.PersistentFlags().StringP(key, argConfig.short, defaultValue,
				argConfig.desc)
		case typeStringArray:
			defaultValue, err := handleStringArrayCommandArg(key, &argConfig)
			if err != nil {
				panic(err)
			}
			argConfig.stringArrayValue = command.PersistentFlags().StringSliceP(key, argConfig.short, defaultValue,
				argConfig.desc)
		case typeBool:
			defaultValue, err := handleBoolCommandArg(key, &argConfig)
			if err != nil {
				panic(err)
			}
			argConfig.boolValue = command.PersistentFlags().BoolP(key, argConfig.short, defaultValue, argConfig.desc)
		}
		myArgs[key] = argConfig
	}
	return myArgs
}

func (as args) GetString(argument string) (value string) {
	arg, exists := as[argument]
	if !exists {
		panic(fmt.Sprintf("requesting %s, but it is not defined", argument))
	}
	switch arg.argType {
	case typePath:
		value = utils.ResolveHome(*arg.stringValue)
		return value
	case typeString:
		value = *arg.stringValue
		return value
	default:
		panic(fmt.Sprintf("requesting string value for %s, but it is not defined as such", argument))
	}
}

func (as args) GetStringSlice(argument string) (value []string) {
	arg, exists := as[argument]
	if !exists {
		panic(fmt.Sprintf("requesting %s, but it is not defined", argument))
	}
	switch arg.argType {
	case typeStringArray:
		value = *arg.stringArrayValue
		return value
	default:
		panic(fmt.Sprintf("requesting string value for %s, but it is not defined as such", argument))
	}
}

func (as args) GetInt(argument string) (value int) {
	arg, exists := as[argument]
	if !exists {
		panic(fmt.Sprintf("requesting %s, but it is not defined", argument))
	}
	if arg.argType != typeCount {
		panic(fmt.Sprintf("requesting int value for %s, but it is not defined as such", argument))
	}
	value = *arg.intValue
	return value
}

func (as args) GetUint(argument string) (value uint) {
	arg, exists := as[argument]
	if !exists {
		panic(fmt.Sprintf("requesting %s, but it is not defined", argument))
	}
	if arg.argType != typeUInt {
		panic(fmt.Sprintf("requesting uint value for %s, but it is not defined as such", argument))
	}
	value = *arg.uIntValue
	return value
}

func (as args) GetBool(argument string) (value bool) {
	arg, exists := as[argument]
	if !exists {
		panic(fmt.Sprintf("requesting %s, but it is not defined", argument))
	}
	if arg.argType != typeBool {
		panic(fmt.Sprintf("requesting bool value for %s, but it is not defined as such", argument))
	}
	value = *arg.boolValue
	return value
}

/*
func getHostName() string {
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostName
}
*/

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
