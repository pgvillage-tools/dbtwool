package main

import (
	"fmt"
	"os"
	"path"
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
	typePath
	typeUnknown
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

var (
	typeToString = map[aType]string{
		typeCount:   "typeCount",
		typeUInt:    "typeUInt",
		typeBool:    "typeBool",
		typeString:  "typeString",
		typePath:    "typePath",
		typeUnknown: "typeUnknown",
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
	short        string
	desc         string
	extraEnvVars []string
	defValue     any
	argType      aType
	stringValue  *string
	uIntValue    *uint
	intValue     *int
	boolValue    *bool
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
		"cfgFile": {short: "c", defValue: "config.yaml", argType: typePath, desc: `config file`},
		"outFile": {short: "o", defValue: "-", argType: typePath, desc: `Send to output (- means stdout)`},
	}
)

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
		envVars := append(argConfig.extraEnvVars, "PGC_"+strings.ToUpper(toSnakeCase(key)))
		defaultFromEnv := fromEnv(envVars)
		switch argConfig.argType {
		case typeCount:
			// if defaultFromEnv != "" {
			// 	var err error
			// 	argConfig.defValue, err = strconv.Atoi(defaultFromEnv)
			// 	if err != nil {
			// 		panic(fmt.Sprintf("default %s from environment vars %v is not a valid int", defaultFromEnv, envVars))
			// 	}
			// } else if argConfig.defValue == nil {
			// 	argConfig.defValue = 0
			// }
			argConfig.intValue = command.PersistentFlags().CountP(key, argConfig.short, argConfig.desc)
		case typeUInt:
			if defaultFromEnv != "" {
				var err error
				argConfig.defValue, err = strconv.Atoi(defaultFromEnv)
				if err != nil {
					panic(fmt.Sprintf("default from environment (%v) is invalid as int", envVars))
				}
			} else if argConfig.defValue == nil {
				argConfig.defValue = 0
			}
			defaultValue, ok := argConfig.defValue.(int)
			if !ok {
				panic(
					fmt.Sprintf(
						"requested argument %s is %s, but %v (%T) cannot be parsed to %T",
						key,
						argConfig.argType.String(),
						argConfig.defValue,
						argConfig.defValue,
						defaultValue,
					))
			}
			argConfig.uIntValue = command.PersistentFlags().UintP(key, argConfig.short, uint(defaultValue), argConfig.desc)
		case typePath, typeString:
			if defaultFromEnv != "" {
				argConfig.defValue = defaultFromEnv
			} else if argConfig.defValue == nil {
				argConfig.defValue = ""
			}
			defaultValue, ok := argConfig.defValue.(string)
			if !ok {
				panic(
					fmt.Sprintf(
						"requested argument %s is %s, but %v (%T) cannot be parsed to %T",
						key,
						argConfig.argType.String(),
						argConfig.defValue,
						argConfig.defValue,
						defaultValue,
					))
			}
			if argConfig.argType == typePath {
				defaultValue = path.Join(confDir, defaultValue)
			}
			argConfig.stringValue = command.PersistentFlags().StringP(key, argConfig.short, defaultValue, argConfig.desc)
		case typeBool:
			if defaultFromEnv != "" {
				var err error
				argConfig.defValue, err = strconv.ParseBool(defaultFromEnv)
				if err != nil {
					panic(fmt.Sprintf("default %s from environment vars %v is not a valid bool", defaultFromEnv, envVars))
				}
			} else if argConfig.defValue == nil {
				argConfig.defValue = false
			}
			defaultValue, ok := argConfig.defValue.(bool)
			if !ok {
				panic(
					fmt.Sprintf(
						"requested argument %s is %s, but %v (%T) cannot be parsed to %T",
						key,
						argConfig.argType.String(),
						argConfig.defValue,
						argConfig.defValue,
						defaultValue,
					))
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
