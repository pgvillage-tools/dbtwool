package arguments

import (
	"fmt"

	"github.com/pgvillage-tools/dbtwool/pkg/utils"
	"github.com/spf13/cobra"
)

// Arg represents a specific argument
type Arg struct {
	short            string
	desc             string
	extraEnvVars     []string
	defValue         any
	argType          Type
	stringValue      *string
	stringArrayValue *[]string
	uIntValue        *uint
	intValue         *int
	boolValue        *bool
}

// Args represents a list of Arg values
type Args map[string]Arg

// CommandArgs can be used to manage all args for a specific command
func (as Args) CommandArgs(command *cobra.Command, enabledArguments []string) (myArgs Args) {
	myArgs = Args{}
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

// GetString returns the string value of an argument
func (as Args) GetString(argument string) (value string) {
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

// GetStringSlice returns a slice of the string values set for an argument
func (as Args) GetStringSlice(argument string) (value []string) {
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

// GetInt returns the int value of an argument
func (as Args) GetInt(argument string) (value int) {
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

// GetUint returns the uint value of an argument
func (as Args) GetUint(argument string) (value uint) {
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

// GetBool returns the string value of an argument
func (as Args) GetBool(argument string) (value bool) {
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
