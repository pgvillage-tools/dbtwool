package arguments

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

func handleUintCommandArg(key string, argConfig *Arg) (uint, error) {
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

func handleStringCommandArg(key string, argConfig *Arg) (string, error) {
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

func handleStringArrayCommandArg(key string, argConfig *Arg) ([]string, error) {
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

func handleBoolCommandArg(key string, argConfig *Arg) (bool, error) {
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
