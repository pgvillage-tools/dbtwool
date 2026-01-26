// Package arguments manages all of the commandline arguments for dbtwool.
// As we have more then one rdbms each having thei own binary, we have moved
// this to an internal module instead of copying everything over to multiple cmd folders
package arguments

import (
	"os"
	"regexp"
	"strings"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

const (
	confDir = "~/.dbtwool"
)

func fromEnv(keys []string) string {
	for _, key := range keys {
		fromEnv := os.Getenv(key)
		if fromEnv != "" {
			return fromEnv
		}
	}
	return ""
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
