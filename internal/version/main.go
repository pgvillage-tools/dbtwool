// Package version is there so we can inject the version from the building process
package version

var appVersion = "v0.0.0"

// GetAppVersion can be used to retrieve the version, which will be injected by the build process
func GetAppVersion() string {
	return appVersion
}
