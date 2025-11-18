package version_test

import (
	"testing"

	"github.com/pgvillage-tools/dbtwool/internal/version"

	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	assert.NotEmpty(t, version.GetAppVersion())
	assert.Regexp(t, `v(\d+\.)?(\d+\.)?(\*|\d+)$`, version.GetAppVersion(), "AppVersion should follow convention")
}
