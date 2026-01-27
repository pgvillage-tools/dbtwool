package pg_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pg Suite")
}
