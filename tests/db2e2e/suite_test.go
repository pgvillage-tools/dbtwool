package db2e2e_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEtcdv2(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DB2 tests")
}
