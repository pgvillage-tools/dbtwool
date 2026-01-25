package db2client

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDb2client(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Db2client Suite")
}
