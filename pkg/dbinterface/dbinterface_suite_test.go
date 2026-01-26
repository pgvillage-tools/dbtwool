package dbinterface

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDbinterface(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Dbinterface Suite")
}
