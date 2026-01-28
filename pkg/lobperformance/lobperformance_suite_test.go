package lobperformance_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLobperformance(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lobperformance Suite")
}
