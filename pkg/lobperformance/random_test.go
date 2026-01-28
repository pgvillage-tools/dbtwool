package lobperformance

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Random", func() {
	const (
		lastValue = 10000
	)
	Context("GenerateString", func() {
		It("should work as expected", func() {
			// Fibonacci
			var (
				i uint = 0
				j uint = 1
			)
			for {
				i, j = j, i+j
				if i > lastValue {
					break
				}
				s := randomString(i)
				Ω(s).To(HaveLen(int(i)))
			}
		})
	})
})
