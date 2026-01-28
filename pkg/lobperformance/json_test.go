package lobperformance

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Json", func() {
	Context("Generate", func() {
		const (
			chunkSize = uint(10)
			lastValue = 1000
		)
		It("should work as expected", func() {
			o := JObj{
				ID:    1,
				Name:  "something",
				Items: []string{"item1", "item2"},
			}
			s, err := o.String()
			Ω(err).NotTo(HaveOccurred())
			Ω(s).To(Equal(`{"id":1,"name":"something","items":["item1","item2"]}`))
		})
		It("should fit to size as requested", func() {
			// Fibonacci till 1000000
			var (
				i = 0
				j = 1
			)
			for {
				i, j = j, i+j
				if i > lastValue {
					break
				}
				if i < int(minJSize+chunkSize) {
					continue
				}
				o := newGeneratedJObj(chunkSize, uint(i))
				s, err := o.String()
				Ω(err).NotTo(HaveOccurred())
				var l = len(s)
				spread := l / 30
				Ω(l).To(BeNumerically("~", i, spread))
			}
		})
	})
})
