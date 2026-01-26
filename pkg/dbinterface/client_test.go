package dbinterface

/*
import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgvillage-tools/dbtwool/pkg/pg"
)

var _ = Describe("Dbinterface", Ordered, func() {
	var (
		ctx context.Context
		cl  Client
		p   Pool
		c   Connection
		i   IsolationLevel
		err error
	)
	BeforeAll(func() {
		ctx = context.Background()
	})
	AfterAll(func() {
	})
	Context("when using pg", func() {
		It("should work properly", func() {
			cl = &pg.Client{}
			p, err = cl.Pool(ctx)
			Ω(err).NotTo(HaveOccurred())
			c, err = p.Connect(ctx)
			Ω(err).To(HaveOccurred())
			err = c.Begin(ctx)
			Ω(err).To(HaveOccurred())
			i = pg.ReadCommitted
			Ω(i.AsQuery()).To(Equal("SET TRANSACTION ISOLATION LEVEL"))
		})
	})
})
*/
