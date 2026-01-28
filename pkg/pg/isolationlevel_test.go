package pg_test

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/pgvillage-tools/dbtwool/pkg/pg"
)

var _ = ginkgo.Describe("IsolationLevel", func() {
	ginkgo.Describe("AsString", func() {
		ginkgo.It("should return the correct string for ReadCommitted", func() {
			level := pg.ReadCommitted
			Expect(level.AsString()).To(Equal("READ COMMITTED"))
		})

		ginkgo.It("should return the correct string for RepeatableRead", func() {
			level := pg.RepeatableRead
			Expect(level.AsString()).To(Equal("REPEATABLE READ"))
		})

		ginkgo.It("should return the correct string for Serializable", func() {
			level := pg.Serializable
			Expect(level.AsString()).To(Equal("SERIALIZABLE"))
		})
	})

	ginkgo.Describe("AsQuery", func() {
		ginkgo.It("should return the correct query for ReadCommitted", func() {
			level := pg.ReadCommitted
			Expect(level.AsQuery()).To(Equal("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
		})

		ginkgo.It("should return the correct query for RepeatableRead", func() {
			level := pg.RepeatableRead
			Expect(level.AsQuery()).To(Equal("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
		})

		ginkgo.It("should return the correct query for Serializable", func() {
			level := pg.Serializable
			Expect(level.AsQuery()).To(Equal("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
		})
	})

	ginkgo.Describe("GetIsolationLevel", func() {
		ginkgo.It("should return ReadCommitted for 0", func() {
			Expect(pg.GetIsolationLevel(0)).To(Equal(pg.ReadCommitted))
		})

		ginkgo.It("should return RepeatableRead for 1", func() {
			Expect(pg.GetIsolationLevel(1)).To(Equal(pg.RepeatableRead))
		})

		ginkgo.It("should return Serializable for 2", func() {
			Expect(pg.GetIsolationLevel(2)).To(Equal(pg.Serializable))
		})

		ginkgo.It("should return a value for a higher index", func() {
			level := pg.GetIsolationLevel(3)
			Expect(level.AsString()).To(Equal(""))
			Expect(level.AsQuery()).To(Equal("SET TRANSACTION ISOLATION LEVEL "))
		})
	})
})
