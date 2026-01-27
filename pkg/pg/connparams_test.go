package pg_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/pgvillage-tools/dbtwool/pkg/pg"
)

var _ = Describe("ConnParams", func() {
	Describe("GetConnString", func() {
		It("should return a correctly formatted connection string", func() {
			params := pg.ConnParams{
				Host:     "myhost",
				Port:     "5432",
				Database: "mydb",
				User:     "myuser",
				Password: "mypassword",
				SslMode:  "disable",
			}
			expected := "host=myhost port=5432 dbname=mydb user=myuser password=mypassword sslmode=disable"
			Expect(params.GetConnString()).To(Equal(expected))
		})
	})

	Describe("ConnParamsFromEnv", func() {
		var originalEnv map[string]string

		BeforeEach(func() {
			originalEnv = make(map[string]string)
			vars := []string{"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"}
			for _, v := range vars {
				originalEnv[v] = os.Getenv(v)
				os.Unsetenv(v)
			}
		})

		AfterEach(func() {
			for k, v := range originalEnv {
				os.Setenv(k, v)
			}
		})

		It("should use default values when no env vars are set", func() {
			params := pg.ConnParamsFromEnv()
			Expect(params.Host).To(Equal("localhost"))
			Expect(params.Port).To(Equal("5432"))
			Expect(params.Database).To(Equal("postgres"))
			Expect(params.User).To(Equal("postgres"))
			Expect(params.Password).To(Equal("postgres"))
			Expect(params.SslMode).To(Equal("disable"))
		})

		It("should use values from env vars when they are set", func() {
			os.Setenv("PGHOST", "testhost")
			os.Setenv("PGPORT", "1234")
			os.Setenv("PGDATABASE", "testdb")
			os.Setenv("PGUSER", "testuser")
			os.Setenv("PGPASSWORD", "testpass")

			params := pg.ConnParamsFromEnv()
			Expect(params.Host).To(Equal("testhost"))
			Expect(params.Port).To(Equal("1234"))
			Expect(params.Database).To(Equal("testdb"))
			Expect(params.User).To(Equal("testuser"))
			Expect(params.Password).To(Equal("testpass"))
			Expect(params.SslMode).To(Equal("disable"))
		})
	})
})
