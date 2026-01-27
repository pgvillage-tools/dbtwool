// Package pge2e_test runs end-to-end tests for dbtwool
package pge2e_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func containerLogs(ctx context.Context, cnt testcontainers.Container) (string, error) {
	reader, err := cnt.Logs(ctx)
	if err != nil {
		return "", err
	}

	buf := new(strings.Builder)
	_, _ = io.Copy(buf, reader)
	return buf.String(), nil
}

/*
func debugContainerLogs(ctx context.Context, cnt testcontainers.Container) error {
	logs, err := containerLogs(ctx, cnt)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(logs, "\n") {
		fmt.Fprintf(GinkgoWriter, "DEBUG - Containerlogs: %s", line)
	}
	return nil
}
*/

func runDbwTool(
	ctx context.Context,
	nw *testcontainers.DockerNetwork,
	env map[string]string,
	command ...string,
) (testcontainers.Container, error) {
	fmt.Printf("running dbwtool with '%s'\n", strings.Join(command, " "))
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd:      command,
				Env:      env,
				Networks: []string{nw.Name},
				FromDockerfile: testcontainers.FromDockerfile{
					Context:        "../../",
					Dockerfile:     "Dockerfile.pg",
					BuildLogWriter: os.Stdout,
				},
				WaitingFor: &wait.ExitStrategy{},
			},
			Started: true,
		})
}

func runPostgres(
	ctx context.Context,
	nw *testcontainers.DockerNetwork,
	aliasses map[string][]string,
	settings map[string]string,
) (testcontainers.Container, error) {
	pgVersion := os.Getenv("PGVERSION")
	if pgVersion == "" {
		pgVersion = "18"
	}
	image := fmt.Sprintf("docker.io/postgres:%s", pgVersion)
	fmt.Printf("starting %s\n", image)
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:          image,
				Env:            settings,
				Networks:       []string{nw.Name},
				NetworkAliases: aliasses,
				WaitingFor: wait.ForLog(
					"database system is ready to accept connections"),
			},
			Started: true,
		})
}
