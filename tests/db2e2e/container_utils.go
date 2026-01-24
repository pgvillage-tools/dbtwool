// Package db2e2e_test runs end-to-end tests for dbtwool
package db2e2e_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func containerLogs(ctx context.Context, cnt testcontainers.Container) (string, error) {
	reader, err := cnt.Logs(ctx)
	if err != nil {
		return "", err
	}

	// Lees alles uit naar een string of print het
	buf := new(strings.Builder)
	_, _ = io.Copy(buf, reader)
	return buf.String(), nil
}

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
					Dockerfile:     "Dockerfile",
					BuildLogWriter: os.Stdout,
				},
			},
			Started: true,
		})
}

func runDB2(
	ctx context.Context,
	nw *testcontainers.DockerNetwork,
	aliasses map[string][]string,
	env map[string]string,
) (testcontainers.Container, error) {
	/*
	   environment:
	*/
	env["LICENSE"] = "accept"

	const image = "icr.io/db2_community/db2"
	fmt.Printf("starting %s\n", image)
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				ImagePlatform:      "linux/amd64",
				Image:              image,
				HostConfigModifier: func(hc *container.HostConfig) { hc.Privileged = true },
				Env:                env,
				Networks:           []string{nw.Name},
				NetworkAliases:     aliasses,
				WaitingFor: wait.ForLog(".*Setup has completed.*").
					AsRegexp().
					WithStartupTimeout(10 * time.Minute),
			},
			Started: true,
		})
}
