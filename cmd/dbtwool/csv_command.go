package main

import (
	"github.com/spf13/cobra"
)

func csvCommand() *cobra.Command {
	var csvArgs args
	cycleCommand := &cobra.Command{
		Use:   "csv",
		Short: "cycle encryption key and file",
		Long:  `Use this command to generate a new key/file from previous key/file.`,
		Run: func(_ *cobra.Command, _ []string) {
			outFile := csvArgs.GetString("outFile")
			if outFile == "" {
				log.Panic("parameter outFile is mandatory for csv")
			}
		},
	}

	csvArgs = allArgs.commandArgs(cycleCommand, append(globalArgs,
		"shred",
	))
	return cycleCommand
}
