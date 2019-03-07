package cmd

import (
	"fmt"

	"github.com/petems/cobra"
)

// Version is set by main during build
const Version = "0.1.0"

// GitCommit is set by main during build
var GitCommit string

// versionCmd represents the version command
func VersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show the version",
		Long:  ``,
		RunE:  runVersionCmd,
	}

	return cmd
}

func runVersionCmd(cmd *cobra.Command, args []string) error {

	if GitCommit == "" {
		GitCommit = "<unknown commit>"
	}

	versionOutputString := fmt.Sprint("s3mini " + GitCommit + "\n" + "Version: " + Version + "\n")

	fmt.Fprint(cmd.OutOrStdout(), versionOutputString)

	return nil
}

func init() {
	rootCmd.AddCommand(VersionCommand())
}
