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
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("s3mini " + GitCommit)
		fmt.Println("Version: " + Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
