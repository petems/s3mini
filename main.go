package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/inconshreveable/mousetrap"

	"github.com/petems/s3mini/cmd"
)

var (
	gitcommit = "-dev"
)

func init() {
	if runtime.GOOS == "windows" {
		if mousetrap.StartedByExplorer() {
			fmt.Println("Don't double-click s3mini!")
			fmt.Println("You need to open cmd.exe/powershell and run it from the command line!")
			time.Sleep(5 * time.Second)
			os.Exit(1)
		}
	}
}

func main() {
	log.SetFlags(log.Lshortfile)
	cmd.GitCommit = gitcommit
	cmd.Execute()
}
