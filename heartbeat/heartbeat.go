package main

import (
	"os"

	"github.com/elastic/beats/v9/heartbeat/cmd"
	_ "github.com/elastic/beats/v9/heartbeat/include"
	_ "github.com/streamnative/pulsar-beat-output/pulsar"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
