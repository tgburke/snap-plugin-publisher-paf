package main

import (
	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin"
	"github.com/tgburke/snap-plugin-publisher-paf/pafdb"
)

func main() {
	plugin.StartPublisher(pafdb.New(), pafdb.Name, pafdb.Version)
}
