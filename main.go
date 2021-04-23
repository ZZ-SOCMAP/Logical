package main

import (
	"flag"
	"io/ioutil"

	"logical/conf"
	"logical/log"
	"logical/river"

	"github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

var configfile = flag.String("config", "", "config")
var loglevel = flag.String("level", "debug", "log level")

func main() {
	flag.Parse()

	lv, err := logrus.ParseLevel(*loglevel)
	if err != nil {
		panic(err)
	}
	log.Logger.SetLevel(lv)

	data, err := ioutil.ReadFile(*configfile)
	if err != nil {
		panic(err)
	}

	var config conf.Conf
	if err := jsoniter.Unmarshal(data, &config); err != nil {
		panic(err)
	}

	amazon := river.New(&config)
	amazon.Start()

	// block forever
	select {}
}
