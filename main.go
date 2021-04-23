package main

import (
	"flag"
	"io/ioutil"
	"logical/core"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"logical/conf"
	"logical/log"
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
	var config conf.Config
	if err = yaml.Unmarshal(data, &config); err != nil {
		panic(err)
	}
	var logical = core.New(&config)
	if err = logical.Start(); err != nil {
		panic(err)
	}
	select {}
}
