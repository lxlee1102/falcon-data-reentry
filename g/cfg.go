// Copyright 2020 CloudMinds, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package g

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/toolkits/file"
)

type FromConfig struct {
	BeginTime    string `json:"UTC_begin_time"`
	EndTime      string `json:"UTC_end_time"`
	ConnTimeout  int    `json:"connect_timeout"`
	RequTimeout  int    `json:"request_timeout"`
	PlusApi      string `json:"plus_api"`
	PlusApiToken string `json:"plus_api_token"`
}

type TransferConfig struct {
	Enabled  bool     `json:"enabled"`
	Addrs    []string `json:"addrs"`
	Interval int      `json:"interval"`
	Timeout  int      `json:"timeout"`
}

type WhiteListConfig struct {
	Enabled        bool     `json:"enabled"`
	MetricsPrefix  []string `json:"metric_prefix"`
	EndpointPrefix []string `json:"endpoint_prefix"`
}

type GlobalConfig struct {
	Debug     bool             `json:"debug"`
	Workers   int              `json:"workers"`
	Batch     int              `json:"batch"`
	Interval  int64            `json:"batch_interval_ms"`
	MaxFiles  uint64           `json:"max_filefds"`
	From      *FromConfig      `json:"from"`
	Transfer  *TransferConfig  `json:"transfer"`
	WhiteList *WhiteListConfig `json:"whitelist"`
}

var (
	ConfigFile string
	config     *GlobalConfig
	lock       = new(sync.RWMutex)
)

func Config() *GlobalConfig {
	lock.RLock()
	defer lock.RUnlock()
	return config
}

func ParseConfig(cfg string) {
	if cfg == "" {
		log.Fatalln("use -c to specify configuration file")
	}

	if !file.IsExist(cfg) {
		log.Fatalln("config file:", cfg, "is not existent. maybe you need `mv cfg.example.json cfg.json`")
	}

	ConfigFile = cfg

	configContent, err := file.ToTrimString(cfg)
	if err != nil {
		log.Fatalln("read config file:", cfg, "fail:", err)
	}

	var c GlobalConfig
	err = json.Unmarshal([]byte(configContent), &c)
	if err != nil {
		log.Fatalln("parse config file:", cfg, "fail:", err)
	}

	lock.Lock()
	defer lock.Unlock()

	config = &c

	log.Println("read config file:", cfg, "successfully")
}
