// Copyright 2020 CloudMinds
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

package funcs

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lxlee1102/falcon-data-reentry/g"
	"github.com/open-falcon/falcon-plus/common/model"
	log "github.com/sirupsen/logrus"
	"github.com/toolkits/net/httplib"
)

var (
	COUTER_PAGE_MAX = 100000
)

type GraphHistoryReq struct {
	Step      int      `json:"step"`
	StartTime int64    `json:"start_time"`
	Hostnames []string `json:"hostnames"`
	EndTime   int64    `json:"end_time"`
	Counters  []string `json:"counters"`
	ConsolFun string   `json:"consol_fun"`
}

type historyValues struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type GraphHistoryResp struct {
	Endpoint string           `json:"endpoint"`
	Counter  string           `json:"counter"`
	Dstype   string           `json:"dstype"`
	Step     int              `json:"step"`
	Values   []*historyValues `json:"Values"`
}

type CounterResp struct {
	Counter    string `json:"counter"`
	EndpointId int64  `json:"endpoint_id"`
	Step       int    `json:"step"`
	Tyep       string `json:"type"`
}

func CurlPlus(uri, method, token_name, token_sig string, headers, params map[string]string) (req *httplib.BeegoHttpRequest, err error) {
	if method == "GET" {
		req = httplib.Get(uri)
	} else if method == "POST" {
		req = httplib.Post(uri)
	} else if method == "PUT" {
		req = httplib.Put(uri)
	} else if method == "DELETE" {
		req = httplib.Delete(uri)
	} else if method == "HEAD" {
		req = httplib.Head(uri)
	} else {
		err = errors.New("invalid http method")
		return
	}

	req = req.SetTimeout(1*time.Second, 5*time.Second)

	token, _ := json.Marshal(map[string]string{
		"name": token_name,
		"sig":  token_sig,
	})
	req.Header("Apitoken", string(token))

	for hk, hv := range headers {
		req.Header(hk, hv)
	}

	for pk, pv := range params {
		req.Param(pk, pv)
	}

	return
}

type EndpointInfo struct {
	Endpoint string `json:"endpoint"`
	Id       int64  `json:"id"`
}

func IsEndpointInWhiteList(ep string) bool {
	cfg := g.Config()
	for _, v := range cfg.WhiteList.EndpointPrefix {
		if v == ".+" {
			return true
		}
		if strings.HasPrefix(ep, v) {
			return true
		}
	}
	return false
}

func getEndpoints() (err error, L []*EndpointInfo) {
	cfg := g.Config()
	uri := fmt.Sprintf("%s/api/v1/graph/endpoint", cfg.From.PlusApi)

	var req *httplib.BeegoHttpRequest
	headers := map[string]string{"Content-type": "application/json"}
	params := map[string]string{"q": ".+", "limit": "999999999"}
	req, err = CurlPlus(uri, "GET", "data-reentry-app", cfg.From.PlusApiToken,
		headers, params)
	if err != nil {
		return
	}

	req.SetTimeout(time.Duration(cfg.From.ConnTimeout)*time.Millisecond,
		time.Duration(cfg.From.RequTimeout)*time.Millisecond)

	err = req.ToJson(&L)
	if err != nil {
		return
	}

	if cfg.WhiteList.Enabled {
		L_t := []*EndpointInfo{}
		for _, v := range L {
			if IsEndpointInWhiteList(v.Endpoint) {
				L_t = append(L_t, v)
			}
		}
		L = L[:0]
		L = append(L, L_t...)
	}

	return
}

func IsCounterInWhiteList(counter string) bool {
	cfg := g.Config()
	for _, v := range cfg.WhiteList.MetricsPrefix {
		if v == ".+" {
			return true
		}
		if strings.HasPrefix(counter, v) {
			return true
		}
	}
	return false
}

func getEndpointCounters(ep *EndpointInfo, page int) (resp []*CounterResp, err error) {
	cfg := g.Config()
	uri := fmt.Sprintf("%s/api/v1/graph/endpoint_counter", cfg.From.PlusApi)

	headers := map[string]string{"Content-type": "application/json"}
	param := map[string]string{
		"eid":   strconv.FormatInt(ep.Id, 10),
		"page":  strconv.Itoa(page),
		"limit": strconv.Itoa(COUTER_PAGE_MAX),
	}
	var req *httplib.BeegoHttpRequest
	req, err = CurlPlus(uri, "GET", "falcon-data-reentry", cfg.From.PlusApiToken,
		headers, param)
	if err != nil {
		return
	}

	req.SetTimeout(time.Duration(cfg.From.ConnTimeout)*time.Millisecond,
		time.Duration(cfg.From.RequTimeout)*time.Millisecond)

	err = req.ToJson(&resp)
	if err != nil {
		return
	}

	return resp, nil
}

func getGrouphHistory(hostnames []string, counter []string, step int, startTime, endTime int64) (err error, L []*GraphHistoryResp) {
	cfg := g.Config()
	uri := fmt.Sprintf("%s/api/v1/graph/history", cfg.From.PlusApi)

	var req *httplib.BeegoHttpRequest
	headers := map[string]string{"Content-type": "application/json"}
	req, err = CurlPlus(uri, "POST", "falcon-data-reentry", cfg.From.PlusApiToken,
		headers, map[string]string{})

	if err != nil {
		return
	}

	req.SetTimeout(time.Duration(cfg.From.ConnTimeout)*time.Millisecond,
		time.Duration(cfg.From.RequTimeout)*time.Millisecond)

	body := GraphHistoryReq{
		Step:      step,
		StartTime: startTime,
		Hostnames: hostnames,
		EndTime:   endTime,
		Counters:  counter,
		ConsolFun: "AVERAGE",
	}

	b, err := json.Marshal(body)
	if err != nil {
		return
	}

	req.Body(b)

	err = req.ToJson(&L)
	if err != nil {
		return
	}

	return nil, L
}

func send2transfer(metrics []*model.MetricValue) (succ, fail, invalid, latency uint64) {
	succ = 0
	fail = 0
	var resp model.TransferResponse

	g.SendMetrics(metrics, &resp)

	if resp.Message == "ok" {
		succ = uint64(resp.Total)
	} else {
		fail = uint64(resp.Total)
	}

	invalid = uint64(resp.Invalid)
	latency = uint64(resp.Latency)

	return
}

func ProcessReentryCounterData(ep *EndpointInfo, counter *CounterResp) (err error) {
	cfg := g.Config()
	batch := cfg.Batch
	step := 60
	begintm, _ := time.Parse(time.RFC3339, cfg.From.BeginTime)
	endtm, _ := time.Parse(time.RFC3339, cfg.From.EndTime)

	hostnames := []string{ep.Endpoint}
	counters := []string{counter.Counter}
	err, data := getGrouphHistory(hostnames, counters, step, begintm.Unix(), endtm.Unix())
	if err != nil {
		log.Errorf("process-counter: [%s] %s, err:%v", ep.Endpoint, counter.Counter, err)
		return
	}

	var succ uint64 = 0
	var fail uint64 = 0
	var invalid uint64 = 0
	var latency uint64 = 0
	var total uint64 = 0

	for _, cv := range data {
		total += uint64(len(cv.Values))
		mvs := []*model.MetricValue{}
		var metric, tags string
		ss := strings.Split(cv.Counter, "/")
		if len(ss) >= 2 {
			metric = ss[0]
			tags = ss[1]
		} else {
			metric = cv.Counter
			tags = ""
		}

		for _, vv := range cv.Values {
			if !math.IsNaN(float64(vv.Value)) {
				m := model.MetricValue{
					Endpoint:  cv.Endpoint,
					Metric:    metric,
					Value:     vv.Value,
					Step:      int64(cv.Step),
					Type:      cv.Dstype,
					Tags:      tags,
					Timestamp: vv.Timestamp,
				}
				mvs = append(mvs, &m)

				if len(mvs) >= batch {
					time.Sleep(time.Duration(cfg.Interval) * time.Millisecond)
					s, f, iv, lat := send2transfer(mvs)
					succ += s
					fail += f
					invalid += iv
					latency += lat
					mvs = mvs[:0]
				}
			} else {
				invalid++
			}
		}
		if len(mvs) > 0 {
			time.Sleep(time.Duration(cfg.Interval) * time.Millisecond)
			s, f, iv, lat := send2transfer(mvs)
			succ += s
			fail += f
			invalid += iv
			latency += lat
			mvs = mvs[:0]
		}
	}

	log.Debugf("process-counter: [%s] %s , points total: %v succ: %v fail: %v invalid: %v latency: %v",
		ep.Endpoint, counter.Counter, total, succ, fail, invalid, latency)

	return
}

func ProcessEndpoint(ep *EndpointInfo) (total, succ, fail int, err error) {
	cfg := g.Config()
	total = 0
	succ = 0
	fail = 0

	// get counters
	pagenum := COUTER_PAGE_MAX
	for p := 1; pagenum == COUTER_PAGE_MAX; p++ {
		counters, errs := getEndpointCounters(ep, p)
		if errs != nil {
			log.Errorf("load-counters [%s] id: %v err:%v",
				ep.Endpoint, ep.Id, errs)
			return total, succ, fail, errs
		}
		pagenum = len(counters)

		if cfg.WhiteList.Enabled {
			tCounters := []*CounterResp{}
			for _, v := range counters {
				if IsCounterInWhiteList(v.Counter) {
					tCounters = append(tCounters, v)
				}
			}
			counters = counters[:0]
			counters = append(counters, tCounters...)
		}

		total = total + len(counters)
		log.Debugf("load-counters [%s] page: %d number: %d ",
			ep.Endpoint, p, total)

		// reenter each counters data
		for _, v := range counters {
			err := ProcessReentryCounterData(ep, v)
			if err != nil {
				fail++
			} else {
				succ++
			}
		}
	}

	return total, succ, fail, nil
}

func ReentryWorker(L []*EndpointInfo, wkId int) {
	b, _ := json.Marshal(L)
	log.Infof("[worker:%v] running, assign endpoints: %v, %v", wkId, len(L), string(b))

	for i, v := range L {
		log.Debugf("[worker:%v] process endpoint[%v]: %s id: %v",
			wkId, i, v.Endpoint, v.Id)

		total, succ, fail, _ := ProcessEndpoint(v)

		log.Infof("[worker:%v] process endpoint[%v]: %s id: %v total: %v succ: %v fail: %v ",
			wkId, i, v.Endpoint, v.Id, total, succ, fail)
	}

	log.Infof("[worker:%v] exit.", wkId)
}

func WorkRun() {
	cfg := g.Config()
	_, err := time.Parse(time.RFC3339, cfg.From.BeginTime)
	if err != nil {
		log.Errorf("time parse: %v, %v, need RFC3399, like 2020-05-27T00:00:00Z",
			cfg.From.BeginTime, err)
		return
	}
	_, err = time.Parse(time.RFC3339, cfg.From.EndTime)
	if err != nil {
		log.Errorf("time parse: %v, %v, need RFC3399, like 2020-05-27T00:00:00Z",
			cfg.From.BeginTime, err)
		return
	}
	if cfg.WhiteList.Enabled {
		log.Infoln("WhiteList is enabled.")
	}

	log.Infof("reentry data time(UTC): %s - %s", cfg.From.BeginTime, cfg.From.EndTime)

	err, eps := getEndpoints()
	if err != nil {
		log.Errorf("load endpoints error, %v", err)
		return
	}
	epNum := len(eps)
	b, _ := json.Marshal(eps)
	log.Infof("load endpoints: %v, %v", epNum, string(b))

	if cfg.Workers <= 1 {
		go ReentryWorker(eps, 0)
	} else {
		wkQueues := make([][]*EndpointInfo, cfg.Workers)
		for i := 0; i < cfg.Workers; i++ {
			wkQueues[i] = []*EndpointInfo{}
		}

		for i, v := range eps {
			idx := i % cfg.Workers
			wkQueues[idx] = append(wkQueues[idx], v)
		}

		for i := 0; i < cfg.Workers; i++ {
			time.Sleep(time.Duration(cfg.Interval/int64(cfg.Workers)) * time.Millisecond)
			go ReentryWorker(wkQueues[i], i)
		}
	}
}
