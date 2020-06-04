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
	log "github.com/sirupsen/logrus"
	"github.com/toolkits/net/httplib"
)

var (
	COUTER_PAGE_MAX = 100000
)

type MetricValue struct {
	Endpoint  string      `json:"endpoint"`
	Metric    string      `json:"metric"`
	Value     interface{} `json:"value"`
	Step      int64       `json:"step"`
	Type      string      `json:"counterType"`
	Tags      string      `json:"tags"`
	Timestamp int64       `json:"timestamp"`
}

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

	return
}

func IsCounterInWhiteList(counter string) bool {
	cfg := g.Config()
	for _, v := range cfg.WhiteList.MetricsPrefix {
		if strings.HasPrefix(counter, v) {
			return true
		}
	}
	return false
}

func getEndpointCounters(ep *EndpointInfo, page int) (resp []string, err error) {
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

type transferResponseData struct {
	Message string `json:"message"`
	Total   int    `json:"Total"`
	Invalid int    `json:"Invalid"`
	Latency int64  `json:"Latency"`
}
type transferResponse struct {
	Msg  string                `json:"msg"`
	Data *transferResponseData `json:"data"`
}

func sendCounterDataToTransfer(mvs []*MetricValue) (err error) {
	cfg := g.Config()

	if len(mvs) <= 0 {
		return nil
	}
	ep := mvs[0].Endpoint
	metric := mvs[0].Metric
	tag := mvs[0].Tags

	var req *httplib.BeegoHttpRequest
	headers := map[string]string{"Content-type": "application/json"}
	req, err = CurlPlus(cfg.To.PushApi, "POST", "falcon-data-reentry", cfg.From.PlusApiToken,
		headers, map[string]string{})
	if err != nil {
		return
	}

	req.SetTimeout(time.Duration(cfg.To.ConnTimeout)*time.Millisecond,
		time.Duration(cfg.To.RequTimeout)*time.Millisecond)

	b, err := json.Marshal(mvs)
	if err != nil {
		log.Errorf("json.Marshal error: %v, endpoint:%s metric:%s tag:%s",
			err, ep, metric, tag)
		return
	}

	req.Body(b)

	resp := transferResponse{}
	err = req.ToJson(&resp)
	if err != nil {
		log.Errorf("getTrasferResponse ToJson error, err %v, endpoint:%s metric:%s tag:%s",
			err, ep, metric, tag)
		return
	}

	if resp.Msg != "success" {
		log.Errorf("sent2transfer return error %v,  endpoint:%s metric:%s tag:%s",
			resp, ep, metric, tag)
	}

	return
}

func ProcessReentryCounterData(ep *EndpointInfo, counter string) (err error) {
	cfg := g.Config()
	batch := cfg.Batch
	step := 60
	begintm, _ := time.Parse(time.RFC3339, cfg.From.BeginTime)
	endtm, _ := time.Parse(time.RFC3339, cfg.From.EndTime)

	hostnames := []string{ep.Endpoint}
	counters := []string{counter}

	err, data := getGrouphHistory(hostnames, counters, step, begintm.Unix(), endtm.Unix())
	if err != nil {
		log.Errorf("failed to get counter data: %s %s err:%v",
			ep.Endpoint, counter, err)
		return
	}

	for _, cv := range data {
		mvs := []*MetricValue{}
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
				m := MetricValue{
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
					err = sendCounterDataToTransfer(mvs)
					if err != nil {
						log.Errorf("send2transfer error: %s %s", ep.Endpoint, cv.Counter)
					}
					mvs = mvs[:0]
				}
			}
		}
		if len(mvs) > 0 {
			err = sendCounterDataToTransfer(mvs)
			if err != nil {
				log.Errorf("send2transfer error: %s %s", ep.Endpoint, cv.Counter)
			}
			mvs = mvs[:0]
		}
	}

	return
}

func ProcessEndpoint(ep *EndpointInfo) (total, succ, fail int, err error) {
	cfg := g.Config()
	total = 0
	succ = 0
	fail = 0

	// get counters
	log.Infof("process endpoint: %s id: %v", ep.Endpoint, ep.Id)
	pagenum := COUTER_PAGE_MAX
	for p := 1; pagenum < COUTER_PAGE_MAX; p++ {
		counters, errs := getEndpointCounters(ep, p)
		if errs != nil {
			log.Errorf("get counters failed, endpoint:%s id:%v , err%v",
				ep.Endpoint, ep.Id, errs)
			return total, succ, fail, errs
		}

		if cfg.WhiteList.Enabled {
			tCounters := []string{}
			for _, v := range counters {
				if IsCounterInWhiteList(v) {
					tCounters = append(tCounters, v)
				}
			}
			counters = counters[:0]
			counters = append(counters, tCounters...)
		}

		pagenum = len(counters)
		total = total + pagenum

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
	log.Infof("[worker:%v] running, endpoints: %v", wkId, len(L))

	for i, v := range L {
		total, succ, fail, err := ProcessEndpoint(v)
		if err != nil {
			log.Errorf("[worker:%v] process endpoint[%v]: %s id: %v total: %v succ: %v fail: %v, err: %v",
				wkId, i, v.Endpoint, v.Id, total, succ, fail, err)
		} else {
			log.Infof("[worker:%v] process endpoint[%v]: %s id: %v total: %v succ: %v fail: %v ",
				wkId, i, v.Endpoint, v.Id, total, succ, fail)
		}
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

	log.Infoln("load all endpoints now...")
	err, eps := getEndpoints()
	if err != nil {
		log.Errorf("load endpoints error, %v", err)
		return
	}
	epNum := len(eps)
	log.Infoln("load endpoints: %v \n %v", epNum, eps)

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
			go ReentryWorker(wkQueues[i], i)
		}
	}
}
