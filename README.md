falcon-data-reentry
===

This is a falcon data points reentry tools, if your data accuracy too high to jammed browser, you can reset falcon's graph data accuracy and reenter old data by it.


## Installation

It is a golang classic project

```bash
# set $GOPATH and $GOROOT
mkdir -p $GOPATH/src/github.com/lxlee1102
cd $GOPATH/src/github.com/lxlee1102
git clone https://github.com/lxlee1102/falcon-data-reentry.git
cd falcon-data-reentry/
go get
./control build
./control start
```

## Configuration

```bash
{
    "debug": true,
    "workers": 2,                    #并发，基于endpoint分配，当endpoint为1时，并发没有意义
    "batch": 200,
    "batch_interval_ms": 200,        #每个work的发送周期，单位 ms，时间太短可能导致迁移丢数据，与硬盘速度有关
    "max_filefds": 100000,
    "from": {
        "UTC_begin_time": "2020-05-27T00:00:00Z",  # 获取数据开时时间，UTC
        "UTC_end_time": "2020-06-03T00:00:00Z",    # 获取数据结束时间，UTC
        "connect_timeout": 5000,
        "request_timeout": 5000,
        "plus_api": "http://falcon-api:8080",   # 等迁移环境中，falcon 的API监听地址
        "plus_api_token": "default-token-used-in-server-side"
    },
    "transfer": {                         # 写入新环境的 falcon transfer 地址
        "enabled": true,
        "addrs": [ "10.13.33.42:8433" ],
        "interval": 60,
        "timeout": 5000
    },
    "whitelist" : {                     # 白名单机制， .+ 表示全匹配，其它以前缀方式匹配
        "enabled": false,
        "metric_prefix": [".+","mcs."],
        "endpoint_prefix": [".+"]
    }
}
```
