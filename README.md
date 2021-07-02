# logical

[![golang](https://img.shields.io/badge/Language-Go-green.svg?style=flat)](https://golang.org)
[![GitHub release](https://img.shields.io/github/release/yanmengfei/logical.svg)](https://github.com/yanmengfei/logical/releases)

logical is tool for synchronizing from PostgreSQL to custom handler through replication slot

## Required

Postgresql 10.0+

## Howto

### Design

![img.png](docs/design.png)

### TODO

- [x] elasticsearch
- [x] RabbitMQ
- [ ] Kafka
- [ ] MongoDB
- [ ] Restful API

### Download

Choose the file matching the destination platform from
the [downloads page](https://github.com/yanmengfei/logical/releases), copy the URL and replace the URL within the
commands below:

```shell
wget -O logical https://github.com/yanmengfei/logical/releases/download/v0.1.0/logical_linux_0.1.0
chmod +x logical
```

### Config file

```toml
# capture config
[capture]
dump_path = "/usr/local/bin/pg_dump" # pg_dump command path
historical = true # dump historical data

# capture database
[capture.database]
host = "127.0.0.1"
port = 5432
name = "asset_intel"
username = "itcp"
password = "123456"


# capture tables
[[capture.tables]]
name = "task" # table name
pk = "id" # primary key field name
fields = ["title", "value"]  # output fields
slotname = "slot_localhost_todo_task" # replication slot name, unique
outputs = ["elasticsearch"] # outputs

# capture tables
[[capture.tables]]
name = "user"
pk = "id"
fields = ["name", "other"]
slotname = "slot_localhost_todo_user"
outputs = ["rabbitmq", "elasticsearch"]

# logs
[logger]
maxsize = 100 # max log file size
maxage = 7 # log retention days
backup = 10 # old log files
level = "debug" # log level
path = "logs/logical.log" # save path

[output.elasticsearch]
index = "iowork"
username = "elastic"
password = "password"
hosts = ["http://127.0.0.1:9200"]

[output.rabbitmq]
url = "amqp://admin:123456@127.0.0.1:5672/tasks"
queue = "test"

[output.kafka]
hosts = ["127.0.0.1:9092"]
topic = "tasks"

[output.restapi]
url = "http://127.0.0.1:8888/todo" # post / json

[output.mongo]
url = "mongodb://127.0.0.1:27017"
```

### Config PostgreSQL

1. change `postgresql.conf`

```
wal_level = 'logical';  # minimal, replica, or logical. postgres default replica, It determines how much information is written to the wal
max_replication_slots = 10; # max number of replication slots, The value should be greater than 1
```

2. change `pg_hba.conf`

```
# Add a new line below `replication`, $ is a variable
host $dbname $user $address md5  # example: `host todo itcp 172.30.0.1/24 md5`
```

### Run

```shell
./logical --config config.toml
```

## Develop

requires Go 1.13 or greater.

### Download

```shell
git clone https://github.com/yanmengfei/logical.git
cd logical
```

### Source code running

```shell
go install
logical -c config.toml
```

### Source code build

```shell
make clean
make install # Cross compiling: `make linux` or `make darwin`
./logical_darwin_x.x.x -c config.yaml
```
