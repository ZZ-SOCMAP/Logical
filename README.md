# logical

[![golang](https://img.shields.io/badge/Language-Go-green.svg?style=flat)](https://golang.org)
[![GoDoc](https://godoc.org/github.com/yanmengfei/logical?status.svg)](https://godoc.org/github.com/yanmengfei/logical)
[![GitHub release](https://img.shields.io/github/release/yanmengfei/logical.svg)](https://github.com/yanmengfei/logical/releases)

logical is tool for synchronizing from PostgreSQL to custom handler through replication slot

## Required

Postgresql 10.0+


## Howto

### Download
Choose the file matching the destination platform from the [downloads page](https://github.com/yanmengfei/logical/releases), copy the URL and replace the URL within the commands below:

```shell
wget -O logical https://github.com/yanmengfei/logical/releases/download/v0.1.0/logical_linux_0.1.0
chmod +x logical
```

### Config file
```yaml
### capture config
capture:
  dump_path: '/usr/local/bin/pg_dump' # command path
  historical: false
  db_host: '127.0.0.1'
  db_port: 5432
  db_name: 'dbname'
  db_user: 'user'
  db_pass: 'password'
  slot_name: 'test_slot_for_localhost'
  tables: # capture tables
    - 'organization'

### log config
logger:
  size: 100       # max size (m)
  age: 7          # max age (d)
  level: info    # log level
  backup: 10      # max backup
  savepath: logs/logical.log # log save path

### upstream config
upstream:
  host: 'localhost:50049'
  timeout: 5 # connect timeout (s)
```

### Config PostgreSQL

1. change `postgresql.conf`
```
wal_level = 'logical';  # minimal, replica, or logical. postgres default replica, It determines how much information is written to the wal
max_replication_slots = 5; # max number of replication slots, The value should be greater than 1
```

2. change `pg_hba.conf`
```
# Add a new line below `replication`
host dbname user address md5 # example: `host units itcp 172.30.0.1/24 md5`
```

### Run
> You need to start the upstream service first, [example](./example)

```shell
./logical --config config.yaml
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
logical -c config.yaml
```

### Source code build
```shell
make clean
make install # Cross compiling: `make linux` or `make darwin`

#./logical_darwin_0.0.1 -c config.yaml
```
