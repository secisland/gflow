# 一、功能
- 支持分布式任务管理，包括任务下发、启停控制等，任务调度保证各节点负载均衡；
- 各节点支持cpu和内存资源限额配置，保证同一计算节点上的不同任务进程互不抢占资源；
- 各节点内置内存缓存数据库；

```
本项目只是一个基本框架的demon，时间关系并没有经过实际的分布式流式计算的验证，待完善。
```
# 二、架构

## 1、master
- 集群资源汇总：节点资源和任务状态信息维护；
- 节点心跳检测；
- 通过http rest接口提交任务，分配节点资源，通过tcp协议在对应的的worker节点上管理任务；
- reduce分布式计算时，提供分布式任务状态一致性约束；
- reduce分布式计算时,提供节点归并调度（两个节点做一次reduce计算，重复直到剩下最后一个节点完成任务reduce）；

## 2、worker
- 维护自身的任务跟踪器和节点资源信息；
- 内置缓存数据库（CDB），并提供grpc接口，实现add/set/get/delete操作；数据格式参考：gflow/services/protocol.Topology

# 三、使用

## 1、protobuf相关命令示例：
```
cd services/protobuf/cdb
protoc -I ./ cdb.proto --go_out=plugins=grpc:.
```

## 2、支持分布式流式计算任务mapreduce（集成go-streams），并将计算中间结果保存到本地缓存中，通过master分式式任务状态协调机制和reduce归并调度实现。（gflow/api/client/cdb.go cdb 文件中的grpc客户端代码，可以连同gflow/services/profobuf/cdb/cdb.proto.go文件一起拷贝到 go-stream，实现外部任务进程可以grpc访问cdb数据库；）

## 3、master http rest API示例：
```
cluster状态查询：
    curl --request GET 'http://192.168.56.101:5080/printclusterinfo'

cluster worker状态查询：
    curl --request GET 'http://192.168.56.101:5080/workers'

cluster task状态查询：
    curl --request GET 'http://192.168.56.101:5080/tasks'

download:
    curl --request POST 'http://192.168.56.101:5080/download?name=test&cmd=/home/a.sh&count=1&cpuquota=50000&memlimit=500000000&filename=/home/tmp'

start:
    curl --request POST 'http://192.168.56.101:5080/tasks?name=test&cmd=/home/a.sh&count=1&cpuquota=50000&memlimit=500000000&filename=/data/filebeat-7.8.0-linux-x86_64.tar.gz'

stop:
    curl --request POST 'http://192.168.56.101:5080/tasks/test'
```

