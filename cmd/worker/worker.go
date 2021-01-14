package main

import (
	"encoding/json"
	"fmt"
	api "gflow/api/client"
	"gflow/services/client"
	"gflow/services/protobuf/cdb"
	"gflow/services/protobuf/download"
	"gflow/services/protocol"
	"gflow/storage/partition"
	"gflow/util"
	"net"
	"os"
	"runtime"
	"syscall"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"time"
)

// TODO: worker宕机 task依然在运行，可以在worker重启后扫描 cgroup 子节点检查task,如果task running就自动添加到任务管理器中



func init() {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	viper.AddConfigPath(path)
	viper.SetConfigName("worker")
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
}

//func InitGrpcCLient() (err error){
//	if api.DownloadRpcClientConn != nil {
//		_ = api.DownloadRpcClientConn.Close()
//	}
//
//	for {
//		api.DownloadRpcClientConn, err = grpc.Dial(viper.GetString("master.grpc_addr"), grpc.WithInSecure())
//		if err != nil {
//			log.Println("服务器grpc连接失败：" + err.Error() + "尝试重连...")
//			time.Sleep(10 * time.Second)
//			return false
//		} else {
//			log.Println("服务器grpc连接成功！")
//		}
//	}
//}



func main() {
	// 初始化
	//util.InitCGroup()
	//api.InitTaskTracker()

	timeout := viper.GetDuration("master.hearbeat_retry_timeout_sec")

	cpuQuota := viper.GetInt64("worker.cpu_quota_ms")
	memLimit := viper.GetInt64("worker.mem_limit_byte")

	dataPath := viper.GetString("worker.data_path")
	grpc_addr := viper.GetString("worker.grpc_addr")

	//cpuQuota := int64(0)
	//memLimit := int64(2147483648)

	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	cpuNum := runtime.NumCPU()

	sysInfo := new(syscall.Sysinfo_t)
	err = syscall.Sysinfo(sysInfo)
	if err != nil {
		fmt.Println("获取内存信息失败：",err.Error())
		os.Exit(1)
	}

	if int64(sysInfo.Totalram) < memLimit {
		memLimit = int64(sysInfo.Totalram)
	}
	fmt.Println("总内存：", sysInfo.Totalram, "内存限制byte：",memLimit)

	if int64(cpuNum * 100000) < cpuQuota {
		cpuQuota = int64(cpuNum * 100000)
	}
	fmt.Println("CPU核心数：",cpuNum,"CPU限额：",cpuQuota)

	if ! util.InitCgroup(cpuQuota,memLimit) {
		os.Exit(1)
	}

	partition.InitCDB()
	// gRPC cdb服务端
	go func(){
		// 限制发送大小 server := grpc.NewServer(grpc.MaxSendMsgSize())
		server := grpc.NewServer()
		cdb.RegisterCDBServer(server,&api.CDB{})
		lis, err:= net.Listen("tcp",grpc_addr)
		if err != nil {
			log.Fatalln("net.Listen err: %v",err)
		}
		fmt.Println("开始启动grpc服务器！")
		server.Serve(lis)
		fmt.Println("启动grpc服务器成功！")
	}()


	time.Sleep(3 * time.Second)
	// grpc 客户端
	gconn,err := grpc.Dial(viper.GetString("master.grpc_addr"), grpc.WithInsecure())
	if err != nil {
		log.Println("服务器grpc连接失败：" + err.Error() + "尝试重连...")
	} else {
		log.Println("服务器grpc连接成功！")
	}

	defer gconn.Close()
	api.DownloadStreamClient = download.NewDonwloadClient(gconn)



	// test cdb grpc client
	top := protocol.Topology{Page:make(map[string]partition.Segment)}
	top.TopologyName = "test"
	top.Version = "0.0.1"
	top.LifeSpan = 10 * time.Minute

	seg := make(map[string]*partition.Aggregator)
	seg["svc1"]= &partition.Aggregator{Count:10,Data:map[string]int{"/test1":3,"/test2":8}}
	seg["svc2"]= &partition.Aggregator{Count:30,Data:map[string]int{"/hello":12,"/world":18}}
	top.Page["qps"] = seg
	top.Page["top10"] = seg

	_ = api.CDBSet(viper.GetString("worker.grpc_addr"),top)


	t := api.CDBGet(viper.GetString("worker.grpc_addr"),"test","0.0.1")
	fmt.Println("worker grpc.get 数据：",t,t.Page["qps"]["svc1"].Data,t.Page["qps"]["svc2"].Data)

	// 初始化任务管理器
	client.InitTaskTracker()

	for {
		conn, err := client.CreateConn(viper.GetString("master.tcp_addr"))

		if err != nil {
			log.Println("服务器tcp连接失败：" + err.Error() + "尝试重连...")
			time.Sleep(10 * time.Second)
			continue
		} else {
			log.Println("服务器tcp连接成功！")
		}

		client.WorkerInfo = &client.Worker{
			Master:conn,
			Valid:true,
			Control:util.CGroup,
			Host:host,
			StartAt:time.Now(),
			CPUTotal:cpuQuota,
			CPUFree:cpuQuota,
			MemTotal:memLimit,
			MemFree:memLimit,
			DataPath: dataPath,
		}

		hbdata := protocol.ProtocolDataHeatbeat{
			Host: host,
			TaskRange:[]string{},
			CPUTotal:cpuQuota,
			CPUFree:cpuQuota,
			MemTotal:memLimit,
			MemFree:memLimit,
		}

		d, err := json.Marshal(hbdata)
		if err != nil {
			log.Println("json序列化失败：", err.Error())
			panic(err)
		}

		req := protocol.Protocol{Host:host,Code:1,Data:d}
		// 新建网络连接后，发送worker状态信息心跳包
		client.Send(req)

		// 循环接收处理master下发的数据包
		// TODO: 任务超时没有做任务处理；
		for{
			if client.Recv(client.WorkerInfo.Master,host) == false {
				client.WorkerInfo.Valid = false
				fmt.Println("服务器tcp连接已断开，尝试重连...")
				time.Sleep(timeout*time.Second)
				break
			}
		}
	}
}



