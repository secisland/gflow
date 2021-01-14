package main

//  任务调度的时间复杂为O(n)
import (
	"encoding/json"
	"fmt"
	api "gflow/api/server"
	"gflow/services/protocol"
	"gflow/services/server"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
	download "gflow/services/protobuf/download"
)

func init() {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	viper.AddConfigPath(path)
	viper.SetConfigName("master")
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
}


func main() {
	//http.HandleFunc("/", api.GetFile)
	//http.HandleFunc("/run", api.RunTest)
	//http.HandleFunc("/stop", api.KillTest)
	//err := http.ListenAndServe(":8088" , nil)
	////err := http.ListenAndServe(":" + strconv.Itoa(*port), nil)
	//if nil != err{
	//	log.Fatalln("Get Dir Err", err.Error())
	//}

	interval := viper.GetDuration("worker.hearbeat_interval_sec")
	timeout := viper.GetDuration("worker.hearbeat_timeout_sec")
	grpc_addr := viper.GetString("master.grpc_addr")

	// restful API
	go func(){
		router := gin.Default()
		router.GET("/tasks", api.GetTasks)
		router.POST("/tasks", api.StartTask)
		router.POST("/tasks/:name", api.StopTask)
		router.POST("/download", api.DownloadTask)
		router.GET("/workers",api.GetWorks)
		router.GET("/printclusterinfo",api.PrintClusterInfo)
		router.Run(":5080")
	}()

	// gRPC服务
	go func(){
		// 限制发送大小 server := grpc.NewServer(grpc.MaxSendMsgSize())
		server := grpc.NewServer()
		download.RegisterDonwloadServer(server,&api.DonwloadServer{})
		lis, err:= net.Listen("tcp",grpc_addr)
		if err != nil {
			log.Fatalln("net.Listen err: %v",err)
		}
		fmt.Println("开始启动grpc服务器：")
		server.Serve(lis)
		fmt.Println("启动grpc服务器成功")
	}()


	// 心跳检测
	go func(){
		for{
			//fmt.Println("心跳检查开始")
			proto := &protocol.Protocol{}
			proto.Code = 1

			data := protocol.ProtocolDataHeatbeat{Host:"master",}
			dataByte, err := json.Marshal(data)
			if err != nil {
				log.Println("json序列化失败：", err.Error())
				return
			}
			proto.Data = dataByte

			for _,worker := range server.WorkerMap.GetWorders() {
				go func(){
					worker.Send(proto)
					t := time.NewTicker(timeout* time.Second)
					defer t.Stop()
					select {
					case <-t.C:
						fmt.Println("Worker心跳超时,删除节点:",worker.Host)
						_ = worker.Conn.Close()
						server.WorkerMap.Delete(worker.Host)
					case <-worker.HeatBeatChan:
						fmt.Println("Worker心跳正常:",worker.Host,
							worker.CPUTotal,
							worker.CPUFree,
							worker.MemTotal,
							worker.MemFree,
							worker.StartAt,
							worker.UpdateAt,
							worker.TaskRange)
					}
				}()
			}
			time.Sleep(interval* time.Second)
		}
	}()


	// tcp服务
	addr := viper.GetString("master.tcp_addr")
	var tcpAddr *net.TCPAddr
	//通过ResolveTCPAddr实例一个具体的tcp端点
	tcpAddr, _ = net.ResolveTCPAddr("tcp", addr)
	//打开一个tcp端点监听
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Println("tcp监听端口异常，程序退出！"+addr)
		return
	}
	defer tcpListener.Close()
	log.Println("Start master ... ")

	//循环接收客户端的连接，创建一个协程具体去处理连接
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			log.Println(err)
			continue
		}
		log.Println("A client connected["+ addr +"] :" + tcpConn.RemoteAddr().String())
		go server.WorkerHandle(tcpConn)
	}

}
