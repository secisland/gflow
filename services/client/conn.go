package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	api "gflow/api/client"
	"gflow/services/protocol"
	"github.com/containerd/cgroups"
	"io"
	"log"
	"net"
	"time"
)


var WorkerInfo *Worker

type Worker struct {
	Master *net.TCPConn
	Valid bool	// 与master socket连接是否可用
	Control cgroups.Cgroup
	Host string
	TaskRange []string
	CPUTotal int64
	CPUFree int64
	MemTotal int64
	MemFree int64
	StartAt time.Time
	DataPath string
}

func CreateConn(addr string) (conn *net.TCPConn, err error) {
	var tcpAddr *net.TCPAddr
	tcpAddr, _ = net.ResolveTCPAddr("tcp", addr)

	conn, err = net.DialTCP("tcp", nil, tcpAddr)
	return
}

func Send(req protocol.Protocol) bool {
	if !WorkerInfo.Valid{
		fmt.Println("与master连接已断开，发送数据失败")
		return false
	}
	b, err := json.Marshal(&req)
	if err != nil {
		fmt.Println("json序列化失败：", err.Error())
		return false
	}

	//发送调度请求
	buf := make([]byte, 2+len(b))
	binary.BigEndian.PutUint16(buf[:2], uint16(2+len(b)))
	copy(buf[2:], b[:])

	_, err = WorkerInfo.Master.Write(buf)
	if err != nil {
		log.Println("发送数据失败：" + err.Error())
		WorkerInfo.Valid = false
		_ = WorkerInfo.Master.Close()
		log.Println("断开连接: " + WorkerInfo.Master.LocalAddr().String())
		return false
	}
	fmt.Println("已发送数据 =>", WorkerInfo.Master.LocalAddr().String())
	return true

	//if Recv(ServerConn.Conn) == true {
	//	return true
	//} else {
	//	ServerConn.Valid = false
	//	_ = ServerConn.Conn.Close()
	//	return false
	//}
}


//接收响应
func Recv(conn *net.TCPConn, host string) bool {
	// 读取包长度
	b := make([]byte, 2)
	_, err := WorkerInfo.Master.Read(b[0:])
	if err != nil {
		fmt.Println(WorkerInfo.Master.RemoteAddr().String()+" 读取包长度错误： ", err)
		return false
	}

	// 大端转换
	length := binary.BigEndian.Uint16(b[:])
	buf := make([]byte, length-2)
	if _, err := io.ReadFull(WorkerInfo.Master, buf); err != nil {
		log.Println(WorkerInfo.Master.RemoteAddr().String()+" 大端转换错误： ", err)
		return false
	}

	//解码
	var req protocol.Protocol
	if err := json.Unmarshal(buf, &req); err != nil {
		log.Println("网络数据包解析异常! " + WorkerInfo.Master.RemoteAddr().String() + ", " + err.Error())
		return false
	}

	switch req.Code {
	// 心跳包
	case 1:
		hbdata := protocol.ProtocolDataHeatbeat{
			Host: host,
			TaskRange: WorkerInfo.TaskRange,
			CPUTotal: WorkerInfo.CPUTotal,
			CPUFree: WorkerInfo.CPUFree,
			MemTotal: WorkerInfo.MemTotal,
			MemFree: WorkerInfo.MemFree,
		}

		d, err := json.Marshal(hbdata)
		if err != nil {
			log.Println("json序列化失败：", err.Error())
			return false
		}

		req.Host = host
		req.Data = d
		// 接收服务端心跳检测包，并响应worker状态信息心跳包
		Send(req)
	// 任务管理
	case 2:
		var task protocol.ProtocolDataTask
		if err := json.Unmarshal(req.Data, &task); err != nil {
			log.Println("任务解码异常： " + WorkerInfo.Master.RemoteAddr().String() + ", " + err.Error())
			return false
		}

		fmt.Println("开始执行任务！")
		// 需要区分task Code: 1代表新建任务； 2代表终卡任务；
		switch task.Code {
		case 1:
			fmt.Println("开始新建任务: ",task.Name)
			exe ,err := NewExecutor(task.Name, task.Cmd[0], task.CpuQuota,task.MemLimit,task.Cmd[0:]...)
			if err != nil {
				task.Success = false

				d, err := json.Marshal(task)
				if err != nil {
					log.Println("json序列化失败：", err.Error())
					return false
				}
				req.Data = d
				fmt.Println("响应新建任务结果:未执行!")
				Send(req)
			} else {
				go func(){
					go exe.Run()

					timeout := time.NewTicker(30*time.Second)
					defer timeout.Stop()

					select {
					case done := <-exe.DoneChan:
						if done{
							task.Success = true
							WorkerInfo.CPUFree = WorkerInfo.CPUFree - task.CpuQuota
							WorkerInfo.MemFree =  WorkerInfo.MemFree - task.MemLimit
						} else {
							task.Success = false
						}
					case <-timeout.C:
						task.Success = false
						log.Println("新建任务超时：",task.Name)
					}

					d, err := json.Marshal(task)
					if err != nil {
						log.Println("json序列化失败：", err.Error())
						panic(err)
					}
					req.Data = d
					fmt.Println("响应新建任务结果：",task.Name,task.Success)
					Send(req)
				}()
			}
		case 2:
			fmt.Println("开始终止任务: ",task.Name)
			if ok, t := hasTask(task.Name);ok{
				go func(){
					go t.Stop()
					timeout := time.NewTicker(30*time.Second)
					defer timeout.Stop()

					select {
					case done := <-t.DoneChan:
						if done{
							task.Success = true
							WorkerInfo.CPUFree = WorkerInfo.CPUFree + task.CpuQuota
							WorkerInfo.MemFree =  WorkerInfo.MemFree + task.MemLimit
						} else {
							task.Success = false
						}
					case <-timeout.C:
						task.Success = false
						log.Println("停止任务超时：",task.Name)
					}
					d, err := json.Marshal(task)
					if err != nil {
						log.Println("json序列化失败：", err.Error())
						panic(err)
					}
					req.Data = d
					log.Println("响应终止任务结果: ",task.Name,task.Success)
					Send(req)
				}()
			} else {
				task.Success = false
				log.Println("任务不存在，终止任务失败: ",task.Name)
				d, err := json.Marshal(task)
				if err != nil {
					log.Println("json序列化失败：", err.Error())
					return false
				}
				req.Data = d
				log.Println("响应终止任务结果: ",task.Name,task.Success)
				Send(req)
			}
		}
	// 下载文件
	case 3:
		var task protocol.ProtocolDataTask
		if err := json.Unmarshal(req.Data, &task); err != nil {
			log.Println("data error! " + conn.RemoteAddr().String() + ", " + err.Error())
			return false
		}
		req.Host = WorkerInfo.Host
		filename :=task.Message
		ctx,_:=context.WithCancel(context.Background())
		if api.DownLoad(ctx,filename,WorkerInfo.DataPath) {
			task.Success = true
		}else {
			task.Success = false
		}

		d, err := json.Marshal(task)
		if err != nil {
			log.Println("json序列化失败：", err.Error())
			return false
		}
		req.Data = d
		Send(req)
	}
	return true
}

