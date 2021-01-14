package server

import (
	"encoding/binary"
	"encoding/json"
	"gflow/services/protocol"
	"io"
	"log"
	"net"
	"time"
)

type Worker struct {
	Conn *net.TCPConn
	Host string
	TaskRange []string  // 任务名
	CPUTotal int64
	CPUFree int64
	MemTotal int64
	MemFree int64
	StartAt time.Time
	UpdateAt time.Time
	HeatBeatChan   chan int
	TaskChan chan protocol.ProtocolDataTask // 用于远程任务执行进度消息同步
}

func (this *Worker) Init() bool {
	if this.Host != "" {
		if ok,_ := WorkerMap.Get(this.Host);ok {
			return false
		}
		WorkerMap.Set(this.Host, this)
		return true
	} else {
		return false
	}
	//ipStr := this.Conn.RemoteAddr().String()
	////ip := strings.Split(ipStr,":")[0]
	//
	////if exist,_:= HostMap.Get(ip);exist {
	////	HostMap.Set(ip,this.Conn)
	////}
	//HostMap.Set(ipStr,this)
}

func (this *Worker) Update() bool {
	if this.Host != "" {
		if ok,_ := WorkerMap.Get(this.Host);ok {
			WorkerMap.Update(this.Host, this)
			return true
		}
		return false
	} else {
		return false
	}
}

func (this *Worker) Destroy() {
	if this.Host != "" {
		WorkerMap.Delete(this.Host)
	}
	//ipStr := this.Conn.RemoteAddr().String()
	////ip := strings.Split(ipStr,":")[0]
	//HostMap.Delete(ipStr)
}

func (this *Worker) Send(req *protocol.Protocol) bool {
	b, err := json.Marshal(req)
	if err != nil {
		log.Println("json序列化失败：", err.Error())
		return false
	}

	//发送命令
	buf := make([]byte, 2+len(b))
	binary.BigEndian.PutUint16(buf[:2], uint16(2+len(b)))
	copy(buf[2:], b[:])

	_, err = this.Conn.Write(buf)
	if err != nil {
		log.Println("发送命令失败：" + err.Error())
		return false
	}
	//log.Println("已发送命令, Host: ", req.Host," ,Code: ",req.Code)
	return true
}



func WorkerHandle(conn *net.TCPConn) {
	worker := &Worker{Conn: conn,
		HeatBeatChan: make(chan int),
		TaskChan: make(chan protocol.ProtocolDataTask),
	}

	// close connection before exit
	defer func() {
		err := conn.Close()
		worker.Destroy()
		if err != nil {
			log.Println("客户端断开异常~~~~")
		}

		log.Println("客户端断开:" + conn.RemoteAddr().String())
	}()

	initConn := true
	for {
		// 读取包长度
		b := make([]byte, 2)
		_, err := conn.Read(b[0:])
		if err != nil {
			log.Println(conn.RemoteAddr().String()+" 读取包长度错误： ", err)
			return
		}

		// 大端转换
		length := binary.BigEndian.Uint16(b[:])
		buf := make([]byte, length-2)
		if _, err := io.ReadFull(conn, buf); err != nil {
			log.Println(conn.RemoteAddr().String()+" 大端转换错误： ", err)
			return
		}

		//解码
		var req protocol.Protocol
		if err := json.Unmarshal(buf, &req); err != nil {
			log.Println("data error! " + conn.RemoteAddr().String() + ", " + err.Error())
			return
		}

		switch req.Code {
		case 1:
			var hb protocol.ProtocolDataHeatbeat
			if err := json.Unmarshal(req.Data, &hb); err != nil {
				log.Println("data error! " + conn.RemoteAddr().String() + ", " + err.Error())
				return
			}
			// 处理worker心跳上报的数据
			worker.Host = hb.Host
			now := time.Now()
			worker.UpdateAt = now

			if initConn {
				// worker状态由master保证数据准确性，第一次新建客户端连接时获取客户端信息，后续worker负载信息由master管理
				worker.CPUTotal = hb.CPUTotal
				worker.CPUFree = hb.CPUFree
				worker.MemTotal = hb.MemTotal
				worker.MemFree = hb.MemFree
				worker.TaskRange = hb.TaskRange

				worker.StartAt = now
				ok := worker.Init()

				if ok {
					initConn = false
					log.Println("节点初始化连接成功！=> ", req.Host)
				} else {
					log.Println("节点初始化连接失败: Host不能为空或Host已存在！=> ", req.Host)
					conn.Close()
				}
			} else {
				worker.Update()
			}
			// 完成接收心跳数据通知
			worker.HeatBeatChan<- 1
		case 2:
			var task protocol.ProtocolDataTask
			if err := json.Unmarshal(req.Data, &task); err != nil {
				log.Println("data error! " + conn.RemoteAddr().String() + ", " + err.Error())
				return
			}
			// 处理worker任务管理结果数据
			worker.TaskChan <- task
		case 3:
			var task protocol.ProtocolDataTask
			if err := json.Unmarshal(req.Data, &task); err != nil {
				log.Println("data error! " + conn.RemoteAddr().String() + ", " + err.Error())
				return
			}
			// 处理worker任务管理结果数据
			worker.TaskChan <- task
		}
	}
}
