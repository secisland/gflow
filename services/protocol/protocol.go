package protocol

import (
	"gflow/storage/partition"
	"time"
)

//命令下发或数据上报
type Protocol struct {
	//协议头
	Host string		`json:"host" binding:"required"`		// 主机名
	Code int  		`json:"code" binding:"required"`		//  1:心跳; 2:任务管理; 3:文件下载；

	// 数据
	Data []byte		`json:"data" binding:"required"`		// 具体数据,内容是json序列化的格式
}

type ProtocolDataTask struct{
	Code int 		`json:"code" binding:"required"`				// 1:启动; 2:停止; 3:下载
	Name string		`json:"name" binding:"required"`				// 任务名称
	Count int 		`json:"count" binding:"required"`				// task实例数量
	Cmd []string	`json:"cmd" binding:"required"`					// 任务启动参数
	CpuQuota int64	`json:"cpuquota,omitempty" binding:"required"`	// CPU限额
	MemLimit int64	`json:"memlimit,omitempty" binding:"required"`	// 内存限额
	Success bool	`json:"success" binding:"required"`				// 执行是否成功
	Message string	`json:"message,omitempty" binding:"required"`	// 执行结果消息/
}

type ProtocolDataHeatbeat struct{
	Host string			`json:"host" binding:"required"`
	TaskRange []string	`json:"taskrange,omitempty" binding:"required"`
	CPUTotal int64		`json:"cputotal,omitempty" binding:"required"`
	CPUFree int64		`json:"cpufree,omitempty" binding:"required"`
	MemTotal int64		`json:"memtotal,omitempty" binding:"required"`
	MemFree int64		`json:"memfree,omitempty" binding:"required"`
}

// CDB
type Topology struct{
	TopologyName string
	Page map[string]partition.Segment // key
	Version string
	LifeSpan time.Duration
}
