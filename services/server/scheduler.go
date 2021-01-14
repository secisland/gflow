package server

import "sort"

// 调度器功能：
// 1、管理所有worker节点负载状态；
// 2、管理所有任务运行的参数；
// 3、如果worker宕机，需要重新对任务进行分配worker节点并下发执行。
// 4、提供cli restful 接口，包括任务启动，停止，重启；


type WorkerLoadList []*Worker

func (this WorkerLoadList) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func (this WorkerLoadList) Len() int {
	return len(this)
}

func (this WorkerLoadList) Less(i, j int) bool {
	return (this[i].CPUFree < this[j].CPUFree) && this[i].MemFree < this[j].MemFree
}


// 调度算法
func Scheduler(insCount int, reqCpu,reqMem int64) []*Worker {
	var validWorkerList WorkerLoadList

	for _,v := range WorkerMap.GetWorders() {
		if (v.CPUFree >= reqCpu) && (v.MemFree >= reqMem) {
			validWorkerList = append(validWorkerList, v)
		}
	}
	if len(validWorkerList) < insCount {
		return []*Worker{}
	}

	sort.Sort(sort.Reverse(validWorkerList))
	return validWorkerList
}



