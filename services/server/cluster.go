package server

import (
	"fmt"
	"log"
	"math"
	"regexp"
	"sync"
	"time"
)

var WorkerMap = &SafeWorkerMap{M: make(map[string](*Worker))}

type SafeWorkerMap struct {
	sync.RWMutex
	M map[string]*Worker
}


func (this *SafeWorkerMap) Get(key string) (bool, *Worker) {
	this.RLock()
	defer this.RUnlock()
	conn, ok := this.M[key]
	return ok, conn
}

func (this *SafeWorkerMap) GetWorders() (ret map[string](*Worker)) {
	this.RLock()
	defer this.RUnlock()

	ret = make(map[string](*Worker))
	for k, v := range this.M {
		ret[k] = v
	}
	return ret
}

func (this *SafeWorkerMap) Set(key string, worker *Worker) {
	this.Lock()
	defer this.Unlock()
	this.M[key] = worker
}


func (this *SafeWorkerMap) Update(key string, worker *Worker) {
	this.Lock()
	defer this.Unlock()
	this.M[key].TaskRange = worker.TaskRange
	this.M[key].CPUTotal = worker.CPUTotal
	this.M[key].CPUFree = worker.CPUFree
	this.M[key].MemTotal = worker.MemTotal
	this.M[key].MemFree = worker.MemFree
	this.M[key].UpdateAt = worker.UpdateAt
}

func (this *SafeWorkerMap) Delete(key string) {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.M[key]; ok {
		delete(this.M, key)
	}
}

func (this *SafeWorkerMap) Filter(regStr string) (ret map[string]*Worker) {
	this.RLock()
	defer this.RUnlock()
	ret = make(map[string]*Worker)
	reg, _ := regexp.Compile(regStr)

	for k, v := range this.M {
		fmt.Println("Filter inner func: ", k, regStr)
		b := reg.MatchString(k)
		if b {
			ret[k] = v
		}
	}
	return ret
}


var TaskMap = &SafeTaskMap{M: make(map[string](*Task))}

type SafeTaskMap struct {
	sync.RWMutex
	M map[string]*Task
}

func (this *SafeTaskMap) Get(key string) (bool, *Task) {
	this.RLock()
	defer this.RUnlock()
	task, ok := this.M[key]
	return ok, task
}

func (this *SafeTaskMap) GetTasks() (ret map[string](*Task)) {
	this.RLock()
	defer this.RUnlock()

	ret = make(map[string](*Task))
	for k, v := range this.M {
		ret[k] = v
	}
	return ret
}

func (this *SafeTaskMap) Set(key string, task *Task) {
	this.Lock()
	defer this.Unlock()
	this.M[key] = task
}


func (this *SafeTaskMap) Update(key string, task *Task) {
	this.Lock()
	defer this.Unlock()
	this.M[key].CMD = task.CMD
	this.M[key].NodeRange = task.NodeRange
	this.M[key].CPUQuato = task.CPUQuato
	this.M[key].MemLimit = task.MemLimit
	this.M[key].Count = task.Count
	this.M[key].LogRotateMax = task.LogRotateMax
	this.M[key].LogExpired = task.LogExpired
	this.M[key].UpdateAt = task.UpdateAt
	this.M[key].State = task.State	// Ready| Running | Stop | Leak
}

func (this *SafeTaskMap) Delete(key string) {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.M[key]; ok {
		delete(this.M, key)
	}
}

func (this *SafeTaskMap) Filter(regStr string) (ret map[string]*Task) {
	this.RLock()
	defer this.RUnlock()
	ret = make(map[string]*Task)
	reg, _ := regexp.Compile(regStr)

	for k, v := range this.M {
		fmt.Println("Filter inner func: ", k, regStr)
		b := reg.MatchString(k)
		if b {
			ret[k] = v
		}
	}
	return ret
}


type Task struct {
	sync.RWMutex
	Name string
	FilePath string // 可执行文件路径
	CMD []string
	NodeRange []string
	CPUQuato int64
	MemLimit int64
	Count int	// 实例数
	LogRotateMax int
	LogExpired time.Duration
	CreateAt time.Time
	UpdateAt time.Time
	State string	// NotReady（没有下载完）| Ready（已下载）| Running（运行中）| Stoping（停止中） | Leak（节点状态不完全一致）
}


func (this *Task) Init() bool {
	if this.Name != "" {
		if ok,_ := TaskMap.Get(this.Name);ok {
			return false
		}
		TaskMap.Set(this.Name, this)
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

func (this *Task) Update() bool {
	this.Lock()
	defer this.Unlock()

	if this.Name != "" {
		if ok,_ := TaskMap.Get(this.Name);ok {
			TaskMap.Update(this.Name, this)
			return true
		}
		return false
	} else {
		return false
	}
}

func (this *Task) AddNode(name string) bool {
	this.Lock()
	defer this.Unlock()

	this.NodeRange = append(this.NodeRange,name)
	return true
}

func (this *Task) RemoveNode(name string) bool {
	this.Lock()
	defer this.Unlock()

	if len(this.NodeRange) == 0 {
		return true
	}

	index := -1
	for i,n := range this.NodeRange {
		if n == name {
			index = i
			break
		}
	}

	if index < 0 {
		return true
	} else if index == 0  {
		this.NodeRange = this.NodeRange[index+1:]
	} else if index == len(this.NodeRange) -1 {
		this.NodeRange = this.NodeRange[:index]
	} else {
		this.NodeRange = append(this.NodeRange[:index],this.NodeRange[index+1:]...)
	}
	return true
}

func (this *Task) Destroy() {
	if this.Name != "" {
		TaskMap.Delete(this.Name)
	}
	//ipStr := this.Conn.RemoteAddr().String()
	////ip := strings.Split(ipStr,":")[0]
	//HostMap.Delete(ipStr)
}

// 查询集群整体概况
func PrintClusterInfo() (all []string){
	msg := "====== Resource Stats ====== "
	log.Println(msg)
	all = append(all,msg)
	cpuTotal,cpuFree,memTotal,memFree := ClusterResourceStats()
	if (cpuTotal !=0) || (memTotal != 0){
		msg = fmt.Sprintf("CpuTotal: %v, CpuFree: %d%% , MemTotal: %d, MemFree: %d%%",
			cpuTotal,
			int(math.Floor((float64(cpuFree)/float64(cpuTotal))*100 + 0/5)),
			memTotal,
			int(math.Floor((float64(memFree)/float64(memTotal))*100 + 0/5)),
		)
		log.Println(msg)
		all = append(all,msg)

	} else {
		msg = "获取集群资源数据为空！"
		log.Println(msg)
		all = append(all,msg)
	}

	msg = "====== Task List ====== "
	log.Println(msg)
	all = append(all,msg)
	for k,v := range TaskMap.GetTasks() {
		msg = fmt.Sprintf("Name: %v, State: %v, Count: %d, CreateAt: %v, CPUQuota: %d, MemLimit: %d",k,v.State,v.Count,v.CreateAt,v.CPUQuato,v.MemLimit)
		log.Println(msg)
		all = append(all,msg)
	}

	msg = "====== Worker List ====== "
	log.Println(msg)
	all = append(all,msg)
	for k,v := range WorkerMap.GetWorders() {
		msg = fmt.Sprintf("Name: %v, TaskCount: %d, StartAt: %v, CPUFree: %d, MemFree: %d",k,len(v.TaskRange),v.StartAt,v.CPUFree,v.MemFree)
		log.Println(msg)
		all = append(all,msg)
	}
	return
}

func ClusterResourceStats()(cpuTotal,cpuFree,memTotal,memFree int64){
	for _,v := range WorkerMap.GetWorders() {
		cpuTotal = cpuTotal + v.CPUTotal
		cpuFree = cpuFree + v.CPUFree
		memTotal = memTotal + v.MemTotal
		memFree = memFree + v.MemFree
	}
	return
}

// 查询集群所有任务信息
func GetClusterTasks() (ret []*Task){
	log.Println("====== Task List ====== ")
	for k,v := range TaskMap.GetTasks() {
		d := fmt.Sprintf("Name: %v, State: %v, Count: %d, CreateAt: %v, CPUQuota: %d, MemLimit: %d",k,v.State,v.Count,v.CreateAt,v.CPUQuato,v.MemLimit)
		log.Println(d)
		ret = append(ret,v)
	}
	return
}

// 查询集群所有节点信息
func GetClusterWorks()(ret []*Worker){
	log.Println("====== Worker List ====== ")
	for k,v := range WorkerMap.GetWorders() {
		d := fmt.Sprintf("Name: %v, TaskCount: %d, StartAt: %v, CPUFree: %d, MemFree: %d",k,len(v.TaskRange),v.StartAt,v.CPUFree,v.MemFree)
		log.Println(d)
		ret = append(ret,v)
	}
	return
}
