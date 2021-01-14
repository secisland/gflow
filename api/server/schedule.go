package api

import (
	"encoding/json"
	"fmt"
	"gflow/services/protocol"
	"gflow/services/server"
	"gflow/util"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func PrintClusterInfo(c *gin.Context){
	// 打印集群信息
	data := server.PrintClusterInfo()
	c.JSON(http.StatusOK, gin.H{
		"code": 1,
		"data": data,
		"msg": "成功",
	})
}

func GetWorks(c *gin.Context){
	// 获取 Worker 列表
	worklist := []string{}
	for _,v := range server.GetClusterWorks() {
		d := fmt.Sprintf("Name: %v, TaskCount: %d, StartAt: %v, CPUFree: %d, MemFree: %d\n",v.Host,len(v.TaskRange),v.StartAt,v.CPUFree,v.MemFree)
		worklist = append(worklist,d)
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 1,
		"data": worklist,
		"msg": "成功",
	})
}

func GetTasks(c *gin.Context){
	// 获取 Task 列表
	tasklist := []string{}
	for _,v := range server.GetClusterTasks() {
		d := fmt.Sprintf("Name: %v, State: %v, Count: %d, CreateAt: %v, CPUQuota: %d, MemLimit: %d\n",v.Name,v.State,v.Count,v.CreateAt,v.CPUQuato,v.MemLimit)
		tasklist = append(tasklist,d)
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 1,
		"data": tasklist,
		"msg": "成功",
	})
}

func StartTask(c *gin.Context){
	fn := c.Request.FormValue("filename")
	name := c.Request.FormValue("name")
	cmd := c.Request.FormValue("cmd")
	count,_ := strconv.Atoi(c.Request.FormValue("count"))
	cpuquota,_ := strconv.ParseInt(c.Request.FormValue("cpuquota"),10,64)
	memlimit,_ := strconv.ParseInt(c.Request.FormValue("memlimit"),10,64)


	if ok, _ := util.PathExist(fn); !ok{
		msg := fmt.Sprintf("新建任务失败, 可执行文件不存在!")
		c.JSON(http.StatusOK, gin.H{
			"code":3,
			"data":nil,
			"msg": msg,
		})
		return
	}

	// 避免重复启动任务
	//if ok, _ := server.TaskMap.Get(name);ok{
	//	msg := fmt.Sprintf("新建任务失败, 任务名已存在！")
	//	c.JSON(http.StatusOK, gin.H{
	//		"code":3,
	//		"data":nil,
	//		"msg": msg,
	//	})
	//	return
	//}

	// 调度计算，返回worker列表
	workers := server.Scheduler(count,cpuquota,memlimit)

	// 构造协议数据包
	task := protocol.ProtocolDataTask{
		Code: 1,
		Name: name,
		Count: count,
		Cmd: strings.Split(cmd, "|"),
		CpuQuota: cpuquota,
		MemLimit: memlimit,
	}
	t,err := json.Marshal(&task)
	if err != nil {
		log.Fatalln(err)
	}
	req := protocol.Protocol{
		Host:"master",
		Code: 2,
		Data: t,
	}

	// 初始化一个任务跟踪器
	workerlist := []string{}
	createAt := time.Now()

	newTask := server.Task{
		Name:task.Name,
		FilePath:fn,
		CMD:task.Cmd,
		CPUQuato:task.CpuQuota,
		MemLimit:task.MemLimit,
		Count:task.Count,
		CreateAt:createAt,
		UpdateAt:createAt,
		State:"Ready",
	}

	server.TaskMap.Set(task.Name, &newTask)
	var counter = util.Counter{I:0}
	// 向 worker 发送启动 task 协议数据包
	for _,worker := range workers {
		workerlist = append(workerlist,worker.Host)
		// 更新worker的可用资源
		w := worker
		w.CPUFree = w.CPUFree - task.CpuQuota
		w.MemFree = w.MemFree - task.MemLimit
		w.TaskRange = append(w.TaskRange,task.Name)
		w.UpdateAt = time.Now()
		server.WorkerMap.Update(w.Host,w)

		go func(){
			worker.Send(&req)
			t := time.NewTicker(30* time.Second)
			defer t.Stop()
			select {
			case <-t.C:
				fmt.Println("Worker任务启动超时:",worker.Host)
				counter.Inc(false,worker.Host)
				// TODO: 优化资源管理,超时不做任务处理
			// TODO：待优化，如果同一节点有多个任务并发执行，chan消息会错乱
			case task := <-worker.TaskChan:
				if task.Success {
					fmt.Println("Worker任务启动成功：",worker.Host,
						worker.CPUTotal,
						worker.CPUFree,
						worker.MemTotal,
						worker.MemFree,
						worker.StartAt,
						worker.UpdateAt,
						worker.TaskRange,task.Name)
					counter.Inc(true,worker.Host)
				} else {
					fmt.Println("Worker任务启动失败：",worker.Host,
						worker.CPUTotal,
						worker.CPUFree,
						worker.MemTotal,
						worker.MemFree,
						worker.StartAt,
						worker.UpdateAt,
						worker.TaskRange,task.Name,task.Message)
					counter.Inc(false,worker.Host+"=>"+task.Message)
					// 更新worker的可用资源
					w.CPUFree = w.CPUFree + task.CpuQuota
					w.MemFree = w.MemFree + task.MemLimit
					w.UpdateAt = time.Now()
					server.WorkerMap.Update(w.Host,w)
				}
			}
		}()
	}

	for len(workers)>0 && counter.I < count {
		time.Sleep(1 * time.Second)
	}

	msg := fmt.Sprintf("启动任务(%v)成功 %d 个, 失败 %d 个（%v）",name,len(counter.Sucess),len(counter.Fail),counter.Fail)
	newTask.NodeRange = workerlist
	if len(counter.Sucess) > 0 {
		if len(counter.Fail) > 0 {
			newTask.State = "Leak"
			server.TaskMap.Update(task.Name,&newTask)
			c.JSON(http.StatusOK, gin.H{
				"code": 2,
				"data": nil,
				"msg": msg,
			})
		} else {
			newTask.State = "Running"
			server.TaskMap.Update(task.Name,&newTask)
			c.JSON(http.StatusOK, gin.H{
				"code": 1,
				"data": nil,
				"msg": msg,
			})
		}
	} else {
		server.TaskMap.Delete(task.Name)
		c.JSON(http.StatusOK, gin.H{
			"code": 3,
			"data": nil,
			"msg": msg,
		})
	}
}

func StopTask(c *gin.Context){
	name := c.Param("name")

	// 构造协议数据包
	task := protocol.ProtocolDataTask{
		Code: 2,
		Name: name,
	}
	t,err := json.Marshal(&task)
	if err != nil {
		log.Fatalln(err)
	}
	req := protocol.Protocol{
		Host:"master",
		Code: 2,
		Data: t,
	}

	// 获取 worker 名称列表
	var worklist []string
	var tasktracer *server.Task
	var ok bool
	if ok, tasktracer = server.TaskMap.Get(name);ok{
		worklist = tasktracer.NodeRange
	} else {
		msg := fmt.Sprintf("停止任务失败, 任务名不存在！")
		c.JSON(http.StatusOK, gin.H{
			"code": 3,
			"data": nil,
			"msg": msg,
		})
		return
	}

	tasktracer.State = "Stoping"
	server.TaskMap.Update(task.Name,tasktracer)

	var counter = util.Counter{I:0}
	// 向 worker 发送启动 task 协议数据包
	for _,host := range worklist {
		ok, worker := server.WorkerMap.Get(host)
		if ! ok {
			log.Println("Worker不存在:",worker.Host)
			counter.Inc(false,worker.Host)
			continue
		}
		go func(){
			worker.Send(&req)
			t := time.NewTicker(30* time.Second)
			defer t.Stop()
			select {
			case <-t.C:
				log.Println("Worker任务停止超时:",worker.Host)
				counter.Inc(false,worker.Host)
				// 如果超时，不再进行节点和任务信息更新
			case task := <-worker.TaskChan:
				if task.Success {
					log.Println("Worker任务停止成功：",worker.Host,
						worker.CPUTotal,
						worker.CPUFree,
						worker.MemTotal,
						worker.MemFree,
						worker.StartAt,
						worker.UpdateAt,
						worker.TaskRange,task.Name)
					counter.Inc(true,worker.Host)
					// 从此任务节点列表中删除此节点
					tasktracer.RemoveNode(host)
					// 更新worker的可用资源
					w := worker
					w.CPUFree = w.CPUFree + tasktracer.CPUQuato
					w.MemFree = w.MemFree + tasktracer.MemLimit
					// 删除worker对应的task名称
					w.TaskRange = util.RemoveStringFromList(worklist,tasktracer.Name)
					w.UpdateAt = time.Now()
					server.WorkerMap.Update(w.Host,w)
				} else {
					log.Println("Worker任务停止失败：",worker.Host,
						worker.CPUTotal,
						worker.CPUFree,
						worker.MemTotal,
						worker.MemFree,
						worker.StartAt,
						worker.UpdateAt,
						worker.TaskRange,task.Name,task.Message)
					counter.Inc(false,worker.Host+"=>"+task.Message)
				}
			}
		}()
	}

	for len(worklist) >0 && counter.I < len(worklist) {
		time.Sleep(1 * time.Second)
	}

	server.TaskMap.Update(task.Name,tasktracer)

	msg := fmt.Sprintf("停止任务(%v)成功 %d 个, 失败 %d 个（%v）",name,len(counter.Sucess),len(counter.Fail),counter.Fail)
	if len(counter.Sucess) > 0 {
		if len(counter.Fail) > 0 {
			c.JSON(http.StatusOK, gin.H{
				"code": 2,
				"data": nil,
				"msg": msg,
			})
		} else {
			// 从任务跟踪器中删除此任务
			server.TaskMap.Delete(task.Name)
			c.JSON(http.StatusOK, gin.H{
				"code": 1,
				"data": nil,
				"msg": msg,
			})
		}
	} else {
		c.JSON(http.StatusOK, gin.H{
			"code": 3,
			"data": nil,
			"msg": msg,
		})
	}
}

// 下载可执行任务文件
func DownloadTask(c *gin.Context){
	fn := c.Request.FormValue("filename")
	name := c.Request.FormValue("name")
	cmd := c.Request.FormValue("cmd")
	count,_ := strconv.Atoi(c.Request.FormValue("count"))
	cpuquota,_ := strconv.ParseInt(c.Request.FormValue("cpuquota"),10,64)
	memlimit,_ := strconv.ParseInt(c.Request.FormValue("memlimit"),10,64)


	if ok, _ := util.PathExist(fn); !ok{
		msg := fmt.Sprintf("下载任务失败, 可执行文件不存在!")
		c.JSON(http.StatusOK, gin.H{
			"code":3,
			"data":nil,
			"msg": msg,
		})
		return
	}

	// 调度计算，返回worker列表
	workers := server.Scheduler(count,cpuquota,memlimit)

	// 构造协议数据包
	task := protocol.ProtocolDataTask{
		Code: 3,
		Name: name,
		Count: count,
		Cmd: strings.Split(cmd, "|"),
		CpuQuota: cpuquota,
		MemLimit: memlimit,
		Message:fn,
	}
	t,err := json.Marshal(&task)
	if err != nil {
		log.Fatalln(err)
	}
	req := protocol.Protocol{
		Host:"master",
		Code: 3,
		Data: t,
	}

	// 初始化一个任务跟踪器
	workerlist := []string{}
	for _,w := range workers {
		workerlist = append(workerlist,w.Host)
	}

	createAt := time.Now()
	newTask := server.Task{
		Name:task.Name,
		FilePath:fn,
		CMD:task.Cmd,
		CPUQuato:task.CpuQuota,
		MemLimit:task.MemLimit,
		Count:task.Count,
		CreateAt:createAt,
		UpdateAt:createAt,
		NodeRange:workerlist,
		State:"NotReady",
	}

	server.TaskMap.Set(task.Name, &newTask)
	successCount,failCount := Download(workers,req)

	msg := fmt.Sprintf("下载任务成功 %d 个, 失败 %d 个",successCount,failCount)
	if failCount > 0 {
		c.JSON(http.StatusOK, gin.H{
			"code": 3,
			"data": nil,
			"msg": msg,
		})
	} else {
		newTask.State = "Ready"
		server.TaskMap.Update(task.Name,&newTask)
		c.JSON(http.StatusOK, gin.H{
			"code": 1,
			"data": nil,
			"msg": msg,
		})
	}
}


func Download(workers []*server.Worker,req protocol.Protocol)(successCount,failCount int) {
	var counter= util.Counter{I: 0}
	// 向 worker 发送下载文件协议数据包
	for _, worker := range workers {
		go func() {
			worker.Send(&req)
			t := time.NewTicker(60 * time.Second)
			defer t.Stop()
			select {
			case <-t.C:
				fmt.Println("Worker下载文件超时:", worker.Host)
				counter.Inc(false, worker.Host)
			// TODO：待优化，如果同一节点有多个任务并发执行，chan消息会错乱
			case task := <-worker.TaskChan:
				if task.Success {
					fmt.Println("Worker下载文件成功：", worker.Host)
					counter.Inc(true, worker.Host)
				} else {
					fmt.Println("Worker下载文件失败：", worker.Host)
					counter.Inc(false, worker.Host+"=>"+task.Message)
				}
			}

		}()
	}

	for len(workers) > 0 && counter.I < len(workers) {
		time.Sleep(1 * time.Second)
	}
	return len(counter.Sucess), len(counter.Fail)
}
	//msg := fmt.Sprintf("下载文件任务成功 %d 个, 失败 %d 个（%v）",len(counter.Sucess),len(counter.Fail),counter.Fail)
	//if len(counter.Sucess) > 0 {
	//	if len(counter.Fail) > 0 {
	//		newTask.State = "Leak"
	//		server.TaskMap.Update(fn,&newTask)
	//		c.JSON(http.StatusOK, gin.H{
	//			"code": 2,
	//			"data": nil,
	//			"msg": msg,
	//		})
	//	} else {
	//		newTask.State = "Ready"
	//		server.TaskMap.Update(fn,&newTask)
	//		c.JSON(http.StatusOK, gin.H{
	//			"code": 1,
	//			"data": nil,
	//			"msg": msg,
	//		})
	//	}
	//} else {
	//	server.TaskMap.Delete(task.Name)
	//	c.JSON(http.StatusOK, gin.H{
	//		"code": 3,
	//		"data": nil,
	//		"msg": msg,
	//	})
	//}
