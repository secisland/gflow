package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"gflow/util"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"log"
	"math/big"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

var TaskTracker *TaskList

type TaskList struct{
	sync.RWMutex
	ListEntry []*Executor
}


func InitTaskTracker(){
	TaskTracker = &TaskList{
		ListEntry:[]*Executor{},
	}
}

func hasTask(name string)(bool,*Executor){
	for _,e := range TaskTracker.ListEntry{
		if e.Name == name {
			return true,e
		}
	}
	return false,nil
}

func addTask(exe *Executor){
	TaskTracker.Lock()
	defer TaskTracker.Unlock()

	TaskTracker.ListEntry = append(TaskTracker.ListEntry,exe)
	return
}

func removeTask(exe *Executor){
	TaskTracker.Lock()
	defer TaskTracker.Unlock()

	for i,e := range TaskTracker.ListEntry{
		if e == exe{
			if i+1 == len(TaskTracker.ListEntry){
				TaskTracker.ListEntry = TaskTracker.ListEntry[:i]
			} else {
				TaskTracker.ListEntry = append(TaskTracker.ListEntry[:i],TaskTracker.ListEntry[i+1:]...)
			}
			return
		}
	}
	return
}

type Executor struct{
	Cmd *exec.Cmd
	Name string
	CpuShares uint64
	CpuQuota int64
	MemLimit int64
	Cxt context.Context
	Cancel context.CancelFunc
	DoneChan chan bool
	CreateAt time.Time
	Log *os.File
}

func NewExecutor(name,cmdstr string, cpuQuota,memLimit int64,args ...string) (*Executor,error){
	if IsValidFileName(name) {
		return nil,errors.New("Task name is not valid")
	}
	if ok,_ := hasTask(name);ok {
		cmd := exec.Command(cmdstr, args...)
		// 进程组, 主进程退出，子进程全部退出
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		a:=CreateRandomString(5)
		fmt.Println("创建新的任务：",name+"-"+a, cpuQuota,memLimit)
		return &Executor{Cmd:cmd,Name:name+"-"+a,CpuQuota:cpuQuota,MemLimit:memLimit,DoneChan: make(chan bool),CreateAt:time.Now()},nil

		//return nil,errors.New("Task has exist")
	}
	cmd := exec.Command(cmdstr, args...)
	// 进程组, 主进程退出，子进程全部退出
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	return &Executor{Cmd:cmd,Name:name,CpuQuota:cpuQuota,MemLimit:memLimit,DoneChan: make(chan bool),CreateAt:time.Now()},nil
}

func (this *Executor)Run(){
	ctx, cancel := context.WithCancel(context.Background())
	defer func(){
		log.Println("退出前，TaskTracker长度：",len(TaskTracker.ListEntry))
		cancel()
		log.Println("退出后，TaskTracker长度：",len(TaskTracker.ListEntry))
		// 关闭log文件对象
		_ = this.Log.Close()
	}()

	this.Cxt = ctx
	this.Cancel = cancel

	// 打印任务运行日志
	//path := viper.GetString("log.path")
	pwd,_ := os.Getwd()
	stdout, err := os.OpenFile(pwd+"/logs/"+this.Name+".log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0655)
	if err != nil {
		this.DoneChan<- false
		fmt.Println("任务处理失败")
		log.Fatalln("创建日志文件失败:",err.Error())
		return
	}
	this.Cmd.Stdout = stdout
	this.Log = stdout

	addTask(this)

	// 获取当前进程号
	pid := os.Getpid()

	fmt.Println("创建cgroup子节点：",this.Name, this.CpuQuota, this.MemLimit)
	// 创建cgroup 子节点
	control,err :=util.CGroup.New(this.Name, &specs.LinuxResources{
		CPU: &specs.LinuxCPU{
			Quota: &this.CpuQuota,
		},
		Memory: &specs.LinuxMemory{
			Limit: &this.MemLimit,
		},
	})

	if err != nil {
		log.Println("创建cgroup子节点失败:",err.Error())
		this.DoneChan<- false
		return
	}

	defer func(){
		// 正常任务退出，会自动清理cgroup子节点
		fmt.Println("自动清理cgroup子节点：",this.Name)
		_ = control.Delete()
	}()

	// 添加当前进程cgroup资源限制，子进程会继承父进程设置
	err = util.AddProcessToCgroup(control,this.Name,pid)
	if err != nil {
		this.DoneChan<- false
		log.Println("添加cgroup子节点任务进程失败:",err.Error())
		return
	}

	log.Println("当前 TaskTracker长度：",len(TaskTracker.ListEntry))

	if err := this.Cmd.Start(); err != nil {
		log.Println("任务启动失败！",err)

		// 还原父进程到 gflow cgroup节点
		_ = util.AddProcessToCgroup(control,"gflow",os.Getpid())
		this.DoneChan<- false
		return
	}

	log.Println("任务启动 PID:",this.Cmd.Process.Pid)

	this.DoneChan<- true

	// 还原父进程到 gflow cgroup节点
	_ = util.AddProcessToCgroup(util.CGroup,"gflow",os.Getpid())

	errCh := make(chan error, 1)
	go func() {
		errCh <- this.Cmd.Wait()
	}()

	// 任务退出
	select {
	case <-ctx.Done():
		log.Println("开始停止任务:",this.Name)
		//pid := this.Cmd.Process.Pid
		pgid,err := syscall.Getpgid(this.Cmd.Process.Pid)
		if err != nil {
			this.DoneChan<- false
			fmt.Println("获取父进程GPID失败！",err.Error())
			removeTask(this)
			return
		}

		if err := syscall.Kill(-1*pgid, syscall.SIGKILL); err != nil {
			log.Println("完成停止任务，出现异常：",err.Error(),"PID:",pgid)
			// 休眠1秒检查进程是否存在
			time.Sleep(1*time.Second)
			if err = syscall.Kill(pgid, 0); err == nil {
				this.DoneChan <- false
			} else {
				fmt.Println("进程已不存在，从任务管理器中移除！")
				this.DoneChan <- true
				removeTask(this)
			}
		} else {
			this.DoneChan <- true
			removeTask(this)
		}
		log.Println("完成停止任务:",this.Name)
	case <-errCh:
		removeTask(this)
		this.DoneChan <- true
	}
}

func (this *Executor)Stop()bool{
	log.Println("Kill Task PID:",this.Cmd.Process.Pid)
	this.Cancel()
	return true
}


func IsValidFileName(fn string) bool{
	// 文件名长度不能超过255
	if len(fn) > 255{
		return false
	}

	// 避免使用加号、减号或者"."作为普通文件的第一个字符
	for _,s := range []string{"+","-","."}{
		if strings.HasPrefix(fn,s){
			return false
		}
	}

	// 文件名避免使用下列特殊字符,包括制表符和退格符
	for _,s := range []string{"/", "\t", "\b", "@", "#", "$", "%", "^", "&", "*", "(", ")", "[", "]"} {
		if strings.Index(fn,s) == -1 {
			return false
		}
	}
	return true
}

func CreateRandomString(len int) string  {
	var container string
	var str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	b := bytes.NewBufferString(str)
	length := b.Len()
	bigInt := big.NewInt(int64(length))
	for i := 0;i < len ;i++  {
		randomInt,_ := rand.Int(rand.Reader,bigInt)
		container += string(str[randomInt.Int64()])
	}
	return container
}


