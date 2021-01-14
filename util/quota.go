package util

import (
	"errors"
	"fmt"
	"github.com/containerd/cgroups"
	"github.com/opencontainers/runtime-spec/specs-go"
	"log"
	"os"
)

var CGroup cgroups.Cgroup

func InitCgroup(cpuQuota,memLimit int64) bool{
	control, err := cgroups.Load(cgroups.V1, cgroups.StaticPath("/gflow"))
	if err != nil {
		log.Println("不存在/gflow cgroup节点,开始创建：")

		control, err = cgroups.New(cgroups.V1, cgroups.StaticPath("/gflow"), &specs.LinuxResources{
			CPU: &specs.LinuxCPU{
				Quota: &cpuQuota,
			},
			Memory: &specs.LinuxMemory{
				Limit: &memLimit,
			},
		})
		if err != nil {
			fmt.Println("创建/gflow cgroup节点失败！",err.Error())
			return false
		}
		fmt.Println("创建/gflow cgroup节点成功！")
	} else {
		fmt.Println("加载 /gflow cgroup节点成功，开始更新cgroup配置：")
	}

	if err := control.Update(&specs.LinuxResources{
		CPU: &specs.LinuxCPU{
			Quota: &cpuQuota,
		},
		Memory: &specs.LinuxMemory{
			Limit: &memLimit,
		},
	}); err != nil {
		fmt.Println("更新 /gflow cgroup节点失败，退出",err.Error())
		return false
	}

	fmt.Println("更新 /gflow cgroup节点成功,开始绑定进程ID：")

	pid := os.Getpid()

	// Add a process to the cgroup
	if err := control.Add(cgroups.Process{Pid:pid}); err != nil {
		fmt.Println("绑定PID：",pid,"到 /gflow cgroup节点失败：",err.Error())
	} else {
		fmt.Println("绑定PID：",pid,"到 /gflow cgroup节点成功!")
	}
	CGroup = control
	return true
}

//
//func InitCGroup(){
//	pid := os.Getpid()
//	cpuQuota := int64(0)
//	memLimit := int64(2147483648)
//
//	cpuNum := runtime.NumCPU()
//	if cpuNum >2 {
//		cpuQuota = int64(cpuNum) - 2
//	}
//
//	sysInfo := new(syscall.Sysinfo_t)
//	err := syscall.Sysinfo(sysInfo)
//	if err != nil {
//		fmt.Println("获取内存信息失败：",err.Error())
//	}
//	memSub := int64(sysInfo.Totalram) - memLimit
//	if memSub > int64(1073741824) {
//		memLimit = memSub
//	}
//
//	fmt.Println("CPU核心数：",cpuNum,cpuQuota)
//	fmt.Println("总内存：", sysInfo.Totalram,"内存限额：",memLimit)
//
//	CGroup,err = CreeateCgroup("/gflow",pid,cpuQuota,memLimit)
//	if err !=nil {
//		log.Println("Failed to init cgroup files")
//		os.Exit(1)
//	}
//}
//
//func CreeateCgroup(path string, pid int, cpuQuota,memLimit int64) (cgroups.Cgroup,error){
//
//	// Attention: Don't forget to clean
//	// control.Delete()
//
//	if ! strings.HasPrefix(path,"/") {
//		return nil,errors.New("The cgroup path which provided is invalid!")
//	}
//
//	period := uint64(100000)
//	quota := int64(cpuQuota)
//	limit := int64(memLimit)
//
//	control, err := cgroups.Load(cgroups.V1, cgroups.StaticPath(path))
//	if err == nil {
//		_ = control.Delete()
//	}
//
//	// 每次新创建cgroup节点
//	control, err = cgroups.New(cgroups.V1, cgroups.StaticPath(path), &specs.LinuxResources{
//		CPU: &specs.LinuxCPU{
//			Period: &period,
//			Quota: &quota,
//		},
//		Memory: &specs.LinuxMemory{
//			Limit: &limit,
//		},
//	})
//	if err != nil {
//		log.Println("创建",path," cgroup节点失败!")
//		return nil, errors.New("The cgroup path which provided is failed to create!")
//
//	}
//
//	log.Println("创建",path," cgroup节点成功！")
//
//
//
//	if err := control.Update(&specs.LinuxResources{
//		CPU: &specs.LinuxCPU{
//			Period: &period,
//			Quota: &quota,
//		},
//		Memory: &specs.LinuxMemory{
//			Limit: &limit,
//		},
//	}); err != nil {
//		log.Println("更新 ",path," cgroup节点失败，退出")
//		return nil,errors.New("The cgroup path which provided is failed to update!")
//	}
//
//	log.Println("更新 ",path," cgroup节点成功,开始绑定进程ID：",pid)
//
//	// Add a process to the cgroup
//	if err := control.Add(cgroups.Process{Pid:pid}); err != nil {
//		log.Println("绑定PID：",pid,"到 ",path,"cgroup节点失败：",err.Error())
//		return nil,errors.New("Failed to add pid to cgroup file!")
//	} else {
//		log.Println("绑定PID：",pid,"到 ",path," cgroup节点成功!")
//	}
//
//	return control,nil
//}

func AddProcessToCgroup(control cgroups.Cgroup,name string,pid int) error{
	// Add a process to the cgroup
	if err := control.Add(cgroups.Process{Pid:pid}); err != nil {
		log.Println("绑定PID：",pid,"到 ",name,"cgroup节点失败：",err.Error())
		return errors.New("Failed to add pid to cgroup file!")
	} else {
		log.Println("绑定PID：",pid,"到 ",name," cgroup节点成功!")
		return nil
	}
}

