package api

import (
	"gflow/services/protobuf/download"
	"io"
	"log"
	"os"
)


type DonwloadServer struct {
}

func (this *DonwloadServer) Get(req *download.GetRequest, stream download.Donwload_GetServer) error {
	log.Println("grpc download 开始...")
	fp, err := os.Open(req.Filename)
	if err !=nil {
		log.Println("grpc download 读取文件出错：",err)
		return err
	}
	defer fp.Close()

	buff := make([]byte, 1024*4) // 55=该文本的长度

	for {
		length, err := fp.Read(buff)
		if err != nil && err != io.EOF {
			log.Println("grpc download异常: ",err)
			return nil
		}

		if err == io.EOF {
			//fmt.Println("读取完毕")
			break
		}

		if length == 1024*4 {
			err := stream.Send(&download.GetReply{Data:buff})
			if err != nil {
				log.Println("grpc download 传输异常: ",err)
				return err
			}
		} else {
			err := stream.Send(&download.GetReply{Data:buff[:length]})
			if err != nil {
				log.Println("grpc download 传输异常: ",err)
				return err
			}
			break
		}
	}
	log.Println("grpc download 完成！")

	return nil
}

//
//import (
//	"fmt"
//	api "gflow/api/client"
//	"io/ioutil"
//	"log"
//	"net/http"
//	"os"
//)
//
////  客户端行为：
////  	启动
////		停止
////		注册信息管理
////		索引管理
////		下发索引数据
////		拓朴管理
////		任务调度
//
//
//// 集群信息查询 restful API
//func ClusterInfo(){
//
//}
//
//func GetFile(w http.ResponseWriter, r *http.Request) {
//	r.ParseForm()
//	pwd,_ := os.Getwd()
//	fmt.Println("PWD:",pwd,"URL:",r.URL.Path)
//	des := pwd + string(os.PathSeparator) + r.URL.Path[1:len(r.URL.Path)]
//
//	desStat,err := os.Stat(des)
//
//	if err != nil {
//		log.Println("File Not Exsit",des)
//		http.NotFoundHandler().ServeHTTP(w,r)
//	} else if(desStat.IsDir()) {
//		log.Println("File Is Dir",des)
//		http.NotFoundHandler().ServeHTTP(w,r)
//	} else {
//		data,err := ioutil.ReadFile(des)
//		if err != nil{
//			log.Println("Read File Err:",err.Error())
//		} else {
//			log.Println("Send File:",des)
//			w.Write(data)
//			w.Write([]byte("ok"))
//		}
//	}
//}
//
//func RunTest(w http.ResponseWriter, r *http.Request) {
//	pwd,_ := os.Getwd()
//	exe,err := api.NewExecutor("test",pwd+string(os.PathSeparator)+"a.sh")
//	if err !=nil {
//		log.Println("任务创建失败:",err.Error())
//		w.Write([]byte("failed"))
//		return
//	}
//	go func(){
//		_ =  exe.Run()
//	}()
//	fmt.Println("TaskTracker长度：",len(api.TaskTracker.ListEntry))
//	w.Write([]byte("ok"))
//}
//
//func KillTest(w http.ResponseWriter, r *http.Request) {
//	api.TaskTracker.RLock()
//	defer api.TaskTracker.RUnlock()
//
//	fmt.Println("Kill 前TaskTracker长度：",len(api.TaskTracker.ListEntry))
//	for i,e := range api.TaskTracker.ListEntry {
//		fmt.Println("序号：",i)
//		if e.Name == "test" {
//			go e.Stop()
//		}
//	}
//	fmt.Println("Kill 后 TaskTracker长度：",len(api.TaskTracker.ListEntry))
//	w.Write([]byte("ok"))
//}
//
