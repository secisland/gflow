package api

import (
	"bytes"
	"context"
	"encoding/binary"
	"gflow/services/protobuf/download"
	"io"
	"log"
	"os"
	"path/filepath"
)

//  客户端行为：
//  	启动
//		停止
//		注册
//		下发数据
// 		任务管理
// 		执行器解码
// 		运行状态查询 cputotal/cpufree/memtotal/memfree/taskinfolist


var DownloadStreamClient download.DonwloadClient

func DownLoad(ctx context.Context, sfile,path string) bool {
	stream, err := DownloadStreamClient.Get(ctx,&download.GetRequest{Filename:sfile})
	if err !=nil {
		log.Println("下载文件失败: ",err)
		return false
	}

	_,filename := filepath.Split(sfile)
	f,err := os.OpenFile(filepath.Join(path,filename),os.O_WRONLY | os.O_CREATE, 0755)
	if err !=nil {
		log.Println("创建目标文件失败: ",err)
		return false
	}

	defer func(){
		_ = f.Close()
	}()

	log.Println("开始保存文件..")
	for {
		resp,err := stream.Recv()
		if err != io.EOF {
			//fmt.Println(string(resp.Data))

			buf := new(bytes.Buffer)
			_ =binary.Write(buf, binary.BigEndian, resp.Data)
			_,err := f.Write(buf.Bytes())
			if err !=nil {
				log.Println("下载文件保存失败: ",err)
				return false
			}
		} else {
			break
		}
	}
	log.Println("保存文件完成!")
	return true
}





//
//
//type Excutor struct {
//	Source []byte
//	Map [][]byte
//	Reduce []byte
//	Sink []byte
//}
//
//func (this *Excutor)setSource(data []byte){
//	this.Source = data
//}
//
//func (this *Excutor)setMap(data []byte){
//	this.Map = append(this.Map,data)
//}
//
//func (this *Excutor)setReduce(data []byte){
//	this.Reduce = data
//}
//
//func (this *Excutor)setSink(data []byte){
//	this.Sink = data
//}
//
//
//func (this *Excutor)Start(){
//
//}
//
//
//type KafkaConfig struct{
//	hosts []string					// []string{"127.0.0.1:9092"}
//	ConsumerStrategy int			// 1
//	ConsumerOffset int				// 0
//	ProducerReturnSuccess bool 		// true
//	Version string					// "2.2.0"
//	TopicName string
//	GroupName string
//}
//
//func startKafkaWorker(conf KafkaConfig){
//	ctx := context.Background()
//	config := sarama.NewConfig()
//
//	switch conf.ConsumerStrategy {
//	case 0:
//		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
//	case 1:
//		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
//	case 2:
//		config.Consumer.Group.Rebalance.Strategy =sarama.BalanceStrategySticky
//	}
//
//	switch conf.ConsumerOffset {
//	case 0:
//		config.Consumer.Offsets.Initial = sarama.OffsetNewest
//	case 1:
//		config.Consumer.Offsets.Initial = sarama.OffsetOldest
//	}
//
//	config.Producer.Return.Successes = conf.ProducerReturnSuccess
//	config.Version, _ = sarama.ParseKafkaVersion(conf.Version)
//	groupID := conf.GroupName
//
//	source := ext.NewKafkaSource(ctx, conf.hosts, groupID, config, conf.TopicName)
//
//
//	flow1 := flow.NewMap(toUpper, 1)
//	flow2 := flow.NewFlatMap(appendAsterix, 1)
//	sink := ext.NewKafkaSink(hosts, config, "test2")
//	throttler := flow.NewThrottler(1, time.Second*1, 50, flow.Discard)
//	// slidingWindow := flow.NewSlidingWindow(time.Second*30, time.Second*5)
//	tumblingWindow := flow.NewTumblingWindow(time.Second * 5)
//
//	source.Via(flow1).Via(throttler).Via(tumblingWindow).Via(flow2).To(sink)
//}
