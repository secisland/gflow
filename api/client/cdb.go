package api

import (
	"context"
	"encoding/json"
	"errors"
	"gflow/services/protobuf/cdb"
	"gflow/services/protocol"
	"gflow/storage/partition"
	"google.golang.org/grpc"
	"io"
	"log"
)

type CDB struct {
	cdb.UnimplementedCDBServer
}

func (this *CDB) Get(req *cdb.GetRequest, stream cdb.CDB_GetServer) error {
	top := protocol.Topology{Page:make(map[string]partition.Segment)}
	top.TopologyName = req.Topname

	ok,t := partition.CDB.IsExistTopology(req.Topname)
	if ok && req.Version == t.Version {
		top.TopologyName = t.TopologyName
		top.Version = t.Version
		top.Page = t.Page
		top.LifeSpan = t.LifeSpan
	}  else {
		return errors.New("No topname:"+req.Topname+"|"+req.Version)
	}

	b,err := json.Marshal(top)
	if err != nil{
		return err
	}

	//buff := make([]byte, 1024*4) // 55=该文本的长度
	l := len(b)
	fixlen := 1024*4

	for i:=0; i< fixlen; i = i + fixlen {
		if l-i <= fixlen {
			err := stream.Send(&cdb.GetReply{Data:b[i:l-i]})
			if err != nil {
				log.Println("grpc cdb.get 传输异常: ",err)
				return err
			}
		} else {
			err := stream.Send(&cdb.GetReply{Data:b[i:fixlen]})
			if err != nil {
				log.Println("grpc cdb.get 传输异常: ",err)
				return err
			}
		}
	}
	log.Println("grpc cdb.get 完成！")

	return nil
}
func (this *CDB) GetByKey(req *cdb.GetByKeyRequest, stream cdb.CDB_GetByKeyServer) error {
	top := protocol.Topology{Page:make(map[string]partition.Segment)}
	top.TopologyName = req.Topname

	ok,t := partition.CDB.IsExistTopology(req.Topname)
	if ok {
		top.TopologyName = t.TopologyName
		top.Version = t.Version
		top.LifeSpan = t.LifeSpan
		if s,ok := t.Page[req.Keyname];ok {
			top.Page[req.Keyname] = s
		} else {
			return errors.New("No keyname:"+req.Keyname)
		}
	}  else {
		return errors.New("No topname:"+req.Topname)
	}

	b,err := json.Marshal(top)
	if err != nil{
		return err
	}

	//buff := make([]byte, 1024*4) // 55=该文本的长度
	l := len(b)
	fixlen := 1024*4

	for i:=0; i< fixlen; i = i + fixlen {
		if l-i <= fixlen {
			err := stream.Send(&cdb.GetReply{Data:b[i:l-i]})
			if err != nil {
				log.Println("grpc cdb.get 传输异常: ",err)
				return err
			}
		} else {
			err := stream.Send(&cdb.GetReply{Data:b[i:fixlen]})
			if err != nil {
				log.Println("grpc cdb.get 传输异常: ",err)
				return err
			}
		}
	}
	log.Println("grpc cdb.get 完成！")

	return nil
}

func (this *CDB) Add(ctx context.Context, req *cdb.AddRequest) (*cdb.AddReply, error) {
	top := protocol.Topology{Page:make(map[string]partition.Segment)}
	if err :=json.Unmarshal(req.Data,&top);err !=nil {
		return &cdb.AddReply{Success:false},errors.New("grpc cdb.add 请求数据不正确！"+err.Error())
	}

	ok,t := partition.CDB.IsExistTopology(top.TopologyName)
	if ok {
		for k,v := range top.Page {
			t.Page[k] = v
		}
	} else {
		tt := partition.CDB.AddTopology(top.TopologyName,top.Version,top.LifeSpan)
		tt.Set(top.Page)
	}

	return &cdb.AddReply{Success:true},nil
}


func (this *CDB) Set(ctx context.Context, req *cdb.SetRequest) (*cdb.SetReply, error) {
	top := protocol.Topology{Page:make(map[string]partition.Segment)}
	if err :=json.Unmarshal(req.Data,&top);err !=nil {
		return &cdb.SetReply{Success:false},errors.New("grpc cdb.add 请求数据不正确！"+err.Error())
	}

	ok,t := partition.CDB.IsExistTopology(top.TopologyName)
	if ok {
		t.Page = top.Page
	} else {
		tt := partition.CDB.AddTopology(top.TopologyName,top.Version,top.LifeSpan)
		tt.Set(top.Page)
	}

	return &cdb.SetReply{Success:true},nil
}

func (this *CDB) Delete(ctx context.Context, req *cdb.DeleteRequest) (resp *cdb.DeleteReply, err error) {
	ok,_ := partition.CDB.IsExistTopology(req.Topname)
	if ok {
		partition.CDB.DeleteTopology(req.Topname,req.Version)
	}
	return &cdb.DeleteReply{Success:true},nil
}
func (this *CDB) DeleteByKey(ctx context.Context, req *cdb.DeleteByKeyRequest) (resp *cdb.DeleteReply, err error) {
	ok,t := partition.CDB.IsExistTopology(req.Topname)
	if ok && t.Version == req.Version {
		delete(t.Page,req.Keyname)
	}
	return &cdb.DeleteReply{Success:true},nil
}


// cdb grpc客户端代码： 可以连同gflow/services/profobuf/cdb/cdb.proto.go文件一起拷贝到 go-stream，实现外部任务进程可以grpc管理cdb数据库；

func CDBGet(addr string,topname ,version string ) *protocol.Topology {
	// grpc 客户端
	conn,err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Println("服务器grpc连接失败：" + err.Error())
	} else {
		log.Println("服务器grpc连接成功！")
	}

	defer conn.Close()
	CdbClient := cdb.NewCDBClient(conn)

	ctx,_:=context.WithCancel(context.Background())

	stream,err := CdbClient.Get(ctx,&cdb.GetRequest{Topname:topname,Version:version})
	if err != nil {
		log.Println("grpc cdb.get 出错了:",err)
		return nil
	}

	top := protocol.Topology{Page:make(map[string]partition.Segment)}
	b := []byte{}
	for {
		resp,err := stream.Recv()
		if err != io.EOF {
			if err != nil {
				log.Println("grpc cdb.get 出错了:",err)
				return nil
			}
			b = append(b,resp.Data...)
		} else {
			break
		}
	}
	if err := json.Unmarshal(b,&top);err !=nil {
		log.Println("grpc cdb.get 数据异常:",err)
		return nil
	}
	return &top
}


func CDBSet(addr string,top protocol.Topology ) error {
	// grpc 客户端
	conn,err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Println("服务器grpc连接失败：" + err.Error())
	} else {
		log.Println("服务器grpc连接成功！")
	}

	defer conn.Close()
	CdbClient := cdb.NewCDBClient(conn)

	b,_ := json.Marshal(top)
	ctx,_:=context.WithCancel(context.Background())
	resp,err := CdbClient.Set(ctx,&cdb.SetRequest{Data:b})
	if err != nil {
		log.Println("grpc cdb.set 出错了:",err.Error())
		return errors.New("grpc cdb.set 出错了:"+err.Error())
	}
	if resp.Success {
		log.Println("grpc cdb.set 成功")
	} else {
		log.Println("grpc cdb.set 失败")
	}

	return nil
}


func CDBDelete(addr string,topname , version string ) error {
	// grpc 客户端
	conn,err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Println("服务器grpc连接失败：" + err.Error())
	} else {
		log.Println("服务器grpc连接成功！")
	}

	defer conn.Close()
	CdbClient := cdb.NewCDBClient(conn)
	ctx,_:=context.WithCancel(context.Background())
	resp,err := CdbClient.Delete(ctx,&cdb.DeleteRequest{Topname:topname,Version:version})
	if err != nil {
		log.Println("grpc cdb.delete 出错了:",err.Error())
		return errors.New("grpc cdb.delete 出错了:"+err.Error())
	}
	if resp.Success {
		log.Println("grpc cdb.delete 成功")
	} else {
		log.Println("grpc cdb.delete 失败")
	}

	return nil
}
