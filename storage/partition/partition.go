package partition

import (
	"sync"
	"time"
)

var CDB *Barrier


func InitCDB() {
	CDB = &Barrier{}
}

type Barrier struct {
	sync.RWMutex
	Tp []*Topology
}
//
//func NewBarrier() *Barrier {
//	return &Barrier{}
//}

func (this *Barrier)IsExistTopology(topologyName string) (bool, *Topology) {
	this.RLock()
	defer this.RUnlock()

	for _, v := range this.Tp {
		if v.TopologyName== topologyName{
			return true,v
		}
	}
	return false,nil
}

func (this *Barrier)AddTopology(topologyName ,version string,expired time.Duration) *Topology {
	this.Lock()
	defer this.Unlock()

	tp := &Topology{
		TopologyName:topologyName,
		Page: make(map[string]Segment),
		Version:version,
		CreateOn:time.Now(),
		LifeSpan:expired,
	}

	this.Tp = append(this.Tp,tp)

	go func(){
		for _,v := range this.Tp {
			if v.TopologyName == topologyName && v.Version == version {
				t := time.Now()
				expiredInterval := expired - t.Sub(v.CreateOn)
				if expiredInterval < 0 * time.Second {
					go this.DeleteTopology(topologyName,version)
				} else {
					ticker := time.NewTicker(expiredInterval)
					defer ticker.Stop()

					for {
						select {
						case <-ticker.C:
							this.DeleteTopology(topologyName,version)
						}
					}
				}
			}
		}
	}()

	return tp
}

func (this *Barrier)DeleteTopology(topologyName,version string){
	this.Lock()
	defer this.Unlock()

	for k, v := range this.Tp {
		if v.TopologyName== topologyName && v.Version == version{
			if k+1 < len(this.Tp) {
				this.Tp = append(this.Tp[:k],this.Tp[k+1:]...)
			} else {
				this.Tp = this.Tp[:k]
			}
		}
	}
}



type Topology struct{
	sync.RWMutex
	TopologyName string
	Page map[string]Segment // key
	Version string
	CreateOn time.Time
	LifeSpan time.Duration
}

type Segment map[string]*Aggregator // group

type Aggregator struct {
	Count int
	Data interface{}
}

func (this *Topology)IsExistKey(key string)bool {
	this.RLock()
	defer this.RUnlock()

	if _,ok := this.Page[key];ok {
		return true
	}
	return false
}

func (this *Topology)IsExistGroup(key, groupName string)bool {
	this.RLock()
	defer this.RUnlock()

	if _,ok := this.Page[key][groupName];ok {
		return true
	}
	return false
}

func (this *Topology)GetDataByKeyGroup(key,groupName string) (bool,interface{}) {
	this.RLock()
	defer this.RUnlock()

	if this.IsExistGroup(key,groupName) {
		return true,this.Page[key][groupName]
	} else {
		return false,nil
	}
}

func (this *Topology)Add(key,group string,count int, data []byte)*Topology{
	this.Lock()
	defer this.Unlock()

	if this.IsExistKey(key) {
		this.Page[key][group].Count = count
		this.Page[key][group].Data = data
	} else {
		// map初始化
		segment := make(map[string]*Aggregator)
		segment[group] = &Aggregator{Count:count, Data:data}

		this.Page[key] = segment
	}
	return this
}


func (this *Topology)Set(page map[string]Segment)*Topology{
	this.Lock()
	defer this.Unlock()

	this.Page = page
	return this
}


