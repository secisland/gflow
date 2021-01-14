package index

import (
	"sync"
	"time"
)


type Index struct{
	*sync.RWMutex
	Topologies map[string]*TopologyVerion  // 拓朴
}

func NewIndex() *Index{
	return &Index{
		Topologies:make(map[string]*TopologyVerion),
	}
}

func (this *Index)IsExistTopology(tp string) bool{
	this.RLock()
	defer this.RUnlock()

	if _,ok := this.Topologies[tp];ok {
		return true
	}
	return false
}


func (this *Index)GetTopologyNames() []string{
	this.RLock()
	defer this.RUnlock()

	var TopologyNames []string
	for name,_ := range this.Topologies {
		TopologyNames = append(TopologyNames, name)
	}
	return TopologyNames
}

func (this *Index)GetTopologyByName(topologyName string) (bool,*TopologyVerion) {
	this.RLock()
	defer this.RUnlock()

	if this.IsExistTopology(topologyName) {
		for ver,_ := range this.Topologies[topologyName].Verion {
			this.CheckTopologyVersion(topologyName,ver)
		}
		return true,this.Topologies[topologyName]
	}
	return false,nil
}

func (this *Index)GetTopologyVersion(topologyName,version string) (bool,*Topology) {
	this.RLock()
	defer this.RUnlock()

	if this.IsExistTopology(topologyName) {
		this.Topologies[topologyName].RLock()
		defer this.Topologies[topologyName].RUnlock()

		if v,ok := this.Topologies[topologyName].Verion[version];ok && this.CheckTopologyVersion(topologyName,version){
			return true,v
		}
		return false,nil
	}
	return false,nil
}


func (this *Index)AddTopologyVersion(topologyName,version string,expired time.Duration) (bool,*Topology) {
	this.Lock()
	defer this.Unlock()

	if this.IsExistTopology(topologyName) {
		if v,ok := this.Topologies[topologyName].Verion[version];ok {
			return false,v
		} else {
			topology := &Topology{
				Data: make(map[string]Partitions),
				CreateOn:time.Now(),
				LifeSpan: expired,
			}
			this.Topologies[topologyName].Verion[version].Lock()
			defer this.Topologies[topologyName].Verion[version].Unlock()

			this.Topologies[topologyName].Verion[version] = topology

			go func(){
				t := time.Now()
				expiredInterval := expired - t.Sub(this.Topologies[topologyName].Verion[version].CreateOn)
				if expiredInterval < 0 * time.Second {
					this.DeleteTopologyVersion(topologyName,version)
				} else {
					ticker := time.NewTicker(expiredInterval)
					defer ticker.Stop()

					for {
						select {
						case <-ticker.C:
							this.DeleteTopologyVersion(topologyName,version)
						}
					}
				}
			}()

			return true,topology
		}
	} else {
		topology := &Topology{
			Data: make(map[string]Partitions),
			CreateOn:time.Now(),
			LifeSpan: expired,
		}
		topologyVersion := &TopologyVerion{
			Verion:make(map[string]*Topology),
		}
		topologyVersion.Verion[version] =topology

		go func(){
			t := time.Now()
			expiredInterval := expired - t.Sub(this.Topologies[topologyName].Verion[version].CreateOn)
			if expiredInterval < 0 * time.Second {
				this.DeleteTopologyVersion(topologyName,version)
			} else {
				ticker := time.NewTicker(expiredInterval)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						this.DeleteTopologyVersion(topologyName,version)
					}
				}
			}
		}()

		this.Topologies[topologyName] = topologyVersion
		return true,topology
	}
}



func (this *Index)CheckTopologyVersion(topologyName,version string) bool {
	this.RLock()
	defer this.RUnlock()

	t := time.Now()
	expiredInterval := this.Topologies[topologyName].Verion[version].LifeSpan - t.Sub(this.Topologies[topologyName].Verion[version].CreateOn)
	if expiredInterval < 0 * time.Second {
		go this.DeleteTopologyVersion(topologyName,version)
		return false
	}
	return true
}


func (this *Index)DeleteTopologyVersion(topologyName,version string) {
	this.Topologies[topologyName].Lock()
	defer this.Topologies[topologyName].Unlock()

	delete(this.Topologies[topologyName].Verion,version)

	if len(this.Topologies[topologyName].Verion) == 0 {
		this.Lock()
		defer this.Unlock()
		delete(this.Topologies, topologyName)
	}
}




type TopologyVerion struct{
	*sync.RWMutex
	Verion map[string]*Topology	// 版本
}

type Topology struct{
	*sync.RWMutex
	CreateOn time.Time
	LifeSpan time.Duration
	Data map[string]Partitions  // key
}

type Partitions []Partition

type Partition struct{
	Group string  // group
	Host string
	Count int
}


func (this *Topology)GetKey(key string) (bool, Partitions){
	this.RLock()
	defer this.RUnlock()

	if pts,ok := this.Data[key];ok && this.CheckTopology() {
			return true, pts
	}
	return false,nil
}

func (this *Topology)GetKeies() (bool, []string){
	this.RLock()
	defer this.RUnlock()

	pt := []string{}

	if ! this.CheckTopology() {
		return false, nil
	}

	for k,_ := range this.Data {
		pt = append(pt, k)
	}
	return true,pt
}


func (this *Topology)CheckTopology()bool {
	this.Lock()
	defer this.Unlock()

	t := time.Now()
	expiredInterval := this.LifeSpan - t.Sub(this.CreateOn)

	if expiredInterval < 0 * time.Second {
		return false
	}
	return true
}


func (this *Topology)AddTopologyPartition(key string, pt Partition) bool{
	this.Lock()
	defer this.Unlock()

	if _,ok := this.Data[key];ok{
		this.Data[key] = append(this.Data[key],pt)
	} else {
		this.Data[key] = []Partition{pt}
	}
	return true
}
