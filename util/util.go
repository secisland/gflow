package util

import (
	"log"
	"os"
	"sync"
)

type Counter struct{
	sync.RWMutex
	I int
	Sucess []interface{}
	Fail []interface{}
}

func (this *Counter)Inc(ok bool,value interface{}){
	this.Lock()
	defer this.Unlock()

	this.I = this.I + 1
	if ok {
		this.Sucess = append(this.Sucess,value)
	} else {
		this.Fail = append(this.Fail,value)
	}
}

func RemoveStringFromList(list []string, s string) []string{
	l := len(list)
	if l == 0{
		return list
	} else {
		index := -1
		for i,n := range list {
			if n == s {
				index = i
				break
			}
		}

		if index < 0 {
			log.Println("列表中不存在此字符串！")
		} else if index == 0  {
			list = list[index+1:]
		} else if index == l -1 {
			list = list[:index]
		} else {
			list = append(list[:index],list[index+1:]...)
		}
		return list
	}
}

func PathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
