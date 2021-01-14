package ext

import "fmt"

// StdoutSink sends items to stdout
type StdoutSink struct {
	in chan interface{}
}

// NewStdoutSink returns a new StdoutSink instance
func NewStdoutSink() *StdoutSink {
	sink := &StdoutSink{make(chan interface{})}
	sink.init()
	return sink
}

func (stdout *StdoutSink) init() {
	go func() {
		for elem := range stdout.in {
			fmt.Println(elem)
		}
	}()
}

// In returns an input channel for receiving data
func (stdout *StdoutSink) In() chan<- interface{} {
	return stdout.in
}

// IgnoreSink sends items to /dev/null
type IgnoreSink struct {
	in chan interface{}
}

// NewIgnoreSink returns a new IgnoreSink instance
func NewIgnoreSink() *IgnoreSink {
	sink := &IgnoreSink{make(chan interface{})}
	sink.init()
	return sink
}

func (ignore *IgnoreSink) init() {
	go func() {
		for {
			_, ok := (<-ignore.in)
			if !ok {
				break
			}
		}
	}()
}

// In returns an input channel for receiving data
func (ignore *IgnoreSink) In() chan<- interface{} {
	return ignore.in
}
