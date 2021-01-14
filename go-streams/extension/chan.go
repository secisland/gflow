package ext

import (
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ChanSource streams data from the input channel
type ChanSource struct {
	in chan interface{}
}

// NewChanSource returns a new ChanSource instance
func NewChanSource(in chan interface{}) *ChanSource {
	return &ChanSource{in}
}

// Via streams data through the given flow
func (cs *ChanSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(cs, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (cs *ChanSource) Out() <-chan interface{} {
	return cs.in
}

// ChanSink sends data to the output channel
type ChanSink struct {
	Out chan interface{}
}

// NewChanSink returns a new ChanSink instance
func NewChanSink(out chan interface{}) *ChanSink {
	return &ChanSink{out}
}

// In returns an input channel for receiving data
func (ch *ChanSink) In() chan<- interface{} {
	return ch.Out
}
