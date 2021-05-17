// Copyright (c) 2019 Andy Pan
// Copyright (c) 2018 Joshua J Baker
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// +build linux freebsd dragonfly darwin

package gnet

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/jackdai123/endpoint"
	"github.com/jackdai123/gnet/errors"
	"github.com/jackdai123/gnet/internal/logging"
	"github.com/jackdai123/gnet/internal/netpoll"
)

type server struct {
	ln           *listener                // the listener for accepting new connections
	lb           loadBalancer             // event-loops for handling events
	wg           sync.WaitGroup           // event-loop close WaitGroup
	opts         *Options                 // options with server
	once         sync.Once                // make sure only signalShutdown once
	cond         *sync.Cond               // shutdown signaler
	codec        ICodec                   // codec for TCP stream
	logger       logging.Logger           // customized logger for logging info
	ticktock     chan time.Duration       // ticker channel
	mainLoop     *eventloop               // main event-loop for accepting connections
	inShutdown   int32                    // whether the server is in shutdown
	eventHandler EventHandler             // user eventHandler
	tw           *timingwheel.TimingWheel //时间轮
}

var serverFarm sync.Map //一维map，key是网络地址、Serial或Network，value是server指针

func (svr *server) isInShutdown() bool {
	return atomic.LoadInt32(&svr.inShutdown) == 1
}

// waitForShutdown waits for a signal to shutdown.
func (svr *server) waitForShutdown() {
	svr.cond.L.Lock()
	svr.cond.Wait()
	svr.cond.L.Unlock()
}

// signalShutdown signals the server to shut down.
func (svr *server) signalShutdown() {
	svr.once.Do(func() {
		svr.cond.L.Lock()
		svr.cond.Signal()
		svr.cond.L.Unlock()
	})
}

func (svr *server) startEventLoops(protoAddr string) {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			el.loopRun(svr.opts.LockOSThread, protoAddr)
			svr.wg.Done()
		}()
		return true
	})
}

func (svr *server) closeEventLoops() {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		_ = el.poller.Close()
		return true
	})
}

func (svr *server) startSubReactors() {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			svr.activateSubReactor(el, svr.opts.LockOSThread)
			svr.wg.Done()
		}()
		return true
	})
}

func (svr *server) activateEventLoops(protoAddr string, numEventLoop int) (err error) {
	// Create loops locally and bind the listeners.
	for i := 0; i < numEventLoop; i++ {
		l := svr.ln
		if i > 0 && svr.opts.ReusePort && l != nil { // 主动服务无需监听服务端口
			if l, err = initListener(svr.ln.network, svr.ln.addr, svr.opts); err != nil {
				return
			}
		}

		var p *netpoll.Poller
		if p, err = netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.ln = l
			el.svr = svr
			el.poller = p
			el.packet = make([]byte, svr.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			//el.mapTimer = make(map[int]*timingwheel.Timer)
			el.eventHandler = svr.eventHandler
			el.calibrateCallback = svr.lb.calibrate
			if el.ln != nil { // 主动服务无需监听服务端口
				_ = el.poller.AddRead(el.ln.fd)
			}
			svr.lb.register(el)

			// Start the ticker.
			if el.idx == 0 && svr.opts.Ticker {
				go el.loopTicker()
			}
		} else {
			return
		}
	}

	// Start event-loops in background.
	svr.startEventLoops(protoAddr)

	return
}

func (svr *server) activateReactors(numEventLoop int) error {
	for i := 0; i < numEventLoop; i++ {
		if p, err := netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.ln = svr.ln
			el.svr = svr
			el.poller = p
			el.packet = make([]byte, svr.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			el.eventHandler = svr.eventHandler
			el.calibrateCallback = svr.lb.calibrate
			svr.lb.register(el)

			// Start the ticker.
			if el.idx == 0 && svr.opts.Ticker {
				go el.loopTicker()
			}
		} else {
			return err
		}
	}

	// Start sub reactors in background.
	svr.startSubReactors()

	if p, err := netpoll.OpenPoller(); err == nil {
		el := new(eventloop)
		el.ln = svr.ln
		el.idx = -1
		el.svr = svr
		el.poller = p
		_ = el.poller.AddRead(el.ln.fd)
		svr.mainLoop = el

		// Start main reactor in background.
		svr.wg.Add(1)
		go func() {
			svr.activateMainReactor(svr.opts.LockOSThread)
			svr.wg.Done()
		}()
	} else {
		return err
	}

	return nil
}

func (svr *server) start(protoAddr string, numEventLoop int) error {
	// 主动服务（ln为nil）直接激活EventLoops
	if svr.ln == nil || svr.opts.ReusePort || svr.ln.network == "udp" {
		return svr.activateEventLoops(protoAddr, numEventLoop)
	}

	return svr.activateReactors(numEventLoop)
}

func (svr *server) stop(s Server) {
	// Wait on a signal for shutdown
	svr.waitForShutdown()

	svr.eventHandler.OnShutdown(s)

	// Notify all loops to close by closing all listeners
	svr.lb.iterate(func(i int, el *eventloop) bool {
		sniffErrorAndLog(el.poller.Trigger(func() error {
			return errors.ErrServerShutdown
		}))
		return true
	})

	if svr.mainLoop != nil {
		svr.ln.close()
		sniffErrorAndLog(svr.mainLoop.poller.Trigger(func() error {
			return errors.ErrServerShutdown
		}))
	}

	// Wait on all loops to complete reading events
	svr.wg.Wait()

	svr.closeEventLoops()

	if svr.mainLoop != nil {
		sniffErrorAndLog(svr.mainLoop.poller.Close())
	}

	// 停止时间轮
	if svr.tw != nil {
		svr.tw.Stop()
	}

	// Stop the ticker.
	if svr.opts.Ticker {
		close(svr.ticktock)
	}

	atomic.StoreInt32(&svr.inShutdown, 1)
}

func serve(eventHandler EventHandler, listener *listener, options *Options, protoAddr string) error {
	// Figure out the proper number of event-loops/goroutines to run.
	numEventLoop := 1
	if options.Multicore {
		numEventLoop = runtime.NumCPU()
	}
	if options.NumEventLoop > 1 {
		numEventLoop = options.NumEventLoop
	}

	svr := new(server)
	svr.opts = options
	svr.eventHandler = eventHandler
	svr.ln = listener

	switch options.LB {
	case RoundRobin:
		svr.lb = new(roundRobinLoadBalancer)
	case LeastConnections:
		svr.lb = new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		svr.lb = new(sourceAddrHashLoadBalancer)
	default:
		return errors.ErrUnsupportedLoadBalancer
	}

	svr.cond = sync.NewCond(&sync.Mutex{})
	svr.ticktock = make(chan time.Duration, channelBuffer)
	svr.logger = logging.DefaultLogger
	svr.codec = func() ICodec {
		if options.Codec == nil {
			return new(BuiltInFrameCodec)
		}
		return options.Codec
	}()

	//默认串口超时区间在10ms到5000ms，串口命令间隔时间最小XXms
	var tick time.Duration = 10 * time.Millisecond
	var size int64 = 500
	if options.TimingWheelTick > 0 {
		tick = options.TimingWheelTick
	}
	if options.TimingWheelSize > 0 {
		size = options.TimingWheelSize
	}

	//主动服务开启时间轮
	if svr.ln == nil {
		svr.tw = timingwheel.NewTimingWheel(tick, size)
		svr.tw.Start()
	}

	server := Server{
		svr:          svr,
		Multicore:    options.Multicore,
		Addr:         listener.lnaddr,
		NumEventLoop: numEventLoop,
		ReusePort:    options.ReusePort,
		TCPKeepAlive: options.TCPKeepAlive,
	}
	switch svr.eventHandler.OnInitComplete(server) {
	case None:
	case Shutdown:
		return nil
	}

	if err := svr.start(protoAddr, numEventLoop); err != nil {
		svr.closeEventLoops()
		svr.logger.Errorf("gnet server is stopping with error: %v", err)
		return err
	}
	defer svr.stop(server)

	serverFarm.Store(protoAddr, svr)

	return nil
}

//二维map，key1是网络地址、串口文件路径，key2是fd，value是EndPoint
var endpointFarm map[string]map[int]endpoint.EndPoint
var endpointFarmMutex sync.RWMutex
var endpointFarmOnce sync.Once

// 打开并管理IO资源
func openEndpoint(protoAddr string, c endpoint.EndPointConfig) (p endpoint.EndPoint, err error) {
	endpointFarmOnce.Do(func() {
		endpointFarm = make(map[string]map[int]endpoint.EndPoint)
	})

	if protoAddr == "Serial" { //串口和串口服务器
		if ok := isEndpointExist(c.AddressName()); ok { // 不可以重复打开串口和串口服务器
			err = errors.ErrSerialOpenRepeated
		} else {
			if p, err = endpoint.Open(c); err == nil {
				storeEndpoint(c.AddressName(), p)
			}
		}
	} else { //网口和UnixSocket
		if p, err = endpoint.Open(c); err == nil {
			storeEndpoint(c.AddressName(), p) // 网络可以重复打开
		}
	}
	return
}

// 迭代endpointFarm
func iterateEndpoint(addressName string, f func(endpoint.EndPoint) error) error {
	endpointFarmMutex.RLock()
	defer endpointFarmMutex.RUnlock()

	if m, ok := endpointFarm[addressName]; ok {
		for _, p := range m {
			if err := f(p); err != nil {
				return err
			}
		}
	}

	return nil
}

// 根据IO资源名，删除IO资源所有endpoint
func deleteEndpointAll(addressName string) {
	endpointFarmMutex.Lock()
	defer endpointFarmMutex.Unlock()

	delete(endpointFarm, addressName)
}

// 根据IO资源名和fd，删除IO资源一个endpoint
func deleteEndpoint(addressName string, fd int) {
	endpointFarmMutex.Lock()
	defer endpointFarmMutex.Unlock()

	if m, ok := endpointFarm[addressName]; ok {
		delete(m, fd)
	}
}

// 检查EndPoint是否存在
func isEndpointExist(addressName string) bool {
	endpointFarmMutex.RLock()
	defer endpointFarmMutex.RUnlock()

	if _, ok := endpointFarm[addressName]; ok {
		return true
	} else {
		return false
	}
}

// 存储EndPoint到endpointFarm
func storeEndpoint(addressName string, p endpoint.EndPoint) {
	endpointFarmMutex.Lock()
	defer endpointFarmMutex.Unlock()

	if m, ok := endpointFarm[addressName]; ok {
		m[p.Fd()] = p
	} else {
		endpointFarm[addressName] = map[int]endpoint.EndPoint{p.Fd(): p}
	}
}
