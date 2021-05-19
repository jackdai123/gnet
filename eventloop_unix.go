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
	"errors"
	"fmt"
	"os"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	//"github.com/RussellLuo/timingwheel"
	"github.com/jackdai123/endpoint"
	gerrors "github.com/jackdai123/gnet/errors"
	"github.com/jackdai123/gnet/internal/netpoll"
	"github.com/jackdai123/gnet/internal/socket"
	"golang.org/x/sys/unix"
)

type eventloop struct {
	internalEventloop

	// Prevents eventloop from false sharing by padding extra memory with the difference
	// between the cache line size "s" and (eventloop mod s) for the most common CPU architectures.
	_ [64 - unsafe.Sizeof(internalEventloop{})%64]byte
}

type internalEventloop struct {
	ln                *listener               // listener
	idx               int                     // loop index in the server loops list
	svr               *server                 // server in loop
	poller            *netpoll.Poller         // epoll or kqueue
	packet            []byte                  // read packet buffer whose capacity is 64KB
	connCount         int32                   // number of active connections in event-loop
	connections       map[int]*conn           // loop connections fd -> conn
	eventHandler      EventHandler            // user eventHandler
	calibrateCallback func(*eventloop, int32) // callback func for re-adjusting connCount
	//mapTimer          map[int]*timingwheel.Timer // 串口连接与时间轮超时映射表
}

func (el *eventloop) closeAllConns() {
	// Close loops and all outstanding connections
	for _, c := range el.connections {
		_ = el.loopCloseConn(c, nil)
	}
}

func (el *eventloop) loopRun(lockOSThread bool, protoAddr string) {
	if lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	defer func() {
		el.closeAllConns()
		if el.ln != nil { // 主动服务无需监听服务端口
			el.ln.close()
		}
		el.svr.signalShutdown()
	}()

	var err error
	if protoAddr == "Serial" { //串口Polling区别于网口
		err = el.poller.PollingSerial(el.handleEvent)
	} else {
		err = el.poller.Polling(el.handleEvent)
	}

	if err != gerrors.ErrServerShutdown { //IO轮询出现异常，通知上层业务
		el.eventHandler.OnError(nil, ErrPollPolling, err)
	}
	el.svr.logger.Infof("Event-loop(%d) is exiting due to error: %v", el.idx, err)
}

func (el *eventloop) loopAccept(fd int) error {
	if fd == el.ln.fd {
		if el.ln.network == "udp" {
			return el.loopReadUDP(fd)
		}

		nfd, sa, err := syscall.Accept(fd)
		if err != nil {
			if err == unix.EAGAIN {
				return nil
			}
			return os.NewSyscallError("accept", err)
		}
		if err = os.NewSyscallError("fcntl nonblock", unix.SetNonblock(nfd, true)); err != nil {
			return err
		}

		netAddr := socket.SockaddrToTCPOrUnixAddr(sa)
		c := newTCPConn(nfd, el, sa, netAddr)
		if err = el.poller.AddRead(c.fd); err == nil {
			el.connections[c.fd] = c
			return el.loopOpen(c)
		}
		return err
	}

	return nil
}

func (el *eventloop) loopOpen(c *conn) error {
	c.opened = true
	el.calibrateCallback(el, 1)

	out, action := el.eventHandler.OnOpened(c)
	if out != nil {
		if el.ln == nil { //主动服务，打开IO资源即发包
			el.eventHandler.PreWrite()
			if err := c.open2(out); err != nil {
				return el.loopCloseConn(c, err)
			}
		} else { //被动服务
			c.open(out)
		}
	}

	if c.endpointType != endpoint.EndPointUDP && !c.outboundBuffer.IsEmpty() {
		if err := el.poller.AddWrite(c.fd); err != nil {
			el.eventHandler.OnError(c, ErrPollAddWrite, err)
		}
	} else {
		//设置读超时
		//el.mapTimer[c.fd] = el.svr.tw.AfterFunc(c.readTimeout, func() {
		//})

		//第一次发送完成，通知上层业务发送完成
		el.eventHandler.OnSendSuccess(c)
	}

	fmt.Printf("eventloop(idx=%v connCount=%v) open remote(addr=%v fd=%v)\n", el.idx, el.connCount, c.remoteAddr, c.fd)

	return el.handleAction(c, action)
}

func (el *eventloop) loopRead(c *conn) error {
	n, err := unix.Read(c.fd, el.packet)
	if n == 0 || err != nil {
		if err == unix.EAGAIN {
			return nil
		}
		el.eventHandler.OnError(c, ErrRead, err) //IO系统调用出现异常，通知上层业务
		return el.loopCloseConn(c, os.NewSyscallError("read", err))
	}
	c.buffer = el.packet[:n]

	for inFrame, _ := c.read(); inFrame != nil; inFrame, _ = c.read() {
		out, timeWait, action := el.eventHandler.React(inFrame, c)
		if out != nil {
			if timeWait > 0 { //等待超时再发送，比如串口命令间隔
				el.svr.tw.AfterFunc(timeWait, func() {
					el.eventHandler.PreWrite()
					c.AsyncWrite(out)
				})
			} else {
				el.eventHandler.PreWrite()
				// Encode data and try to write it back to the client, this attempt is based on a fact:
				// a client socket waits for the response data after sending request data to the server,
				// which makes the client socket writable.
				if err = c.write(out); err != nil {
					return err
				}
			}
		}

		switch action {
		case None:
		case Close:
			return el.loopCloseConn(c, nil)
		case Shutdown:
			return gerrors.ErrServerShutdown
		}

		// Check the status of connection every loop since it might be closed during writing data back to client due to
		// some kind of system error.
		if !c.opened {
			return nil
		}
	}
	_, _ = c.inboundBuffer.Write(c.buffer)

	return nil
}

func (el *eventloop) loopWrite(c *conn) error {
	el.eventHandler.PreWrite()

	head, tail := c.outboundBuffer.LazyReadAll()
	n, err := unix.Write(c.fd, head)
	if err != nil {
		if err == unix.EAGAIN {
			return nil
		}
		el.eventHandler.OnError(c, ErrWrite, err) //IO系统调用出现异常，通知上层业务
		return el.loopCloseConn(c, os.NewSyscallError("write", err))
	}
	c.outboundBuffer.Shift(n)

	if n == len(head) && tail != nil {
		n, err = unix.Write(c.fd, tail)
		if err != nil {
			if err == unix.EAGAIN {
				return nil
			}
			el.eventHandler.OnError(c, ErrWrite, err) //IO系统调用出现异常，通知上层业务
			return el.loopCloseConn(c, os.NewSyscallError("write", err))
		}
		c.outboundBuffer.Shift(n)
	}

	// All data have been drained, it's no need to monitor the writable events,
	// remove the writable event from poller to help the future event-loops.
	if c.outboundBuffer.IsEmpty() {
		if err := el.poller.ModRead(c.fd); err != nil {
			el.eventHandler.OnError(c, ErrPollModRead, err)
		}
		//缓冲区发送完成，通知上层业务发送完成
		el.eventHandler.OnSendSuccess(c)
		//等待接收前需设置读超时
		//el.mapTimer[c.fd] = el.svr.tw.AfterFunc(c.readTimeout, func() {
		//})
	}

	return nil
}

func (el *eventloop) loopCloseConn(c *conn, err error) (rerr error) {
	if !c.opened {
		//return fmt.Errorf("the fd=%d in event-loop(%d) is already closed, skipping it", c.fd, el.idx)
		return nil
	}

	// Send residual data in buffer back to client before actually closing the connection.
	if el.ln != nil && c.endpointType != endpoint.EndPointUDP && !c.outboundBuffer.IsEmpty() {
		el.eventHandler.PreWrite()

		head, tail := c.outboundBuffer.LazyReadAll()
		if n, err0 := unix.Write(c.fd, head); err0 == nil {
			if n == len(head) && tail != nil {
				_, _ = unix.Write(c.fd, tail)
			}
		}
	}

	if el.ln == nil {
		//主动服务，关闭socket前清空系统缓冲区中的数据
		if c.endpointType == endpoint.EndPointUDP {
			if n, _, err0 := syscall.Recvfrom(c.fd, el.packet, 0); err0 == nil {
				el.eventHandler.React(el.packet[:n], c)
			}
		} else {
			if n, err0 := unix.Read(c.fd, el.packet); err0 == nil {
				c.buffer = el.packet[:n]
				for inFrame, _ := c.read(); inFrame != nil; inFrame, _ = c.read() {
					el.eventHandler.React(inFrame, c)
				}
			}
		}

		//endpointFarm与connection同步释放
		deleteEndpoint(c.RemoteAddr().String(), c.fd)
	}

	if err0, err1 := el.poller.Delete(c.fd), unix.Close(c.fd); err0 == nil && err1 == nil {
		delete(el.connections, c.fd)
		el.calibrateCallback(el, -1)
		if el.eventHandler.OnClosed(c, err) == Shutdown { //连接关闭，通过OnClosed回调，通知外部（比如重连）
			return gerrors.ErrServerShutdown
		}
		c.releaseConn()
	} else {
		if err0 != nil {
			el.eventHandler.OnError(c, ErrPollDelete, err0)
			rerr = fmt.Errorf("failed to delete fd=%d from poller in event-loop(%d): %v", c.fd, el.idx, err0)
		}
		if err1 != nil {
			el.eventHandler.OnError(c, ErrClose, err1)
			err1 = fmt.Errorf("failed to close fd=%d in event-loop(%d): %v", c.fd, el.idx, os.NewSyscallError("close", err1))
			if rerr != nil {
				rerr = errors.New(rerr.Error() + " & " + err1.Error())
			} else {
				rerr = err1
			}
		}
	}

	return
}

func (el *eventloop) loopWake(c *conn) error {
	if co, ok := el.connections[c.fd]; !ok || co != c {
		return nil // ignore stale wakes.
	}

	out, _, action := el.eventHandler.React(nil, c)
	if out != nil {
		if err := c.write(out); err != nil {
			return err
		}
	}

	return el.handleAction(c, action)
}

func (el *eventloop) loopTicker() {
	var (
		delay time.Duration
		open  bool
		err   error
	)
	for {
		err = el.poller.Trigger(func() (err error) {
			delay, action := el.eventHandler.Tick()
			el.svr.ticktock <- delay
			switch action {
			case None:
			case Shutdown:
				err = gerrors.ErrServerShutdown
			}
			return
		})
		if err != nil {
			el.svr.logger.Errorf("Failed to awake poller in event-loop(%d), error:%v, stopping ticker", el.idx, err)
			break
		}
		if delay, open = <-el.svr.ticktock; open {
			time.Sleep(delay)
		} else {
			break
		}
	}
}

func (el *eventloop) handleAction(c *conn, action Action) error {
	switch action {
	case None:
		return nil
	case Close:
		return el.loopCloseConn(c, nil)
	case Shutdown:
		return gerrors.ErrServerShutdown
	default:
		return nil
	}
}

func (el *eventloop) loopReadUDP(fd int) error {
	n, sa, err := syscall.Recvfrom(fd, el.packet, 0)
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return nil
		}
		return fmt.Errorf("failed to read UDP packet from fd=%d in event-loop(%d), %v",
			fd, el.idx, os.NewSyscallError("recvfrom", err))
	}

	c := newUDPConn(fd, el, sa)
	out, _, action := el.eventHandler.React(el.packet[:n], c)
	if out != nil {
		el.eventHandler.PreWrite()
		_ = c.sendTo(out)
	}
	if action == Shutdown {
		return gerrors.ErrServerShutdown
	}
	c.releaseUDP()

	return nil
}

func (el *eventloop) loopReadUDP2(c *conn) error {
	n, _, err := syscall.Recvfrom(c.fd, el.packet, 0)
	if err != nil {
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			return nil
		}
		//return fmt.Errorf("failed to read UDP packet from fd=%d in event-loop(%d), %v",
		//	c.fd, el.idx, os.NewSyscallError("recvfrom", err))
		//主动服务recvfrom失败，通知上层业务，触发IO资源回收
		el.eventHandler.OnError(c, ErrRecvFrom, err)
		return el.loopCloseConn(c, os.NewSyscallError("recvfrom", err))
	}

	out, _, action := el.eventHandler.React(el.packet[:n], c)
	if out != nil {
		el.eventHandler.PreWrite()
		//UDP发包，成功和失败均通知上层业务
		if c.sendTo(out) == nil {
			el.eventHandler.OnSendSuccess(c)
		} else {
			el.eventHandler.OnError(c, ErrSendTo, err)
		}
	}

	switch action {
	case None:
	case Close:
		return el.loopCloseConn(c, nil)
	case Shutdown:
		return gerrors.ErrServerShutdown
	}

	return nil
}
