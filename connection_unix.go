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

//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package gnet

import (
	"net"
	"os"
	"syscall"
	"time"

	"github.com/jackdai123/endpoint"
	"github.com/jackdai123/gnet/internal/socket"
	"github.com/panjf2000/gnet/pool/bytebuffer"
	prb "github.com/panjf2000/gnet/pool/ringbuffer"
	"github.com/panjf2000/gnet/ringbuffer"
	"golang.org/x/sys/unix"
)

type conn struct {
	fd             int                    // file descriptor
	sa             syscall.Sockaddr       // remote socket address
	ctx            interface{}            // user-defined context
	loop           *eventloop             // connected event-loop
	codec          ICodec                 // codec for TCP
	buffer         []byte                 // reuse memory of inbound data as a temporary buffer
	opened         bool                   // connection opened event fired
	localAddr      net.Addr               // local addr
	remoteAddr     net.Addr               // remote addr
	byteBuffer     *bytebuffer.ByteBuffer // bytes buffer for buffering current packet and data in ring-buffer
	inboundBuffer  *ringbuffer.RingBuffer // buffer for data from client
	outboundBuffer *ringbuffer.RingBuffer // buffer for data that is ready to write to client
	endpointType   endpoint.EndPointType  // 连接的IO资源类型
	readTimeout    time.Duration          // 一次完整数据包读取超时
	writeTimeout   time.Duration          // 一次完整数据包发送超时
}

func newConn(p endpoint.EndPoint, el *eventloop, codec ICodec) (c *conn) {
	switch p.Type() {
	case endpoint.EndPointTCP, endpoint.EndPointUnix, endpoint.EndPointSerial:
		c = newTCPConn(p.Fd(), el, p.SockAddr(), p.NetAddr())
	case endpoint.EndPointUDP:
		c = newUDPConn(p.Fd(), el, p.SockAddr())
	}

	c.endpointType = p.Type()
	c.readTimeout = p.ReadTimeout()
	c.writeTimeout = p.WriteTimeout()
	if codec != nil {
		c.codec = codec
	}
	return
}

func (c *conn) releaseConn() {
	switch c.endpointType {
	case endpoint.EndPointTCP, endpoint.EndPointUnix, endpoint.EndPointSerial:
		c.releaseTCP()
	case endpoint.EndPointUDP:
		c.releaseUDP()
	}
}

func newTCPConn(fd int, el *eventloop, sa syscall.Sockaddr, remoteAddr net.Addr) (c *conn) {
	var addr net.Addr
	if el.ln != nil {
		addr = el.ln.lnaddr
	}

	return &conn{
		fd:             fd,
		sa:             sa,
		loop:           el,
		codec:          el.svr.codec,
		localAddr:      addr,
		remoteAddr:     remoteAddr,
		inboundBuffer:  prb.Get(),
		outboundBuffer: prb.Get(),
		endpointType:   endpoint.EndPointTCP,
	}
}

func (c *conn) releaseTCP() {
	c.opened = false
	c.sa = nil
	c.ctx = nil
	c.buffer = nil
	c.localAddr = nil
	c.remoteAddr = nil
	prb.Put(c.inboundBuffer)
	prb.Put(c.outboundBuffer)
	c.inboundBuffer = ringbuffer.EmptyRingBuffer
	c.outboundBuffer = ringbuffer.EmptyRingBuffer
	bytebuffer.Put(c.byteBuffer)
	c.byteBuffer = nil
}

func newUDPConn(fd int, el *eventloop, sa syscall.Sockaddr) *conn {
	var addr net.Addr
	if el.ln != nil {
		addr = el.ln.lnaddr
	}

	return &conn{
		fd:           fd,
		sa:           sa,
		localAddr:    addr,
		remoteAddr:   socket.SockaddrToUDPAddr(sa),
		endpointType: endpoint.EndPointUDP,
	}
}

func (c *conn) releaseUDP() {
	c.opened = false
	c.ctx = nil
	c.localAddr = nil
	c.remoteAddr = nil
}

func (c *conn) open(buf []byte) {
	n, err := unix.Write(c.fd, buf)
	if err != nil {
		_, _ = c.outboundBuffer.Write(buf)
		return
	}

	if n < len(buf) {
		_, _ = c.outboundBuffer.Write(buf[n:])
	}
}

// 区别于open，需对数据Encode再发，同时支持UDP
func (c *conn) open2(buf []byte) (err error) {
	var outFrame []byte
	if outFrame, err = c.codec.Encode(c, buf); err != nil {
		c.loop.eventHandler.OnError(c, ErrEncode, err) //IO系统调用出现异常，通知上层业务
		return
	}

	//UDP发送
	if c.endpointType == endpoint.EndPointUDP {
		if err = c.sendTo(outFrame); err != nil {
			c.loop.eventHandler.OnError(c, ErrSendTo, err) //IO系统调用出现异常，通知上层业务
		}
		return
	}

	//TCP、UnixSocket、串口发送
	//串口写缓冲区足够大（超过1k），通常发包不超100字节，故不存在写超时一说
	n, err := unix.Write(c.fd, outFrame)
	if err != nil {
		_, _ = c.outboundBuffer.Write(outFrame)
		return nil
	}

	if n < len(outFrame) {
		_, _ = c.outboundBuffer.Write(outFrame[n:])
	}

	return
}

func (c *conn) read() ([]byte, error) {
	return c.codec.Decode(c)
}

func (c *conn) write(buf []byte) (err error) {
	var outFrame []byte
	if outFrame, err = c.codec.Encode(c, buf); err != nil {
		c.loop.eventHandler.OnError(c, ErrEncode, err) //IO系统调用出现异常，通知上层业务
		return
	}
	// If there is pending data in outbound buffer, the current data ought to be appended to the outbound buffer
	// for maintaining the sequence of network packets.
	if !c.outboundBuffer.IsEmpty() {
		_, _ = c.outboundBuffer.Write(outFrame)
		return
	}

	var n int
	if n, err = unix.Write(c.fd, outFrame); err != nil {
		// A temporary error occurs, append the data to outbound buffer, writing it back to client in the next round.
		if err == unix.EAGAIN {
			_, _ = c.outboundBuffer.Write(outFrame)
			if err = c.loop.poller.ModReadWrite(c.fd); err != nil {
				c.loop.eventHandler.OnError(c, ErrPollModReadWrite, err) //IO系统调用出现异常，通知上层业务
			}
			return
		}
		c.loop.eventHandler.OnError(c, ErrWrite, err) //IO系统调用出现异常，通知上层业务
		return c.loop.loopCloseConn(c, os.NewSyscallError("write", err))
	}
	// Fail to send all data back to client, buffer the leftover data for the next round.
	if n < len(outFrame) {
		_, _ = c.outboundBuffer.Write(outFrame[n:])
		if err = c.loop.poller.ModReadWrite(c.fd); err != nil {
			c.loop.eventHandler.OnError(c, ErrPollModReadWrite, err) //IO系统调用出现异常，通知上层业务
		}
	} else { //通知上层业务发送完成
		c.loop.eventHandler.OnSendSuccess(c)
	}
	return
}

func (c *conn) sendTo(buf []byte) error {
	return syscall.Sendto(c.fd, buf, 0, c.sa)
}

// ================================= Public APIs of gnet.Conn =================================

func (c *conn) Read() []byte {
	if c.inboundBuffer.IsEmpty() {
		return c.buffer
	}
	c.byteBuffer = c.inboundBuffer.WithByteBuffer(c.buffer)
	return c.byteBuffer.Bytes()
}

func (c *conn) ResetBuffer() {
	c.buffer = c.buffer[:0]
	c.inboundBuffer.Reset()
	bytebuffer.Put(c.byteBuffer)
	c.byteBuffer = nil
}

func (c *conn) ReadN(n int) (size int, buf []byte) {
	inBufferLen := c.inboundBuffer.Length()
	tempBufferLen := len(c.buffer)
	if totalLen := inBufferLen + tempBufferLen; totalLen < n || n <= 0 {
		n = totalLen
	}
	size = n
	if c.inboundBuffer.IsEmpty() {
		buf = c.buffer[:n]
		return
	}
	head, tail := c.inboundBuffer.LazyRead(n)
	c.byteBuffer = bytebuffer.Get()
	_, _ = c.byteBuffer.Write(head)
	_, _ = c.byteBuffer.Write(tail)
	if inBufferLen >= n {
		buf = c.byteBuffer.Bytes()
		return
	}

	restSize := n - inBufferLen
	_, _ = c.byteBuffer.Write(c.buffer[:restSize])
	buf = c.byteBuffer.Bytes()
	return
}

func (c *conn) ShiftN(n int) (size int) {
	inBufferLen := c.inboundBuffer.Length()
	tempBufferLen := len(c.buffer)
	if inBufferLen+tempBufferLen < n || n <= 0 {
		c.ResetBuffer()
		size = inBufferLen + tempBufferLen
		return
	}
	size = n
	if c.inboundBuffer.IsEmpty() {
		c.buffer = c.buffer[n:]
		return
	}

	bytebuffer.Put(c.byteBuffer)
	c.byteBuffer = nil

	if inBufferLen >= n {
		c.inboundBuffer.Shift(n)
		return
	}
	c.inboundBuffer.Reset()

	restSize := n - inBufferLen
	c.buffer = c.buffer[restSize:]
	return
}

func (c *conn) BufferLength() int {
	return c.inboundBuffer.Length() + len(c.buffer)
}

func (c *conn) AsyncWrite(buf []byte) (err error) {
	if err = c.loop.poller.Trigger(func() error {
		if c.opened {
			return c.write(buf)
		}
		return nil
	}); err != nil {
		c.loop.eventHandler.OnError(c, ErrPollTrigger, err)
	}
	return
}

func (c *conn) SendTo(buf []byte) error {
	return c.sendTo(buf)
}

func (c *conn) Wake() error {
	return c.loop.poller.Trigger(func() error {
		return c.loop.loopWake(c)
	})
}

func (c *conn) Close() error {
	return c.loop.poller.Trigger(func() error {
		return c.loop.loopCloseConn(c, nil)
	})
}

func (c *conn) Context() interface{}       { return c.ctx }
func (c *conn) SetContext(ctx interface{}) { c.ctx = ctx }
func (c *conn) LocalAddr() net.Addr        { return c.localAddr }
func (c *conn) RemoteAddr() net.Addr       { return c.remoteAddr }
func (c *conn) Fd() int                    { return c.fd }
