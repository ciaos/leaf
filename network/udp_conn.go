package network

import (
	"net"
	"sync"

	"github.com/ciaos/leaf/kcp"
	"github.com/ciaos/leaf/log"
)

type UDPConnSet map[*kcp.UDPSession]struct{}

type UDPConn struct {
	sync.Mutex
	conn      *kcp.UDPSession
	writeChan chan []byte
	closeFlag bool
	msgParser *UDPMsgParser
}

func newUDPConn(conn *kcp.UDPSession, pendingWriteNum int, msgParser *UDPMsgParser) *UDPConn {
	udpConn := new(UDPConn)
	udpConn.conn = conn
	udpConn.writeChan = make(chan []byte, pendingWriteNum)
	udpConn.msgParser = msgParser

	go func() {
		for b := range udpConn.writeChan {
			if b == nil {
				break
			}

			_, err := conn.Write(b)
			if err != nil {
				break
			}
		}

		conn.Close()
		udpConn.Lock()
		udpConn.closeFlag = true
		udpConn.Unlock()
	}()

	return udpConn
}

func (udpConn *UDPConn) doDestroy() {
	udpConn.conn.Close()

	if !udpConn.closeFlag {
		close(udpConn.writeChan)
		udpConn.closeFlag = true
	}
}

func (udpConn *UDPConn) Destroy() {
	udpConn.Lock()
	defer udpConn.Unlock()

	udpConn.doDestroy()
}

func (udpConn *UDPConn) Close() {
	udpConn.Lock()
	defer udpConn.Unlock()
	if udpConn.closeFlag {
		return
	}

	udpConn.doWrite(nil)
	udpConn.closeFlag = true
}

func (udpConn *UDPConn) doWrite(b []byte) {
	if len(udpConn.writeChan) == cap(udpConn.writeChan) {
		log.Debug("close conn: channel full")
		udpConn.doDestroy()
		return
	}

	udpConn.writeChan <- b
}

// b must not be modified by the others goroutines
func (udpConn *UDPConn) Write(b []byte) {
	udpConn.Lock()
	defer udpConn.Unlock()
	if udpConn.closeFlag || b == nil {
		return
	}

	udpConn.doWrite(b)
}

func (udpConn *UDPConn) Read(b []byte) (int, error) {
	return udpConn.conn.Read(b)
}

func (udpConn *UDPConn) LocalAddr() net.Addr {
	return udpConn.conn.LocalAddr()
}

func (udpConn *UDPConn) RemoteAddr() net.Addr {
	return udpConn.conn.RemoteAddr()
}

func (udpConn *UDPConn) ReadMsg() ([]byte, error) {
	return udpConn.msgParser.Read(udpConn)
}

func (udpConn *UDPConn) WriteMsg(args ...[]byte) error {
	return udpConn.msgParser.Write(udpConn, args...)
}
