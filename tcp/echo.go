package tcp

import (
	"bufio"
	"context"
	"github.com/eurekawm/godis/lib/logger"
	"github.com/eurekawm/godis/lib/sync/atomic"
	"github.com/eurekawm/godis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// EchoClient 客户端数据结构
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// Close implement of io.Closer
func (echoClient *EchoClient) Close() error {
	// 等待业务完成 10s 再关闭
	echoClient.Waiting.WaitWithTimeout(10 * time.Second)
	_ = echoClient.Conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

func (echoHandler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if echoHandler.closing.Get() {
		_ = conn.Close()
	}
	client := &EchoClient{
		Conn: conn,
	}
	echoHandler.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				echoHandler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 开始做业务
		client.Waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

// Close 关闭处理引擎 要处理里面所有的client
func (echoHandler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	echoHandler.closing.Set(true)
	echoHandler.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Conn.Close()
		return true
	})
	return nil
}
