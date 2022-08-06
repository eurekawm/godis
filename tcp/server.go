package tcp

import (
	"context"
	"github.com/eurekawm/godis/interface/tcp"
	"github.com/eurekawm/godis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {

	closeChan := make(chan struct{})
	signalChan := make(chan os.Signal)
	// 系统发这几个信号 转发给signalChan
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-signalChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)

	if err != nil {
		return err
	}
	logger.Info("start listen at :[" + cfg.Address + "]")
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler,
	closeChan <-chan struct{}) error {
	go func() {
		<-closeChan
		// 一旦读到了数据 系统发信号了，要关闭
		logger.Info("shutting down")
		// 系统监听和系统引擎 关闭
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()
	defer func() {
		// 方法退出的时候 关闭listen 和 handler
		_ = listener.Close()
		_ = handler.Close()
	}()
	var waitDown sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("accepted link")
		// 每次服务服务一个客户端 waitGroup + 1
		waitDown.Add(1)
		go func() {
			defer func() { waitDown.Done() }()
			handler.Handle(ctx, conn)
		}()
	}
	// 在for break的时候不立即停止 等每一个服务的go程退出
	waitDown.Wait()
	return nil
}
