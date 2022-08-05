package parser

import (
	"bufio"
	"errors"
	"github.com/eurekawm/godis/interface/resp"
	"io"
)

type Payload struct {
	Data resp.Reply
	Err  error
}
type readState struct {
	// 解析单行数据还是多行
	readingMultiLine bool
	// 读取的指令应该有几个参数
	expectedArgsCount int
	// 用户消息类型
	msgType byte
	// 用户传递过来的数据本身 set key value
	args [][]byte
	// 数据块的长度
	bulkLen int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	// 业务和解析是异步的
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
}

func readLine(reader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	if state.bulkLen == 0 {
		// 1 没有任何的$指令 直接根据\r\n 切分
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] == '\r' {
			return nil, false, errors.New("protocol error")
		}
	} else {
		// 2如果之前读到了$数字 严格读取字符个数
		// 实际内容+2 /r/n
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] == '\r' || msg[len(msg)-1] == '\n' {
			return nil, false, errors.New("protocol error")
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}
