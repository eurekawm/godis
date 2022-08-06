package parser

import (
	"bufio"
	"errors"
	"github.com/eurekawm/godis/interface/resp"
	"github.com/eurekawm/godis/lib/logger"
	"github.com/eurekawm/godis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	// 解析单行数据还是多行
	readingMultiLine bool
	// 读取的指令应该有几个参数 set key value = 3个
	expectedArgsCount int
	// 用户消息类型
	msgType byte
	// 用户传递过来的数据本身 set key value
	args [][]byte
	// 数据块的长度 set长度3 key长度3 value长度5
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
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for true {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		// 处理错误
		if err != nil {
			// io错误返回并关闭通道
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			// 协议错误 解析下一行
			ch <- &Payload{Err: err}
			state = readState{}
			continue
		}
		// 判断是否为多行解析模式 初始化状态
		// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5value\r\n
		if !state.readingMultiLine {
			if msg[0] == '*' {
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{Data: &reply.EmptyMultiBulkReply{}}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				// $4\r\nPING\r\n
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}
				// $-1\r\n
				if state.bulkLen == -1 {
					ch <- &Payload{Data: &reply.NullBulkReply{}}
					state = readState{}
					continue
				}
			} else {
				// parseSingleLine
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{Data: result, Err: err}
				state = readState{}
				continue
			}
		} else {
			// readBody
			err := readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{}
				continue
			}
			// 解析完了
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

//
//  readLine
//  @Description:  读取一行
//  @param reader  io Reader
//  @param state   状态
//  @return []byte 读取后的数据
//  @return bool   是否有io错误
//  @return error  返回的error
//
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 { // read normal line
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // read bulk line (binary safe)
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

//
//  parseMultiBulkHeader
//  @Description: MultiBulkHeader 比如*3
//  @param msg    消息体
//  @param state  状态
//  @return error 异常信息
//	*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5value\r\n
//
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	// *3\r\n 取出数字
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error:" + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5value\r\n
		// 这行有几个命令
		state.expectedArgsCount = int(expectedLine)
		// 正在读数组 *状态
		state.msgType = msg[0]
		// 多行读取中
		state.readingMultiLine = true
		// 初始化args
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error:" + string(msg))
	}
}

//
//  parseBulkHeader
//  @Description: 解析BulkHeader
//  @param msg    命令 $4\r\nPING\r\n
//  @param state  解析的状态
//  @return error 返回异常
//
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	logger.Info("bulkLen is: " + string(msg[1:len(msg)-2]))
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

//
//  parseSingleLineReply
//  @Description:  单行解析 +OK\r\n -err\r\n :5\r\n
//  @param msg     消息
//  @return resp.Reply 返回一个解析后的
//  @return error 返回异常
//
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrorReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error:" + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

//
//  readBody
//  @Description: 解析完第一符号之后再解析消息体
//   比如$3\r\nSET\r\n$3\r\nkey\r\n$5value\r\n是解析了第一个* PING\r\n是解析完第一个$
//  @param msg 消息体
//  @param state 当前解析的状态
//  @return error 返回异常
//
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	// 如果是$3
	if line[0] == '$' {
		// 把3放入bulklen
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error:" + string(msg))
		}
		// $0\r\n
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
