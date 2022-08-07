package database

import (
	"github.com/eurekawm/godis/datastruct/dict"
	"github.com/eurekawm/godis/interface/database"
	"github.com/eurekawm/godis/interface/resp"
	"github.com/eurekawm/godis/resp/reply"
	"strings"
)

// DB
//  @Description: 数据库 类似redis 16个数据库
//
type DB struct {
	index int
	data  dict.Dict
}

func makeDB(db *DB) *DB {
	return &DB{data: dict.MakeSyncDict()}
}

// ExecFunc 命令的执行 set key val
type ExecFunc func(db *DB, args [][]byte) resp.Reply

type CmdLine [][]byte

func (db *DB) exec(c resp.Connection, line CmdLine) resp.Reply {
	// PING SET SETNX
	cmdName := strings.ToLower(string(line[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrorReply("ERR unknown command " + cmdName)
	}
	// set key 参数不对
	if !validateArity(cmd.arity, line) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	// 这里line = set hello 11 已经找到了这个set指令的执行方法了 那就把后面的交给执行方法
	return fun(db, line[1:])
}

//
//  validateArity
//  @Description: 校验命令参数个数是否合法
//  @param arity  参数个数
//  @param cmdArgs 命令
//  @return bool 是否合法
//
func validateArity(arity int, cmdArgs [][]byte) bool {
	// 命令个数确定的 SET hello 1 arity = 3
	// 命令不固定的 用-表示 EXISTS hello nihao arity = -2
	argNum := len(cmdArgs)
	if arity > 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

// GetEntity
//  @Description: 根据key获取DataEntity交给上层api来使用的
//  @receiver db 数据库
//  @param key   键
//  @return *database.DataEntity 实现这个接口的具体类型
//  @return bool 操作结果是否OK
//
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity
//  @Description: 放入一个key value
//  @receiver db  数据库
//  @param key    键
//  @param entity value
//  @return int  插入了几个
//
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExist(key, entity)

}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.data.Clear()
}
