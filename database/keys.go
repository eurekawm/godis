package database

import (
	"github.com/eurekawm/godis/interface/resp"
	"github.com/eurekawm/godis/lib/wildcard"
	"github.com/eurekawm/godis/resp/reply"
)

// DEL
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

// EXISTS
func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// KEYS
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	// 符合这个通配符的key
	result := make([][]byte, 0)
	db.data.Foreach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

// FLUSHDB
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	return reply.MakeOkReply()
}

// TYPE
func execType(db *DB, args [][]byte) resp.Reply {
	// TYPE key
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		reply.MakeStatusReply("string")
	}
	// TODO 实现其他的数据类型
	return &reply.UnknownErrReply{}
}

// RENAME
func execRename(db *DB, args [][]byte) resp.Reply {
	// RENAME key1 key2
	src := string(args[0])
	des := string(args[1])
	entity, exists := db.GetEntity(src)
	if exists {
		db.PutEntity(des, entity)
		db.Removes(src)
		return reply.MakeOkReply()
	} else {
		return reply.MakeErrorReply("no such key")
	}
}

// RENAMENX
func execRenamenx(db *DB, args [][]byte) resp.Reply {
	// RENAMENX key1 key2
	src := string(args[0])
	des := string(args[1])
	_, ok := db.GetEntity(des)
	if ok {
		return reply.MakeIntReply(0)
	}
	entity, exists := db.GetEntity(src)
	if exists {
		db.PutEntity(des, entity)
		db.Removes(src)
		return reply.MakeIntReply(1)
	} else {
		return reply.MakeErrorReply("no such key")
	}
}

func init() {
	RegisterCommand("DEL", execDel, -2)
	RegisterCommand("EXISTS", execExists, -2)
	RegisterCommand("FLUSHDB", execFlushDB, -1)
	RegisterCommand("TYPE", execType, 2) // TYPE hello
	RegisterCommand("RENAME", execRename, 3)
	RegisterCommand("RENAMENX", execRenamenx, 3)
	RegisterCommand("KEYS", execKeys, 2) // keys *
}
