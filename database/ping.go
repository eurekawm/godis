package database

import (
	"github.com/eurekawm/godis/interface/resp"
	"github.com/eurekawm/godis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

func init() {
	RegisterCommand("PING", Ping, 1)
}
