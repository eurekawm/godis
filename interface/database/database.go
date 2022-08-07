package database

import "github.com/eurekawm/godis/interface/resp"

type CommandLine [][]byte

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply
	Close()
	AfterClientClose(c resp.Connection)
}
type DataEntity struct {
	Data interface{}
}
