package resp

type Connection interface {
	Write([]byte) error
	GetDBIndex() int
	SelectDB(dbNum int)
}
