package dict

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExist(key string, val interface{}) (result int)
	Remove(key string) (result int)
	Foreach(consumer Consumer)
	Keys() []string
	// RandomKeys
	//  @Description: 随机返回limit个key
	//  @param limit  随机返回key的个数
	//  @return []string 字符串数组 key
	//
	RandomKeys(limit int) []string
	// RandomDistinctKeys
	//  @Description: 随机返回limit个key 但是不重复
	//  @param limit  随机返回key的个数
	//  @return []string 字符串数组 key
	//
	RandomDistinctKeys(limit int) []string
	Clear()
}
type Consumer func(key string, val interface{}) bool
