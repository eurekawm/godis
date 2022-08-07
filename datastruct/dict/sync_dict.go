package dict

import "sync"

//
//  syncDict
//  @Description: 最底层的数据结构
//
type syncDict struct {
	m sync.Map
}

func (dict *syncDict) Get(key string) (val interface{}, exists bool) {
	res, ok := dict.m.Load(key)
	return res, ok
}

func (dict *syncDict) Len() int {
	var mLen int
	dict.m.Range(func(key, value interface{}) bool {
		mLen++
		return true
	})
	return mLen
}

func (dict *syncDict) Put(key string, val interface{}) (result int) {
	_, exists := dict.m.Load(key)
	dict.m.Store(key, val)
	if exists {
		return 0
	}
	return 1
}

func (dict *syncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, exists := dict.m.Load(key)
	if !exists {
		dict.m.Store(key, val)
		return 1
	} else {
		return 0
	}
}

func (dict *syncDict) PutIfExist(key string, val interface{}) (result int) {
	_, exists := dict.m.Load(key)
	if exists {
		dict.m.Store(key, val)
		return 1
	} else {
		return 0
	}
}

func (dict *syncDict) Remove(key string) (result int) {
	_, exists := dict.m.Load(key)
	dict.m.Delete(key)
	if exists {
		return 1
	}
	return 0
}

func (dict *syncDict) Foreach(consumer Consumer) {
	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})
}

func (dict *syncDict) Keys() []string {
	result := make([]string, dict.Len())
	index := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[index] = key.(string)
		index++
		return true
	})
	return result
}

func (dict *syncDict) RandomKeys(limit int) []string {
	result := make([]string, dict.Len())
	for i := 0; i < limit; i++ {
		dict.m.Range(func(key, value interface{}) bool {
			result[i] = key.(string)
			return false
		})
	}
	return result
}

func (dict *syncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})

	return result
}

func (dict *syncDict) Clear() {
	*dict = *MakeSyncDict()
}

func MakeSyncDict() *syncDict {
	return &syncDict{}
}
