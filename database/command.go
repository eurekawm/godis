package database

import "strings"

//
//  command
//  @Description: 每个命令比如KEYS都有一个执行方法 我们就实现这个执行方法
//  在DB里面执行这个方法
//
var cmdTable = make(map[string]*command)

type command struct {
	// 执行方法
	executor ExecFunc
	// 参数个数
	arity int
}

// RegisterCommand
//  @Description: 具体的执行方法注册到map里面
//  @param name   方法名
//  @param execFunc 执行函数
//  @param arity    参数个数
//
func RegisterCommand(name string, execFunc ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: execFunc,
		arity:    arity,
	}
}
