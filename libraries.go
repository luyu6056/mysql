package mysql

import (
	"fmt"
	"runtime"
	"unsafe"
)

const (
	ISDEBUG = true
)

//一些与mysql无关的func
func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}
func DEBUG(v ...interface{}) {
	if ISDEBUG {
		_, file, line, ok := runtime.Caller(1)
		if ok {
			v = append([]interface{}{fmt.Sprintf("%s,line %d:", file, line)}, v...)
		}
		fmt.Println(v...)
	}
}

func Log(format string, v ...interface{}) {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		v = append([]interface{}{fmt.Sprintf("%s,line %d:", file, line)}, v...)
	}
	fmt.Printf("%s "+format+"\r\n", v...)
}
func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
