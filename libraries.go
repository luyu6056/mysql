package mysql

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/dlclark/regexp2"
	jsoniter "github.com/json-iterator/go"
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

//返回匹配结果,n=次数
func Preg_match_result(regtext string, text string, n int) ([][]string, error) {

	r, err := regexp2.Compile(regtext, 0)
	if err != nil {
		return nil, err
	}

	m, err := r.FindStringMatch(text)
	if err != nil {
		return nil, err
	}
	var result [][]string
	for m != nil && n != 0 {
		var res_v []string
		for _, v := range m.Groups() {
			res_v = append(res_v, v.String())
		}

		m, _ = r.FindNextMatch(m)
		result = append(result, res_v)
		n--
	}

	return result, nil
}

//正则替换
func Preg_replace(regtext string, text string, src string) (string, error) {
	r, _ := regexp2.Compile(regtext, 0)
	return r.Replace(src, text, -1, -1)

}

//简单正则匹配,返回是否成功
func Preg_match(regtext string, text string) bool {

	r, err := regexp2.Compile(regtext, 0)
	if err != nil {
		Log("简单匹配正则语句出错%s", regtext)
		return false
	}

	m, err := r.FindStringMatch(text)
	if err != nil || m == nil {
		if err != nil {
			Log("简单匹配出错%v", err)
		}
		return false
	}
	return true
}

func JsonMarshalString(i interface{}) string {
	s, e := jsoniter.MarshalToString(i)
	if e != nil {
		DEBUG(i, "json序列化失败", e)
	}
	return s
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
