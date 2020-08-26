package mysql

import (
	"errors"
	"reflect"
	"sync"
)

var rows_pool = sync.Pool{New: func() interface{} {
	row := &MysqlRows{Buffer: new(MsgBuffer), Buffer2: new(MsgBuffer), field_m: make(map[string]map[string]*Field_struct)}

	return row
}}

type Field_struct struct {
	Offset  uintptr
	Kind    reflect.Kind
	Field_t reflect.Type
}
type MysqlRows struct {
	Buffer    *MsgBuffer
	Buffer2   *MsgBuffer
	field_len int
	msg_len   []int
	buffer    []byte
	//msg_buffer_no *int

	field_m map[string]map[string]*Field_struct

	result_len int
}

func (row *MysqlRows) Columns(mysql *Mysql_Conn) (columns []string, err error) {

	row.result_len = 0
	columns = make([]string, row.field_len)
	var index uint32
	var def string
	var msglen, field_index int

	for msglen, err = mysql.readOneMsg(); err == nil; msglen, err = mysql.readOneMsg() {
		if msglen == 5 && mysql.buffer.PreBytes(1)[0] == 0xfe { //EOF
			mysql.buffer.Shift(5)
			break
		}
		row.Buffer.Reset()
		row.Buffer.Write(mysql.buffer.Next(msglen))

		def, err = ReadLengthCodedStringFromBuffer(row.Buffer, true)
		if err != nil || def != "def" {
			return nil, errors.New("读取查询结果目录头错误")
		}

		_, err = ReadLengthCodedStringFromBuffer(row.Buffer, false)
		if err != nil {
			return
		}

		_, err = ReadLengthCodedStringFromBuffer(row.Buffer, false)
		if err != nil {
			return
		}

		_, err = ReadLengthCodedStringFromBuffer(row.Buffer, false)
		if err != nil {
			return
		}

		msglen, err = ReadLength_Coded_Binary(row.Buffer)
		if err != nil {
			return
		}
		columns[index] = string(row.Buffer.Next(msglen))
		field_index += msglen
		index++
	}
	//libraries.DEBUG(row.Buffer.Bytes())
	row.Buffer.Reset()
	row.msg_len = row.msg_len[:0]
	for msglen, err = mysql.readOneMsg(); err == nil; msglen, err = mysql.readOneMsg() {
		if msglen == 5 && mysql.buffer.PreBytes(1)[0] == 0xfe { //EOF
			mysql.buffer.Shift(5)
			break
		}
		row.Buffer.Write(mysql.buffer.Next(msglen))
		row.result_len++
		row.msg_len = append(row.msg_len, msglen)
	}
	return columns, err
}

func (row *MysqlRows) Scan(a ...*[]byte) error {
	var err error
	for _, v := range a {
		*v, err = ReadLength_Coded_Byte(row.Buffer)
		if err != nil {
			return err
		}
	}
	return nil

}
