package mysql

import (
	"reflect"
	"sync"
)

var rows_pool = sync.Pool{New: func() interface{} {
	row := &MysqlRows{Buffer: new(MsgBuffer)}
	return row
}}

type Field_struct struct {
	Offset  uintptr
	Kind    reflect.Kind
	Field_t reflect.Type
}
type MysqlRows struct {
	Buffer    *MsgBuffer
	field_len int
	msg_len   []int

	buffer []byte
	//msg_buffer_no *int

	result_len int
	columns    []MysqlColumn
}
type MysqlColumn struct {
	name      string
	fieldtype fieldType
	fleldflag fieldFlag
	decimals  uint8
}

func (row *MysqlRows) Columns(mysql *Mysql_Conn) (err error) {

	if cap(row.columns) < row.field_len {
		row.columns = make([]MysqlColumn, row.field_len)
	}
	row.columns = row.columns[:row.field_len]

	var index uint32
	var msglen, pos int

	for msglen, err = mysql.readOneMsg(); err == nil; msglen, err = mysql.readOneMsg() {
		data := mysql.readBuffer.Next(msglen)

		if msglen == 5 && data[0] == 0xfe { //EOF
			break
		}
		pos = 0
		msglen, err = ReadLength_Coded_Slice(data, &pos)
		if err != nil {
			return err
		}
		pos += msglen

		// Database [len coded string]
		msglen, err = ReadLength_Coded_Slice(data[pos:], &pos)
		if err != nil {
			return
		}

		pos += msglen
		// Table [len coded string]
		msglen, err = ReadLength_Coded_Slice(data[pos:], &pos)
		if err != nil {
			return
		}
		pos += msglen
		// Original table [len coded string]
		msglen, err = ReadLength_Coded_Slice(data[pos:], &pos)
		if err != nil {
			return
		}
		pos += msglen
		// Name [len coded string]
		msglen, err = ReadLength_Coded_Slice(data[pos:], &pos)
		if err != nil {
			return
		}

		row.columns[index].name = string(data[pos : pos+msglen])
		pos += msglen
		msglen, err = ReadLength_Coded_Slice(data[pos:], &pos)
		if err != nil {
			return
		}
		pos += msglen
		pos += 7
		row.columns[index].fieldtype = fieldType(data[pos])
		row.columns[index].fleldflag = fieldFlag(uint16(data[pos+1]) + uint16(data[pos+2])<<8)
		row.columns[index].decimals = data[pos+3]
		index++
	}
	if err != nil {
		return
	}
	return row.ReadResultMsg(mysql)
}
func (row *MysqlRows) ReadResultMsg(mysql *Mysql_Conn) (err error) {
	row.Buffer.Reset()
	row.result_len = 0
	row.msg_len = row.msg_len[:0]
	for msglen, err := mysql.readOneMsg(); err == nil; msglen, err = mysql.readOneMsg() {
		data := mysql.readBuffer.Next(msglen)
		if msglen == 5 && data[0] == 0xfe { //EOF
			return nil
		}
		row.Buffer.Write(data)
		row.result_len++
		row.msg_len = append(row.msg_len, msglen)
	}
	return nil
}
