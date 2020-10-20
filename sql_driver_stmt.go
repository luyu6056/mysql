package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var stmtNo uint64

type Database_mysql_stmt struct {
	query        string
	name         string
	conn         *Database_mysql_conn
	numInput     int
	lastInsertId int64
	rowsAffected int64
	ref          int32
	id           uint32
}

func (conn *Database_mysql_conn) Prepare(query string) (driver.Stmt, error) {
	conn.stmtMutex.RLock()
	if stmt, exists := conn.stmtCache[query]; exists {
		// must update reference counter in lock scope
		atomic.AddInt32(&stmt.ref, 1)
		conn.stmtMutex.RUnlock()
		return stmt, nil
	}
	conn.stmtMutex.RUnlock()

	conn.stmtMutex.Lock()
	defer conn.stmtMutex.Unlock()
	if stmt, exists := conn.stmtCache[query]; exists {
		atomic.AddInt32(&stmt.ref, 1)
		return stmt, nil
	}
	stmt := &Database_mysql_stmt{conn: conn, query: query, ref: 1}
	if strings.Index(query, "?") == -1 {
		stmt.numInput = -1
		return stmt, nil
	}
	var err error
	sql := Str2bytes(query)
	msglen := len(sql) + 1
	if msglen > max_packet_size {
		err = errors.New("消息大于最大长度" + strconv.Itoa(max_packet_size))
		return nil, err
	}
	buf := bufpool.Get().(*MsgBuffer)
	defer bufpool.Put(buf)
	buf.Reset()
	b := buf.Make(5 + len(sql))
	b[0] = byte(msglen)
	b[1] = byte(msglen >> 8)
	b[2] = byte(msglen >> 16)
	b[3] = 0
	b[4] = 22 //StmtPrepare
	copy(b[5:], sql)

	_, err = conn.Mysql_Conn.conn.Write(b)
	if err != nil {
		return nil, err
	}

	msglen, err = conn.Mysql_Conn.readOneMsg()
	if err != nil {
		return nil, err
	}
	buffer := conn.Mysql_Conn.readBuffer.Next(msglen)
	switch buffer[0] {
	case 0: //ok报文
		stmt.id = binary.LittleEndian.Uint32(buffer[1:5])
		//columnCount := binary.LittleEndian.Uint16(buffer[5:7])
		stmt.numInput = int(binary.LittleEndian.Uint16(buffer[7:9]))

		conn.Mysql_Conn.readBuffer.Reset()

	case 255: //err报文

		var msg string
		errcode := int(buffer[1]) | int(buffer[2])<<8
		if conn.Mysql_Conn.Status { //未连接成功之前
			msg = string(buffer[9:])
		} else {
			msg = string(buffer[3:])
		}
		if err != nil {

			return nil, err
		}
		return nil, errors.New(strconv.Itoa(errcode) + "-" + msg)
	default:
		return nil, errors.New("无法识别StmtPrepare报文" + strconv.Itoa(int(buffer[0])))
	}

	conn.stmtCache[query] = stmt
	return stmt, nil
}
func (stmt Database_mysql_stmt) Exec(args []driver.Value) (driver.Result, error) {

	var err error
	if stmt.numInput == -1 {
		stmt.lastInsertId, stmt.rowsAffected, err = stmt.conn.Exec(Str2bytes(stmt.query))
	} else {
		if len(args) != stmt.numInput {
			return nil, errors.New("预处理传入的参数数量不对，语句:" + stmt.query + ",参数数量:" + strconv.Itoa(stmt.numInput))
		}
		err = stmt.Execute(args)
		if err != nil {
			return nil, err
		}

		var errmsg string
		stmt.lastInsertId, stmt.rowsAffected, _, errmsg, err = stmt.conn.Mysql_Conn.readmsg()
		if errmsg != "" {
			err = errors.New(errmsg)
		}
	}

	return stmt, err
}
func (stmt *Database_mysql_stmt) Query(args []driver.Value) (driver.Rows, error) {

	var errmsg string
	var err error
	row := rows_pool.Get().(*MysqlRows)
	if stmt.numInput == -1 {
		err = stmt.conn.Query(Str2bytes(stmt.query), row)

		if err != nil {
			rows_pool.Put(row)
			return nil, err
		}
	} else {
		if len(args) != stmt.numInput {
			return nil, errors.New("预处理传入的参数数量不对，语句:" + stmt.query + ",参数数量:" + strconv.Itoa(stmt.numInput))
		}

		err = stmt.Execute(args)
		if err != nil {
			rows_pool.Put(row)
			return nil, err
		}

		_, _, row.field_len, errmsg, err = stmt.conn.Mysql_Conn.readmsg()

		if errmsg != "" {
			err = errors.New(errmsg)
		}
		if err != nil {
			rows_pool.Put(row)
			return nil, err
		}
		err = row.Columns(stmt.conn.Mysql_Conn)
		if err != nil {
			rows_pool.Put(row)
			return nil, err
		}

	}

	return &Database_rows{r: row, stmt: stmt}, err
}
func (stmt Database_mysql_stmt) Execute(args []driver.Value) error {

	var err error
	conn := stmt.conn.Mysql_Conn
	buf := bufpool.Get().(*MsgBuffer)
	defer bufpool.Put(buf)
	buf.Reset()
	data := buf.Make(14)

	data[3] = 0
	data[4] = 23 //StmtExecute
	data[5] = byte(stmt.id)
	data[6] = byte(stmt.id >> 8)
	data[7] = byte(stmt.id >> 16)
	data[8] = byte(stmt.id >> 24)
	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data[9] = 0x00

	// iteration_count (uint32(1)) [4 bytes]
	data[10] = 0x01
	data[11] = 0x00
	data[12] = 0x00
	data[13] = 0x00
	if len(args) > 0 {
		pos := 0
		var nullMask []byte
		maskLen, typesLen := (len(args)+7)/8, 1+2*len(args)
		// buffer has to be extended but we don't know by how much so
		// we depend on append after all data with known sizes fit.
		// We stop at that because we deal with a lot of columns here
		// which makes the required allocation size hard to guess.
		data := buf.Make(maskLen + typesLen)
		nullMask = data[:maskLen]
		// No need to clean nullMask as make ensures that.
		pos += maskLen

		for i := range nullMask {
			nullMask[i] = 0
		}

		// newParameterBoundFlag 1 [1 byte]
		data[pos] = 0x01
		pos++

		// type of each parameter [len(args)*2 bytes]
		paramTypes := data[pos:]
		pos += len(args) * 2

		// value of each parameter [n bytes]

		for i, arg := range args {
			// build NULL-bitmap
			if arg == nil {
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(fieldTypeNULL)
				paramTypes[i+i+1] = 0x00
				continue
			}

			// cache types and values
			switch v := arg.(type) {
			case int64:
				paramTypes[i+i] = byte(fieldTypeLongLong)
				paramTypes[i+i+1] = 0x00

				b := buf.Make(8)
				uint64ToBytes(uint64(v), b)

			case uint64:
				paramTypes[i+i] = byte(fieldTypeLongLong)
				paramTypes[i+i+1] = 0x80 // type is unsigned
				b := buf.Make(8)
				uint64ToBytes(v, b)
			case float64:
				paramTypes[i+i] = byte(fieldTypeDouble)
				paramTypes[i+i+1] = 0x00

				b := buf.Make(8)
				uint64ToBytes(math.Float64bits(v), b)

			case bool:
				paramTypes[i+i] = byte(fieldTypeTiny)
				paramTypes[i+i+1] = 0x00
				b := buf.Make(1)
				if v {
					b[0] = 0x01
				} else {
					b[0] = 0x00
				}

			case []byte:
				// Common case (non-nil value) first
				if v != nil {
					paramTypes[i+i] = byte(fieldTypeString)
					paramTypes[i+i+1] = 0x00

					if len(v) < max_packet_size/(stmt.numInput+1) {
						Writelenmsg(buf, v)

					} else {
						return errors.New("输入的[]byte数据超过设计数值，请联系作者完善")

					}
					continue
				}

				// Handle []byte(nil) as a NULL value
				nullMask[i/8] |= 1 << (uint(i) & 7)
				paramTypes[i+i] = byte(fieldTypeNULL)
				paramTypes[i+i+1] = 0x00

			case string:
				paramTypes[i+i] = byte(fieldTypeString)
				paramTypes[i+i+1] = 0x00

				if len(v) < max_packet_size/(stmt.numInput+1) {
					Writelenmsg(buf, Str2bytes(v))
				} else {
					return errors.New("输入的[]byte数据超过设计数值，请联系作者完善")
				}

			case time.Time:
				paramTypes[i+i] = byte(fieldTypeString)
				paramTypes[i+i+1] = 0x00

				if v.IsZero() {
					Writelenmsg(buf, Str2bytes("0000-00-00"))
				} else {
					Writelenmsg(buf, Str2bytes(v.In(conn.loc).Format("2006-01-02 15:04:05.999999")))
				}

			default:
				return fmt.Errorf("cannot convert type: %T", arg)
			}
		}

		// Check if param values exceeded the available buffer
		// In that case we must build the data packet with the new values buffer
	}
	msglen := buf.Len() - 4
	data = buf.Bytes()
	data[0] = byte(msglen)
	data[1] = byte(msglen >> 8)
	data[2] = byte(msglen >> 16)
	_, err = conn.conn.Write(data)
	return err
}
func (stmt Database_mysql_stmt) NumInput() int {
	return stmt.numInput
}
func (stmt Database_mysql_stmt) Close() (err error) {
	if stmt.numInput == -1 {
		return
	}
	stmt.conn.stmtMutex.Lock()
	defer stmt.conn.stmtMutex.Unlock()

	if atomic.AddInt32(&stmt.ref, -1) == 0 {
		buf := bufpool.Get().(*MsgBuffer)
		defer bufpool.Put(buf)

		buf.Reset()
		data := buf.Make(9)
		data[0] = 5
		data[1] = 0
		data[2] = 0
		data[3] = 0
		data[4] = 25
		data[5] = byte(stmt.id)
		data[6] = byte(stmt.id >> 8)
		data[7] = byte(stmt.id >> 16)
		data[8] = byte(stmt.id >> 24)

		_, err = stmt.conn.Mysql_Conn.conn.Write(data)
		if err != nil {
			return err
		}
		delete(stmt.conn.stmtCache, stmt.query)
	}
	return err
}
func (stmt Database_mysql_stmt) LastInsertId() (int64, error) {
	return stmt.lastInsertId, nil
}

func (stmt Database_mysql_stmt) RowsAffected() (int64, error) {
	return stmt.rowsAffected, nil
}
