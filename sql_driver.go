package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	Mysql_ssl_ca   string
	Mysql_ssl_cert string
	Mysql_ssl_key  string
)

type MySQLDriver struct{}

//返回一个全新的mysql conn
func (d MySQLDriver) Open(dsn string) (driver.Conn, error) {

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	r, _ := regexp.Compile(`([^:]+):(\S*)@(tcp)?(unix)?\(([^)]*)\)\/([^?]+)(\?[^?]+)`)
	res := r.FindSubmatch([]byte(dsn))
	var str = make([]string, 8)
	for k, v := range res {
		str[k] = string(v)
	}
	var tlsconfig *tls.Config
	if Mysql_ssl_ca != "" && Mysql_ssl_cert != "" && Mysql_ssl_key != "" {
		cert, err := tls.LoadX509KeyPair(Mysql_ssl_cert, Mysql_ssl_key)
		if err != nil {
			return nil, errors.New("mysql证书初始化失败 err: " + err.Error())

		}
		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(Mysql_ssl_ca)
		if err != nil {
			return nil, errors.New("mysql证书初始化失败 err: " + err.Error())
		}
		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return nil, errors.New("mysql证书初始化失败 certPool.AppendCertsFromPEM err")
		}
		tlsconfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
			RootCAs:            certPool,
		}
	}

	var charset = "utf8"
	var mysqlconn = &Mysql_Conn{
		buffer: new(MsgBuffer),
		loc:    time.UTC,
	}

	if str[7] != "" {
		for _, s := range strings.Split(str[7], "&") {
			if value := strings.Split(url.PathEscape(s), "="); len(value) == 2 {
				switch value[0] {
				case "charset":
					charset = value[1]
				case "loc":
					if newloc, err := time.LoadLocation(value[1]); err == nil {
						mysqlconn.loc = newloc
					}
				case "parseTime":
					var isBool bool
					mysqlconn.parseTime, isBool = readBool(value[1])
					if !isBool {
						return nil, errors.New("invalid bool value: " + value[1])

					}
				}
			}
		}
	}
	err := mysqlconn.connect_new(str[1], str[2], str[5], str[6], charset, tlsconfig)
	return &Database_mysql_conn{Mysql_Conn: mysqlconn, stmtCache: make(map[string]*Database_mysql_stmt)}, err

}

func init() {
	sql.Register("mysql", &MySQLDriver{})
}

//扩展mysqlconn接口以符合database
var bufpool = sync.Pool{New: func() interface{} {
	row := NewBuffer(1024)
	return row
}}

type Database_mysql_conn struct {
	stmtCache map[string]*Database_mysql_stmt
	stmtMutex sync.RWMutex
	*Mysql_Conn
}

func (conn *Database_mysql_conn) Begin() (driver.Tx, error) {
	_, _, err := conn.Exec(start_transaction) //start transaction
	if err != nil {
		return nil, err
	}
	return Database_mysql_tx{conn.Mysql_Conn}, nil
}

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
	//DEBUG(string(sql))
	_, err = conn.Mysql_Conn.conn.Write(b)
	if err != nil {

		return nil, err
	}
	conn.Mysql_Conn.buffer.Reset()
	msglen, err = conn.Mysql_Conn.readOneMsg()
	if err != nil {
		return nil, err
	}

	buffer := conn.Mysql_Conn.buffer.Next(msglen)
	switch buffer[0] {
	case 0: //ok报文

		stmt.id = binary.LittleEndian.Uint32(buffer[1:5])
		//columnCount := binary.LittleEndian.Uint16(buffer[5:7])
		stmt.numInput = int(binary.LittleEndian.Uint16(buffer[7:9]))

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
		stmt.conn.Mysql_Conn.buffer.Reset()
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
		stmt.conn.Mysql_Conn.buffer.Reset()
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
		//fmt.Println("关闭了")
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

type Database_mysql_tx struct {
	*Mysql_Conn
}

func (tx Database_mysql_tx) Commit() error {
	_, _, err := tx.Exec([]byte{99, 111, 109, 109, 105, 116})
	return err
}
func (tx Database_mysql_tx) Rollback() error {
	_, _, err := tx.Exec([]byte{114, 111, 108, 108, 98, 97, 99, 107})
	return err
}

type Database_rows struct {
	r    *MysqlRows
	stmt *Database_mysql_stmt
	line int
}

func (row *Database_rows) Close() error {

	rows_pool.Put(row.r)
	return nil
}
func (row *Database_rows) Columns() (columns []string) {
	columns = make([]string, len(row.r.columns))
	for k, v := range row.r.columns {
		columns[k] = v.name
	}
	return
}
func (row *Database_rows) Next(dest []driver.Value) (err error) {
	if row.line >= row.r.result_len {
		return io.EOF
	}

	rows := row.r
	if row.stmt.numInput == -1 {

		data := rows.Buffer.Next(rows.msg_len[row.line])
		pos := 0
		for i, _ := range dest {
			msglen, err := ReadLength_Coded_Slice(data[pos:], &pos)
			if err != nil {
				if err.Error() == "NULL" {
					dest[i] = nil
					continue
				}
				return err
			}
			str := Bytes2str(data[pos : pos+msglen])

			switch rows.columns[i].fieldtype {
			case fieldTypeNULL:
				dest[i] = nil

			// Numeric Types
			case fieldTypeTiny:
				dest[i], _ = strconv.ParseInt(str, 10, 8)

			case fieldTypeShort, fieldTypeYear:
				dest[i], _ = strconv.ParseInt(str, 10, 16)

			case fieldTypeInt24, fieldTypeLong:
				dest[i], _ = strconv.ParseInt(str, 10, 32)

			case fieldTypeLongLong:
				dest[i], _ = strconv.ParseInt(str, 10, 64)

			case fieldTypeFloat:
				dest[i], _ = strconv.ParseFloat(str, 32)

			case fieldTypeDouble:
				dest[i], _ = strconv.ParseFloat(str, 64)

			// Length coded Binary Strings
			case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
				fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
				fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
				fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON:
				dest[i] = string(data[pos : pos+msglen])
			case
				fieldTypeDate, fieldTypeNewDate, // Date YYYY-MM-DD
				fieldTypeTime,                         // Time [-][H]HH:MM:SS[.fractal]
				fieldTypeTimestamp, fieldTypeDateTime: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]
				dest[i], _ = time.ParseInLocation("2006-01-02 15:04:05", str, row.stmt.conn.loc)

			// Please report if this happens!
			default:
				return fmt.Errorf("unknown field type %d", rows.columns[i].fieldtype)
			}

			pos += msglen

		}
	} else {

		nulllen := (len(dest) + 7 + 2) / 8
		msglen := rows.msg_len[row.line]
		data := rows.Buffer.Next(msglen)

		if msglen == 5 && data[0] == 0xfe { //EOF
			return io.EOF
		}
		if data[0] != 0 {
			return errors.New("返回协议错误，返回的内容不是Binary Protocol")
		}
		pos := 1 + nulllen
		nullMask := data[1 : 1+pos]
		for i, _ := range dest {
			if nullMask[i/8]>>(uint(i)&7) == 1 {
				dest[i] = nil
				continue
			}

			// Convert to byte-coded string
			switch rows.columns[i].fieldtype {
			case fieldTypeNULL:
				dest[i] = nil
				continue

			// Numeric Types
			case fieldTypeTiny:
				if rows.columns[i].fleldflag&flagUnsigned != 0 {
					dest[i] = int64(data[pos])
				} else {
					dest[i] = int64(int8(data[pos]))
				}
				pos++
				continue

			case fieldTypeShort, fieldTypeYear:
				if rows.columns[i].fleldflag&flagUnsigned != 0 {
					dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
				} else {
					dest[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
				}
				pos += 2
				continue

			case fieldTypeInt24, fieldTypeLong:
				if rows.columns[i].fleldflag&flagUnsigned != 0 {
					dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
				} else {
					dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
				}
				pos += 4
				continue

			case fieldTypeLongLong:
				if rows.columns[i].fleldflag&flagUnsigned != 0 {
					val := binary.LittleEndian.Uint64(data[pos : pos+8])
					if val > math.MaxInt64 {
						dest[i] = uint64ToString(val)
					} else {
						dest[i] = int64(val)
					}
				} else {
					dest[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
				}
				pos += 8
				continue

			case fieldTypeFloat:
				dest[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4]))
				pos += 4
				continue

			case fieldTypeDouble:
				dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
				pos += 8
				continue

			// Length coded Binary Strings
			case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
				fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
				fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
				fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON:

				msglen, err := ReadLength_Coded_Slice(data[pos:], &pos)
				if err != nil {

					return err
				}
				dest[i] = string(data[pos : pos+msglen])
				pos += msglen
			case
				fieldTypeDate, fieldTypeNewDate, // Date YYYY-MM-DD
				fieldTypeTime,                         // Time [-][H]HH:MM:SS[.fractal]
				fieldTypeTimestamp, fieldTypeDateTime: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]

				n, err := ReadLength_Coded_Slice(data[pos:], &pos)
				if err != nil {

					return err
				}

				switch {
				case msglen == 0:
					dest[i] = nil
					continue
				case rows.columns[i].fieldtype == fieldTypeTime:
					// database/sql does not support an equivalent to TIME, return a string
					var dstlen uint8
					switch decimals := rows.columns[i].decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 8
					case 1, 2, 3, 4, 5, 6:
						dstlen = 8 + 1 + decimals
					default:
						return fmt.Errorf(
							"protocol error, illegal decimals value %d",
							rows.columns[i].decimals,
						)
					}
					dest[i], err = formatBinaryTime(data[pos:pos+int(n)], dstlen)
				case row.stmt.conn.Mysql_Conn.parseTime:
					dest[i], err = parseBinaryDateTime(uint64(n), data[pos:], row.stmt.conn.Mysql_Conn.loc)
				default:
					var dstlen uint8
					if rows.columns[i].fieldtype == fieldTypeDate {
						dstlen = 10
					} else {
						switch decimals := rows.columns[i].decimals; decimals {
						case 0x00, 0x1f:
							dstlen = 19
						case 1, 2, 3, 4, 5, 6:
							dstlen = 19 + 1 + decimals
						default:
							return fmt.Errorf(
								"protocol error, illegal decimals value %d",
								rows.columns[i].decimals,
							)
						}
					}
					dest[i], err = formatBinaryDateTime(data[pos:pos+int(n)], dstlen)
				}

				if err != nil {
					pos += int(n)
					continue
				} else {
					return err
				}

			// Please report if this happens!
			default:
				return fmt.Errorf("unknown field type %d", rows.columns[i].fieldtype)
			}
		}
	}
	row.line++
	return
}
