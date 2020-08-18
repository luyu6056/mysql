package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"net/url"
	"reflect"
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
	var str [][]string
	if str, _ = Preg_match_result(`([^:]+):([^@]*)@(tcp)?(unix)?\(([^)]*)\)\/([^?]+)(\?[^?]+)`, dsn, 1); len(str) == 0 {
		return nil, errors.New("mysql初始化失败，解析连接字串错误" + dsn)

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
	_, offset := time.Now().Zone()
	var time_zone string
	if offset >= 0 {
		time_zone = "+" + strconv.Itoa(offset/3600) + ":00"
	} else {
		time_zone = strconv.Itoa(offset/3600) + ":00"
	}
	if str[0][7] != "" {
		for _, s := range strings.Split(str[0][7], "&") {
			if value := strings.Split(url.PathEscape(s), "="); len(value) == 2 {
				switch value[0] {
				case "charset":
					charset = value[1]
				case "time_zone":
					time_zone = value[2]
				}
			}
		}
	}
	conn, err := connect_new(str[0][1], str[0][2], str[0][5], str[0][6], charset, time_zone, tlsconfig)
	return &Database_mysql_conn{conn}, err
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
	conn         *Mysql_Conn
	numInput     int
	lastInsertId int64
	rowsAffected int64
}

func (conn *Database_mysql_conn) Prepare(query string) (driver.Stmt, error) {

	buf := bufpool.Get().(*MsgBuffer)
	defer bufpool.Put(buf)
	buf.Reset()
	buf.WriteString("PREPARE sql_")
	stmt := &Database_mysql_stmt{conn: conn.Mysql_Conn, numInput: strings.Count(query, "?"), query: query}
	name := atomic.AddUint64(&stmtNo, 1)
	stmt.name = strconv.FormatUint(name, 10)
	buf.WriteString(stmt.name)
	buf.WriteString(" FROM '")
	buf.WriteString(query)
	buf.WriteString("'")
	_, _, err := conn.Exec(buf.Bytes()) //start transaction
	if err != nil {
		return nil, err
	}

	return stmt, nil
}
func (stmt Database_mysql_stmt) Exec(values []driver.Value) (driver.Result, error) {
	if len(values) != stmt.numInput {
		return nil, errors.New("预处理传入的参数数量不对，语句:" + stmt.query + ",参数数量:" + strconv.Itoa(stmt.numInput))
	}
	buf := bufpool.Get().(*MsgBuffer)
	defer bufpool.Put(buf)
	for k, v := range values {
		buf.Reset()
		buf.WriteString("SET @v")
		buf.WriteString(strconv.Itoa(k))
		buf.WriteString(" = ")
		str, err := sqlValueToStr(v)
		if err != nil {
			return nil, err
		}
		buf.WriteString(str)
		_, _, err = stmt.conn.Exec(buf.Bytes())
		if err != nil {
			return nil, err
		}

	}

	buf.Reset()
	buf.WriteString("EXECUTE sql_")
	buf.WriteString(stmt.name)
	if len(values) > 0 {
		buf.WriteString(" USING ")
		for k, _ := range values {
			buf.WriteString("@v")
			buf.WriteString(strconv.Itoa(k))
			buf.WriteString(",")
		}
		buf.Truncate(buf.Len() - 1)
	}
	var err error
	stmt.lastInsertId, stmt.rowsAffected, err = stmt.conn.Exec(buf.Bytes())
	return stmt, err
}
func (stmt Database_mysql_stmt) Query(values []driver.Value) (driver.Rows, error) {
	if len(values) != stmt.numInput {
		return nil, errors.New("预处理传入的参数数量不对，语句:" + stmt.query + ",参数数量:" + strconv.Itoa(stmt.numInput))
	}
	buf := bufpool.Get().(*MsgBuffer)
	defer bufpool.Put(buf)
	for k, v := range values {
		buf.Reset()
		buf.WriteString("SET @v")
		buf.WriteString(strconv.Itoa(k))
		buf.WriteString(" = ")
		str, err := sqlValueToStr(v)
		if err != nil {
			return nil, err
		}
		buf.WriteString(str)
		_, _, err = stmt.conn.Exec(buf.Bytes())
		if err != nil {
			return nil, err
		}

	}

	buf.Reset()
	buf.WriteString("EXECUTE sql_")
	buf.WriteString(stmt.name)
	if len(values) > 0 {
		buf.WriteString(" USING ")
		for k, _ := range values {
			buf.WriteString("@v")
			buf.WriteString(strconv.Itoa(k))
			buf.WriteString(",")
		}
		buf.Truncate(buf.Len() - 1)
	}
	row := rows_pool.Get().(*MysqlRows)
	columns, err := stmt.conn.Query(buf.Bytes(), row)
	r := &Database_rows{r: row, c: stmt.conn}
	r.columns = make([]string, len(columns))
	for k, v := range columns {
		r.columns[k] = string(v)
	}
	return r, err
}
func (stmt Database_mysql_stmt) NumInput() int {
	return stmt.numInput
}
func (stmt Database_mysql_stmt) Close() error {
	buf := bufpool.Get().(*MsgBuffer)
	defer bufpool.Put(buf)
	buf.Reset()
	buf.WriteString("DROP PREPARE sql_")
	buf.WriteString(stmt.name)
	_, _, err := stmt.conn.Exec(buf.Bytes())
	return err
}
func (stmt Database_mysql_stmt) LastInsertId() (int64, error) {
	return stmt.lastInsertId, nil
}

func (stmt Database_mysql_stmt) RowsAffected() (int64, error) {
	return stmt.rowsAffected, nil
}
func sqlValueToStr(value driver.Value) (string, error) {
	switch v := value.(type) {
	case int64:

		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		if v {
			return "'true'", nil
		} else {
			return "'false'", nil
		}
	case []byte:
		return "0x" + hex.EncodeToString(v), nil
	case string:
		return "'" + v + "'", nil
	case time.Time:
		return "'" + v.Format("2006-01-02 15:04:05") + "'", nil
	}
	return "", errors.New("未识别driver.Value类型" + reflect.TypeOf(value).Kind().String())
	//   "database/sql/driver" 里面只列出以下几项
	//   int64
	//   float64
	//   bool
	//   []byte
	//   string
	//   time.Time
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
	c       *Mysql_Conn
	r       *MysqlRows
	line    int
	columns []string
}

func (row *Database_rows) Close() error {
	rows_pool.Put(row.r)
	return nil
}
func (row *Database_rows) Columns() []string {
	return row.columns
}
func (row *Database_rows) Next(dest []driver.Value) (err error) {
	if row.line >= row.r.result_len {
		return io.EOF
	}
	rows := row.r
	msglen := rows.msg_len[row.line]
	rows.Buffer2.Reset()
	rows.Buffer2.Write(rows.Buffer.Next(msglen))
	for k, _ := range dest {
		rows.buffer, err = ReadLength_Coded_Byte(rows.Buffer2)
		if err != nil {
			return
		}
		dest[k] = string(rows.buffer)
	}

	row.line++
	return
}
