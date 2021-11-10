package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io/ioutil"
	"net/url"
	"regexp"
	"strings"
	"sync"
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
		readBuffer:  new(MsgBuffer),
		writeBuffer: new(MsgBuffer),
		loc:         time.UTC,
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
	return &Database_mysql_tx{conn.Mysql_Conn}, nil
}

type Database_mysql_tx struct {
	*Mysql_Conn
}

func (tx *Database_mysql_tx) Commit() error {
	_, _, err := tx.Exec([]byte{99, 111, 109, 109, 105, 116})
	return err
}
func (tx *Database_mysql_tx) Rollback() error {
	_, _, err := tx.Exec([]byte{114, 111, 108, 108, 98, 97, 99, 107})
	return err
}
