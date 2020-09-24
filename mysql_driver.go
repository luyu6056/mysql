package mysql

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	//"fmt"

	"io/ioutil"
	"net"
	"strconv"
	"strings"
)

const (
	max_packet_size = 16777215
)

//capabilities定义
const (
	CLIENT_LONG_PASSWORD     = 0x00000001
	CLIENT_LONG_FLAG         = 0x00000004 //1
	CLIENT_CONNECT_WITH_DB   = 0x00000008 //1
	CLIENT_COMPRESS          = 0x00000020
	CLIENT_LOCAL_FILES       = 0x00000080 //1
	CLIENT_PROTOCOL_41       = 0x00000200 //1
	CLIENT_SSL               = 0x00000800
	CLIENT_SECURE_CONNECTION = 0x00008000 //1
	CLIENT_TRANSACTIONS      = 0x00002000 //1
	CLIENT_MULTI_RESULTS     = 0x00020000 //1
	CLIENT_PLUGIN_AUTH       = 0x00080000 //1
)

type fieldType byte

const (
	fieldTypeDecimal fieldType = iota
	fieldTypeTiny
	fieldTypeShort
	fieldTypeLong
	fieldTypeFloat
	fieldTypeDouble
	fieldTypeNULL
	fieldTypeTimestamp
	fieldTypeLongLong
	fieldTypeInt24
	fieldTypeDate
	fieldTypeTime
	fieldTypeDateTime
	fieldTypeYear
	fieldTypeNewDate
	fieldTypeVarChar
	fieldTypeBit
)
const (
	fieldTypeJSON fieldType = iota + 0xf5
	fieldTypeNewDecimal
	fieldTypeEnum
	fieldTypeSet
	fieldTypeTinyBLOB
	fieldTypeMediumBLOB
	fieldTypeLongBLOB
	fieldTypeBLOB
	fieldTypeVarString
	fieldTypeString
	fieldTypeGeometry
)

type fieldFlag uint16

const (
	flagNotNULL fieldFlag = 1 << iota
	flagPriKey
	flagUniqueKey
	flagMultipleKey
	flagBLOB
	flagUnsigned
	flagZeroFill
	flagBinary
	flagEnum
	flagAutoIncrement
	flagTimestamp
	flagSet
	flagUnknown1
	flagUnknown2
	flagUnknown3
	flagUnknown4
)

type Mysql_Conn struct {
	//Version            string
	Thread_id uint32 //线程ID
	//seed               []byte
	Capabilities uint32 //协议协商
	//serverCharsetIndex uint8  //编码格式
	//clientCharsetIndex uint8
	//serverStatus       uint16 //状态码
	//restOfScrambleBuff []byte //保留字节 长度13
	//seed2 []byte //长度12
	//capability_flags uint16
	auth_plugin_name string
	Status           bool
	//username      string
	//passwd        string
	//database      string
	msg_no uint8
	conn   net.Conn
	buffer *MsgBuffer
	//msg_buffer_no int

	//Debug         bool
	//buf_4 []byte
	//wg           sync.WaitGroup
	loc       *time.Location //database/sql value格式化的时候用到
	parseTime bool
}

func (mysql *Mysql_Conn) Close() error {
	if mysql != nil && mysql.conn != nil {
		mysql.buffer.Reset()
		b := mysql.buffer.Make(5)
		b[0] = 1
		b[1] = 0
		b[2] = 0
		b[3] = 0
		b[4] = 1 //COM_QUIT
		mysql.buffer.WriteByte(0)
		mysql.buffer.WriteByte(0x01)
		mysql.conn.Write(mysql.buffer.Bytes())

		mysql.conn.Close()
		mysql.conn = nil
	}
	mysql.Status = false
	return nil
}
func (mysqlconn *Mysql_Conn) connect_new(username, passwd, ip_port, database, charset string, tlsconfig *tls.Config) error {

	var conn net.Conn
	if strings.Contains(ip_port, ".sock") {
		addr, err := net.ResolveUnixAddr("unix", ip_port[:strings.Index(ip_port, ".sock")+5])
		if err != nil {
			return err
		}
		conn, err = net.DialUnix("unix", nil, addr)
		if err != nil {
			return err
		}
	} else {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", ip_port)
		if err != nil {
			return err
		}
		conn, err = net.DialTCP("tcp", nil, tcpAddr)

		if err != nil {
			return err
		}
	}

	mysqlconn.conn = conn
	//new_connect.buf_4 = make([]byte, 4)
	//new_connect.buf_exec = []byte{0, 0, 0, 0, 3}
	err, seed, seed2 := mysqlconn.handshakePacket()
	if err != nil {
		return err
	}
	err = mysqlconn.handshakeResponse(seed, seed2, username, passwd, database, charset, tlsconfig)
	if err != nil {
		mysqlconn.Status = false
	} else {
		_, offset := time.Now().In(mysqlconn.loc).Zone()
		var time_zone string
		if offset >= 0 {
			time_zone = "+" + strconv.Itoa(offset/3600) + ":00"
		} else {
			time_zone = strconv.Itoa(offset/3600) + ":00"
		}
		_, _, err = mysqlconn.Exec([]byte("set time_zone='" + time_zone + "'"))
	}
	return err
}

var start_transaction = []byte{115, 116, 97, 114, 116, 32, 116, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110}

func (mysql *Mysql_Conn) Query(sql []byte, rows *MysqlRows) (err error) {

	msglen := len(sql) + 1
	if msglen > max_packet_size {
		err = errors.New("消息大于最大长度" + strconv.Itoa(max_packet_size))
		return
	}
	b := mysql.buffer.Make(5 + len(sql))
	b[0] = byte(msglen)
	b[1] = byte(msglen >> 8)
	b[2] = byte(msglen >> 16)
	b[3] = 0
	b[4] = 3
	copy(b[5:], sql)
	_, err = mysql.conn.Write(b)
	if err != nil {
		if strings.Contains(err.Error(), "connection reset by peer") {
			err = errors.New("EOF")
		}
		mysql.Status = false
		mysql.Close()
		return
	}
	mysql.buffer.Reset()
	var errmsg string
	_, _, rows.field_len, errmsg, err = mysql.readmsg()
	if errmsg != "" {
		if strings.Contains(errmsg, "1927-Connection was killed") {
			err = errors.New("EOF")
		} else { //err报文不影响mysql的status,在这里重新包装err
			err = errors.New(errmsg)
			return
		}
	}
	if err != nil {
		mysql.Status = false
		mysql.Close()
		return
	}
	err = rows.Columns(mysql)
	if err != nil {
		mysql.Status = false
		mysql.Close()
		return
	}
	//mysql.mysqlRows.msg_no = mysql.msg_no
	//DEBUG(mysql.buffer.Bytes())
	return nil
}
func (mysql *Mysql_Conn) Exec(sql []byte) (lastInsertId int64, rowsAffected int64, err error) {

	msglen := len(sql) + 1
	if msglen > max_packet_size {
		err = errors.New("消息大于最大长度" + strconv.Itoa(max_packet_size))
		return
	}
	b := mysql.buffer.Make(5 + len(sql))
	b[0] = byte(msglen)
	b[1] = byte(msglen >> 8)
	b[2] = byte(msglen >> 16)
	b[3] = 0
	b[4] = 3
	copy(b[5:], sql)
	//DEBUG(string(sql))
	_, err = mysql.conn.Write(b)
	if err != nil {
		if strings.Contains(err.Error(), "connection reset by peer") {
			err = errors.New("EOF")
		}
		mysql.Status = false
		mysql.Close()
		return
	}
	mysql.buffer.Reset()
	rowsAffected, lastInsertId, _, errmsg, err := mysql.readmsg()
	if errmsg != "" {
		if strings.Contains(errmsg, "1927-Connection was killed") {
			err = errors.New("EOF")
		} else { //err报文不影响mysql的status,在这里重新包装err
			err = errors.New(errmsg)
			return
		}
	}
	if err != nil {
		mysql.Status = false
		mysql.Close()
		return
	}
	return
}

//一次性读取n个字节

func (mysql *Mysql_Conn) read(need int) error {

	olen := mysql.buffer.Len()
	n, err := mysql.conn.Read(mysql.buffer.Make(need))
	if err != nil {
		mysql.buffer.Truncate(olen)
		return err
	}
	mysql.buffer.Truncate(olen + n)

	return nil
}

//握手包
func (mysql *Mysql_Conn) handshakePacket() (err error, seed []byte, seed2 []byte) {
	conn := mysql.conn
	mysql.buffer.Reset()
	msglen, err := mysql.readOneMsg()
	if msglen < 1 || err != nil {
		err = errors.New(conn.RemoteAddr().String() + "连接数据库失败，获取消息报文长度错误")
		return
	}

	switch mysql.buffer.Next(1)[0] {
	case 10:
		break
	case 255:
		mysql.buffer.Next(1)
		err = errors.New("连接失败" + mysql.buffer.String())
		return
	default:
		err = errors.New(conn.RemoteAddr().String() + "连接数据库失败,不支持的协议版本")
		return
	}

	//mysql.Version, err =
	ReadNullTerminatedString(mysql.buffer)
	b := mysql.buffer.Next(4)
	mysql.Thread_id = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24

	seed = make([]byte, 8)
	_, err = mysql.buffer.Read(seed)

	if err != nil {
		err = errors.New(conn.RemoteAddr().String() + "连接数据库失败,无法获取seed")
		return
	}

	mysql.buffer.ReadByte() //读取0x00

	//mysql.serverCapabilities = binary.LittleEndian.Uint16()
	//mysql.buffer.Next(8)
	//mysql.buffer.Read(mysql.buf_4[:1])
	//mysql.serverCharsetIndex = mysql.buf_4[0]
	//mysql.serverStatus = binary.LittleEndian.Uint16(mysql.buffer.Next(2))

	//new_connect.restOfScrambleBuff = make([]byte, 13)

	b = mysql.buffer.Next(2)
	mysql.Capabilities = uint32(b[0]) | uint32(b[1])<<8
	if mysql.buffer.Len() > 0 {
		mysql.buffer.Shift(3)
		b = mysql.buffer.Next(2)
		mysql.Capabilities |= uint32(b[0])<<16 | uint32(b[1])<<24
		authlen, _ := mysql.buffer.ReadByte()

		if authlen == 0 || authlen != 21 { //seed2长度是authlen-8,  13位，12位值+0值
			return
		}
		mysql.buffer.Shift(10) //读取10个字节
		seed2 = make([]byte, 12)
		mysql.buffer.Read(seed2)
		mysql.buffer.Next(1)
		if mysql.Capabilities&CLIENT_PLUGIN_AUTH != 0 {

			if mysql.auth_plugin_name, _ = ReadNullTerminatedString(mysql.buffer); mysql.auth_plugin_name != "mysql_native_password" && mysql.auth_plugin_name != "caching_sha2_password" {
				err = errors.New(conn.RemoteAddr().String() + "连接数据库失败,不支持的密码协议" + mysql.auth_plugin_name + "，期望值是mysql_native_password与caching_sha2_password")
				return
			}
		}

	}

	if mysql.Capabilities&CLIENT_PROTOCOL_41 == 0 {
		err = errors.New(conn.RemoteAddr().String() + "连接数据库失败,服务器版本太旧，不支持4.1协议")
		return
	}

	//mysql.buffer.Read(make([]byte, 1)) //读取0x00
	//mysql.auth_plugin_name, _ = ReadNullTerminatedString(mysql.buffer)
	//reader.Read(new_connect.restOfScrambleBuff)
	return
}

func (mysql *Mysql_Conn) handshakeResponse(seed, seed2 []byte, username, passwd, database, charset string, tlsConfig *tls.Config) error {
	capability_flags := uint32(CLIENT_PROTOCOL_41)
	if mysql.Capabilities&CLIENT_CONNECT_WITH_DB != 0 {
		capability_flags |= CLIENT_CONNECT_WITH_DB
	}
	if mysql.Capabilities&CLIENT_PLUGIN_AUTH != 0 {
		capability_flags |= CLIENT_PLUGIN_AUTH
	}
	if mysql.Capabilities&CLIENT_SECURE_CONNECTION != 0 {
		capability_flags |= CLIENT_SECURE_CONNECTION
	}
	clientCharsetIndex := collations[charset]
	if clientCharsetIndex == 0 {
		clientCharsetIndex = 33
	}
	//binary.Write(reader, binary.LittleEndian, uint32(new_connect.capability_flags))
	if mysql.Capabilities&CLIENT_SSL != 0 && tlsConfig != nil {
		capability_flags |= CLIENT_SSL
		if err := mysql.handshakeSSL(capability_flags, clientCharsetIndex, tlsConfig); err != nil {
			return err
		}

	}
	mysql.buffer.Reset()

	binary.LittleEndian.PutUint32(mysql.buffer.Make(4), capability_flags)
	binary.LittleEndian.PutUint32(mysql.buffer.Make(4), uint32(max_packet_size))

	mysql.buffer.WriteByte(clientCharsetIndex)
	mysql.buffer.Make(23)

	WriteNullTerminatedString(mysql.buffer, username)

	if mysql.Capabilities&CLIENT_SECURE_CONNECTION != 0 {
		Write1lenmsg(mysql.buffer, mysql.prepare_password(seed, seed2, passwd))
	} else {
		WriteNullmsg(mysql.buffer, mysql.prepare_password(seed, seed2, passwd))
	}

	if mysql.Capabilities&CLIENT_CONNECT_WITH_DB != 0 {
		WriteNullTerminatedString(mysql.buffer, database)
	}
	if mysql.Capabilities&CLIENT_PLUGIN_AUTH != 0 {
		WriteNullTerminatedString(mysql.buffer, mysql.auth_plugin_name)
	}
	msg := make([]byte, mysql.buffer.Len())
	copy(msg, mysql.buffer.Bytes())
	mysql.writemsg(msg)
	mysql.buffer.Reset()

	if mysql.auth_plugin_name == "caching_sha2_password" {
		msglen, err := mysql.readOneMsg()
		if err != nil {
			return err
		}

		buffer := mysql.buffer.Bytes()[:msglen]
		//mysql8这里返回一个0x01 0x03
		if msglen != 2 || buffer[0] != 1 || buffer[1] != 3 {
			return errors.New("caching_sha2_password握手返回未知消息包" + fmt.Sprintf("% x", buffer))
		}
		mysql.buffer.Next(msglen)
	}
	_, _, _, errmsg, err := mysql.readmsg()
	if errmsg != "" {
		return errors.New(errmsg)
	}
	if err != nil {
		return err
	}

	mysql.Status = true
	if mysql.Capabilities&CLIENT_CONNECT_WITH_DB == 0 && database != "" { //未验证
		mysql.Exec([]byte("use " + database))
	}
	return nil
}
func (mysql *Mysql_Conn) handshakeSSL(capability_flags uint32, clientCharsetIndex byte, tlsConfig *tls.Config) error {

	mysql.buffer.Reset()
	binary.LittleEndian.PutUint32(mysql.buffer.Make(4), capability_flags)
	binary.LittleEndian.PutUint32(mysql.buffer.Make(4), uint32(max_packet_size))
	mysql.buffer.WriteByte(clientCharsetIndex)
	mysql.buffer.Make(23)
	msg := make([]byte, mysql.buffer.Len())
	copy(msg, mysql.buffer.Bytes())

	mysql.writemsg(msg)
	tconn := tls.Client(mysql.conn, tlsConfig.Clone())
	if err := tconn.Handshake(); err != nil {
		return err
	}
	mysql.conn = tconn
	mysql.buffer.Reset()
	return nil
}
func (mysql *Mysql_Conn) readmsg() (rowsAffected, lastInsertId int64, result int, errmsg string, err error) {
	msglen, err := mysql.readOneMsg()
	if err != nil {
		return
	}

	buffer := mysql.buffer.Bytes()[:msglen]
	switch buffer[0] {
	case 0: //ok报文
		var r, l int

		mysql.buffer.Next(1)
		r, err = ReadLength_Coded_Binary(mysql.buffer)
		if err != nil {
			return
		}
		l, err = ReadLength_Coded_Binary(mysql.buffer)
		if err != nil {
			return
		}

		mysql.buffer.Shift(4)

		//mysql.serverStatus = binary.LittleEndian.Uint16(mysql.buffer.Next(2))
		if err != nil {
			return
		}
		return int64(r), int64(l), 0, "", nil
	case 255: //err报文

		mysql.buffer.Next(1)
		b := mysql.buffer.Next(2)
		errcode := int(b[0]) | int(b[1])<<8
		if mysql.Status { //未连接成功之前
			mysql.buffer.Shift(6)
		}
		msg, err := ioutil.ReadAll(mysql.buffer)
		if err != nil {
			return 0, 0, 255, "", err
		}
		return 0, 0, 255, strconv.Itoa(errcode) + "-" + string(msg), nil
	case 254: //EOF报文

		mysql.buffer.Shift(5)
		return 0, 0, 254, "", nil
	default: //Result Set报文
		result, err = ReadLength_Coded_Binary(mysql.buffer)
		return
	}
	return 0, 0, 0, "", nil
}

//至少读一条消息
func (mysql *Mysql_Conn) readOneMsg() (msglen int, err error) {

	for mysql.buffer.Len() < 4 { //至少包含长度
		err = mysql.read(16384) //读取一定字节
		if err != nil {
			return
		}
	}
	b := mysql.buffer.Next(4)
	msglen = int(b[0]) | int(b[1])<<8 | int(b[2])<<16
	if msglen > max_packet_size {
		return 0, errors.New("EOF")
	}
	for mysql.buffer.Len() < msglen { //至少包含一条消息的长度
		err = mysql.read(msglen - mysql.buffer.Len())
		if err != nil {
			return 0, err
		}
	}
	mysql.msg_no = b[3]
	return
}
func (mysql *Mysql_Conn) writemsg(msg []byte) error {
	msglen := len(msg)
	if msglen > max_packet_size {
		return errors.New("消息大于最大长度" + strconv.Itoa(max_packet_size))
	}
	mysql.buffer.Reset()
	b := mysql.buffer.Make(3)
	b[0] = byte(msglen)
	b[1] = byte(msglen >> 8)
	b[2] = byte(msglen >> 16)
	mysql.msg_no++

	mysql.buffer.WriteByte(mysql.msg_no)
	mysql.buffer.Write(msg)

	_, err := mysql.conn.Write(mysql.buffer.Bytes())
	if err != nil {
		mysql.Status = false
		mysql.Close()
		return err
	}

	return err
}
func (mysql *Mysql_Conn) prepare_password(seed, seed2 []byte, passwd string) []byte {
	if passwd == "" {
		return nil
	}
	switch mysql.auth_plugin_name {
	case "mysql_native_password":
		h := sha1.New()
		h.Write(Str2bytes(passwd))
		s1 := h.Sum(nil)
		h.Reset()
		h.Write(s1)
		s2 := h.Sum(nil)
		h.Reset()
		h.Write(seed)
		h.Write(seed2)
		h.Write(s2)
		s3 := h.Sum(nil)
		reply := make([]byte, len(s1))
		for k, _ := range s1 {
			reply[k] = s1[k] ^ s3[k]
		}
		return reply
	case "caching_sha2_password":
		h := sha256.New()
		h.Write(Str2bytes(passwd))
		s1 := h.Sum(nil)
		h.Reset()
		h.Write(s1)
		s2 := h.Sum(nil)
		h.Reset()
		h.Write(s2)
		h.Write(seed)
		h.Write(seed2)

		s3 := h.Sum(nil)
		reply := make([]byte, len(s1))
		for k, _ := range s1 {
			reply[k] = s1[k] ^ s3[k]
		}
		return reply
	}
	return nil

}
func Write1lenmsg(write *MsgBuffer, msg []byte) {
	msglen := len(msg)
	if msglen > 255 {
		return
	}
	write.WriteByte(uint8(msglen))
	write.Write(msg)
}
func Writelenmsg(write *MsgBuffer, msg []byte) {
	n := len(msg)
	switch {
	case n <= 250:
		write.WriteByte(byte(n))
	case n <= 0xffff:
		b := write.Make(3)
		b[0], b[1], b[2] = 0xfc, byte(n), byte(n>>8)
	case n <= 0xffffff:
		b := write.Make(4)
		b[0], b[1], b[2], b[3] = 0xfd, byte(n), byte(n>>8), byte(n>>16)
	default:
		b := write.Make(9)
		b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8] = 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24), byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56)
	}
	write.Write(msg)

}
func WriteNullmsg(write *MsgBuffer, msg []byte) {
	write.Write(msg)
	write.WriteByte(0)
}
func WriteNullTerminatedString(write *MsgBuffer, msg string) {
	write.Write(Str2bytes(msg))
	write.WriteByte(0)
}
func ReadLength_Coded_Binary(buf *MsgBuffer) (int, error) {
	if buf.Len() == 0 {
		return 0, errors.New("ReadLength_Coded_Binary err: buff length 0")
	}
	lentype, _ := buf.ReadByte()
	switch {
	case lentype < 251:
		return int(lentype), nil
	case lentype == 251:
		return 0, errors.New("NULL")
	case lentype == 252:
		if buf.Len() < 2 {
			return 0, errors.New("ReadLength_Coded_Binary err1")
		}
		b := buf.Next(2)
		return int(b[0]) | int(b[1])<<8, nil
	case lentype == 253:
		if buf.Len() < 3 {
			return 0, errors.New("ReadLength_Coded_Binary err2")
		}
		b := buf.Next(3)
		return int(b[0]) | int(b[1])<<8 | int(b[2])<<16, nil
	case lentype == 254:
		if buf.Len() < 8 {
			return 0, errors.New("ReadLength_Coded_Binary err3")
		}
		b := buf.Next(8)
		return int(b[0]) | int(b[1])<<8 | int(b[2])<<16 | int(b[3])<<24 | int(b[4])<<32 | int(b[5])<<40 | int(b[6])<<48 | int(b[7])<<56, nil
	}
	return 0, nil
}
func ReadLength_Coded_Slice(data []byte, pos *int) (l int, err error) {
	if len(data) == 0 {
		return 0, errors.New("ReadLength_Coded_Slice err: buff length 0")
	}
	switch {
	case data[0] < 251:
		*pos++
		return int(data[0]), nil
	case data[0] == 251:
		*pos++
		return 0, errors.New("NULL")
	case data[0] == 252:
		if len(data) < 2 {
			return 0, errors.New("ReadLength_Coded_Slice err1")
		}
		*pos = *pos + 3
		return int(data[1]) | int(data[2])<<8, nil
	case data[0] == 253:
		if len(data) < 3 {
			return 0, errors.New("ReadLength_Coded_Slice err2")
		}
		*pos = *pos + 4
		return int(data[1]) | int(data[2])<<8 | int(data[3])<<16, nil
	case data[0] == 254:
		if len(data) < 8 {
			return 0, errors.New("ReadLength_Coded_Slice err3")
		}
		*pos = *pos + 9
		return int(data[1]) | int(data[2])<<8 | int(data[3])<<16 | int(data[4])<<24 | int(data[5])<<32 | int(data[6])<<40 | int(data[7])<<48 | int(data[8])<<56, nil
	}
	return 0, nil
}
func ReadNullTerminatedString(msg *MsgBuffer) (string, error) {
	var b []byte
	top := msg.Bytes()
	for k, v := range top {
		if v == 0 {
			b = make([]byte, k)
			copy(b, msg.Next(k+1))
			break
		}
	}
	return Bytes2str(b), nil
}

func ReadLength_Coded_Byte(msg *MsgBuffer) ([]byte, error) {
	msglen, err := ReadLength_Coded_Binary(msg)
	if err != nil {
		if err.Error() == "NULL" {
			return []byte("NULL"), nil
		}
		return nil, err
	}
	if msglen == 0 {
		return nil, nil
	}
	return msg.Next(int(msglen)), nil
}
func ReadLengthCodedStringFromBuffer(msg *MsgBuffer, return_str bool) (string, error) {
	msglen, err := ReadLength_Coded_Binary(msg)
	if err != nil {
		if err.Error() == "NULL" {
			return "NULL", nil
		}
		return "", err
	}
	if msglen == 0 {
		return "", nil
	}
	if return_str {
		return string(msg.Next(int(msglen))), err
	}
	msg.Shift(int(msglen))
	return "", err
}

var collations = map[string]byte{
	"big5_chinese_ci":          1,
	"latin2_czech_cs":          2,
	"dec8_swedish_ci":          3,
	"cp850_general_ci":         4,
	"latin1_german1_ci":        5,
	"hp8_english_ci":           6,
	"koi8r_general_ci":         7,
	"latin1_swedish_ci":        8,
	"latin2_general_ci":        9,
	"swe7_swedish_ci":          10,
	"ascii_general_ci":         11,
	"ujis_japanese_ci":         12,
	"sjis_japanese_ci":         13,
	"cp1251_bulgarian_ci":      14,
	"latin1_danish_ci":         15,
	"hebrew_general_ci":        16,
	"tis620_thai_ci":           18,
	"euckr_korean_ci":          19,
	"latin7_estonian_cs":       20,
	"latin2_hungarian_ci":      21,
	"koi8u_general_ci":         22,
	"cp1251_ukrainian_ci":      23,
	"gb2312_chinese_ci":        24,
	"gb2312":                   24,
	"greek_general_ci":         25,
	"cp1250_general_ci":        26,
	"latin2_croatian_ci":       27,
	"gbk_chinese_ci":           28,
	"gbk":                      28,
	"cp1257_lithuanian_ci":     29,
	"latin5_turkish_ci":        30,
	"latin1_german2_ci":        31,
	"armscii8_general_ci":      32,
	"utf8_general_ci":          33,
	"utf8":                     33,
	"cp1250_czech_cs":          34,
	"ucs2_general_ci":          35,
	"cp866_general_ci":         36,
	"keybcs2_general_ci":       37,
	"macce_general_ci":         38,
	"macroman_general_ci":      39,
	"cp852_general_ci":         40,
	"latin7_general_ci":        41,
	"latin7_general_cs":        42,
	"macce_bin":                43,
	"cp1250_croatian_ci":       44,
	"utf8mb4_general_ci":       45,
	"utf8mb4_bin":              46,
	"latin1_bin":               47,
	"latin1_general_ci":        48,
	"latin1_general_cs":        49,
	"cp1251_bin":               50,
	"cp1251_general_ci":        51,
	"cp1251_general_cs":        52,
	"macroman_bin":             53,
	"utf16_general_ci":         54,
	"utf16_bin":                55,
	"utf16le_general_ci":       56,
	"cp1256_general_ci":        57,
	"cp1257_bin":               58,
	"cp1257_general_ci":        59,
	"utf32_general_ci":         60,
	"utf32_bin":                61,
	"utf16le_bin":              62,
	"binary":                   63,
	"armscii8_bin":             64,
	"ascii_bin":                65,
	"cp1250_bin":               66,
	"cp1256_bin":               67,
	"cp866_bin":                68,
	"dec8_bin":                 69,
	"greek_bin":                70,
	"hebrew_bin":               71,
	"hp8_bin":                  72,
	"keybcs2_bin":              73,
	"koi8r_bin":                74,
	"koi8u_bin":                75,
	"latin2_bin":               77,
	"latin5_bin":               78,
	"latin7_bin":               79,
	"cp850_bin":                80,
	"cp852_bin":                81,
	"swe7_bin":                 82,
	"utf8_bin":                 83,
	"big5_bin":                 84,
	"euckr_bin":                85,
	"gb2312_bin":               86,
	"gbk_bin":                  87,
	"sjis_bin":                 88,
	"tis620_bin":               89,
	"ucs2_bin":                 90,
	"ujis_bin":                 91,
	"geostd8_general_ci":       92,
	"geostd8_bin":              93,
	"latin1_spanish_ci":        94,
	"cp932_japanese_ci":        95,
	"cp932_bin":                96,
	"eucjpms_japanese_ci":      97,
	"eucjpms_bin":              98,
	"cp1250_polish_ci":         99,
	"utf16_unicode_ci":         101,
	"utf16_icelandic_ci":       102,
	"utf16_latvian_ci":         103,
	"utf16_romanian_ci":        104,
	"utf16_slovenian_ci":       105,
	"utf16_polish_ci":          106,
	"utf16_estonian_ci":        107,
	"utf16_spanish_ci":         108,
	"utf16_swedish_ci":         109,
	"utf16_turkish_ci":         110,
	"utf16_czech_ci":           111,
	"utf16_danish_ci":          112,
	"utf16_lithuanian_ci":      113,
	"utf16_slovak_ci":          114,
	"utf16_spanish2_ci":        115,
	"utf16_roman_ci":           116,
	"utf16_persian_ci":         117,
	"utf16_esperanto_ci":       118,
	"utf16_hungarian_ci":       119,
	"utf16_sinhala_ci":         120,
	"utf16_german2_ci":         121,
	"utf16_croatian_ci":        122,
	"utf16_unicode_520_ci":     123,
	"utf16_vietnamese_ci":      124,
	"ucs2_unicode_ci":          128,
	"ucs2_icelandic_ci":        129,
	"ucs2_latvian_ci":          130,
	"ucs2_romanian_ci":         131,
	"ucs2_slovenian_ci":        132,
	"ucs2_polish_ci":           133,
	"ucs2_estonian_ci":         134,
	"ucs2_spanish_ci":          135,
	"ucs2_swedish_ci":          136,
	"ucs2_turkish_ci":          137,
	"ucs2_czech_ci":            138,
	"ucs2_danish_ci":           139,
	"ucs2_lithuanian_ci":       140,
	"ucs2_slovak_ci":           141,
	"ucs2_spanish2_ci":         142,
	"ucs2_roman_ci":            143,
	"ucs2_persian_ci":          144,
	"ucs2_esperanto_ci":        145,
	"ucs2_hungarian_ci":        146,
	"ucs2_sinhala_ci":          147,
	"ucs2_german2_ci":          148,
	"ucs2_croatian_ci":         149,
	"ucs2_unicode_520_ci":      150,
	"ucs2_vietnamese_ci":       151,
	"ucs2_general_mysql500_ci": 159,
	"utf32_unicode_ci":         160,
	"utf32_icelandic_ci":       161,
	"utf32_latvian_ci":         162,
	"utf32_romanian_ci":        163,
	"utf32_slovenian_ci":       164,
	"utf32_polish_ci":          165,
	"utf32_estonian_ci":        166,
	"utf32_spanish_ci":         167,
	"utf32_swedish_ci":         168,
	"utf32_turkish_ci":         169,
	"utf32_czech_ci":           170,
	"utf32_danish_ci":          171,
	"utf32_lithuanian_ci":      172,
	"utf32_slovak_ci":          173,
	"utf32_spanish2_ci":        174,
	"utf32_roman_ci":           175,
	"utf32_persian_ci":         176,
	"utf32_esperanto_ci":       177,
	"utf32_hungarian_ci":       178,
	"utf32_sinhala_ci":         179,
	"utf32_german2_ci":         180,
	"utf32_croatian_ci":        181,
	"utf32_unicode_520_ci":     182,
	"utf32_vietnamese_ci":      183,
	"utf8_unicode_ci":          192,
	"utf8_icelandic_ci":        193,
	"utf8_latvian_ci":          194,
	"utf8_romanian_ci":         195,
	"utf8_slovenian_ci":        196,
	"utf8_polish_ci":           197,
	"utf8_estonian_ci":         198,
	"utf8_spanish_ci":          199,
	"utf8_swedish_ci":          200,
	"utf8_turkish_ci":          201,
	"utf8_czech_ci":            202,
	"utf8_danish_ci":           203,
	"utf8_lithuanian_ci":       204,
	"utf8_slovak_ci":           205,
	"utf8_spanish2_ci":         206,
	"utf8_roman_ci":            207,
	"utf8_persian_ci":          208,
	"utf8_esperanto_ci":        209,
	"utf8_hungarian_ci":        210,
	"utf8_sinhala_ci":          211,
	"utf8_german2_ci":          212,
	"utf8_croatian_ci":         213,
	"utf8_unicode_520_ci":      214,
	"utf8_vietnamese_ci":       215,
	"utf8_general_mysql500_ci": 223,
	"utf8mb4_unicode_ci":       224,
	"utf8mb4_icelandic_ci":     225,
	"utf8mb4_latvian_ci":       226,
	"utf8mb4_romanian_ci":      227,
	"utf8mb4_slovenian_ci":     228,
	"utf8mb4_polish_ci":        229,
	"utf8mb4_estonian_ci":      230,
	"utf8mb4_spanish_ci":       231,
	"utf8mb4_swedish_ci":       232,
	"utf8mb4_turkish_ci":       233,
	"utf8mb4_czech_ci":         234,
	"utf8mb4_danish_ci":        235,
	"utf8mb4_lithuanian_ci":    236,
	"utf8mb4_slovak_ci":        237,
	"utf8mb4_spanish2_ci":      238,
	"utf8mb4_roman_ci":         239,
	"utf8mb4_persian_ci":       240,
	"utf8mb4_esperanto_ci":     241,
	"utf8mb4_hungarian_ci":     242,
	"utf8mb4_sinhala_ci":       243,
	"utf8mb4_german2_ci":       244,
	"utf8mb4_croatian_ci":      245,
	"utf8mb4_unicode_520_ci":   246,
	"utf8mb4_vietnamese_ci":    247,
}
