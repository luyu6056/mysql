package mysql

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
)

type Database_rows struct {
	r    *MysqlRows
	stmt *Database_mysql_stmt
	line int
}

func (rows *Database_rows) Close() error {

	rows_pool.Put(rows.r)
	return nil
}
func (rows *Database_rows) Columns() (columns []string) {
	columns = make([]string, len(rows.r.columns))
	for k, v := range rows.r.columns {
		columns[k] = v.name
	}
	return
}
func (rows *Database_rows) Next(dest []driver.Value) (err error) {
	if rows.line >= rows.r.result_len {
		return io.EOF
	}
	columns := rows.r.columns
	if rows.stmt.numInput == -1 {

		data := rows.r.Buffer.Next(rows.r.msg_len[rows.line])
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

			switch columns[i].fieldtype {
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
				dest[i], _ = time.ParseInLocation("2006-01-02 15:04:05", str, rows.stmt.conn.loc)

			// Please report if this happens!
			default:
				return fmt.Errorf("unknown field type %d", columns[i].fieldtype)
			}

			pos += msglen

		}
	} else {

		msglen := rows.r.msg_len[rows.line]
		data := rows.r.Buffer.Next(msglen)

		if msglen == 5 && data[0] == 0xfe { //EOF
			return io.EOF
		}
		if data[0] != 0 {
			return errors.New("返回协议错误，返回的内容不是Binary Protocol")
		}
		pos := 1 + (len(dest)+7+2)>>3
		nullMask := data[1:pos]
		for i, _ := range dest {
			if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
				dest[i] = nil
				continue
			}

			// Convert to byte-coded string
			switch columns[i].fieldtype {
			case fieldTypeNULL:
				dest[i] = nil
				continue

			// Numeric Types
			case fieldTypeTiny:
				if columns[i].fleldflag&flagUnsigned != 0 {
					dest[i] = int64(data[pos])
				} else {
					dest[i] = int64(int8(data[pos]))
				}
				pos++
				continue

			case fieldTypeShort, fieldTypeYear:
				if columns[i].fleldflag&flagUnsigned != 0 {
					dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
				} else {
					dest[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
				}
				pos += 2
				continue

			case fieldTypeInt24, fieldTypeLong:
				if columns[i].fleldflag&flagUnsigned != 0 {
					dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
				} else {
					dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
				}
				pos += 4
				continue

			case fieldTypeLongLong:
				if columns[i].fleldflag&flagUnsigned != 0 {
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

				if msglen == 0 {
					dest[i] = nil
				} else {
					dest[i] = string(data[pos : pos+msglen])
				}
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
				case n == 0:
					dest[i] = nil
					continue
				case columns[i].fieldtype == fieldTypeTime:
					// database/sql does not support an equivalent to TIME, return a string
					var dstlen uint8
					switch decimals := columns[i].decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 8
					case 1, 2, 3, 4, 5, 6:
						dstlen = 8 + 1 + decimals
					default:
						return fmt.Errorf(
							"protocol error, illegal decimals value %d",
							columns[i].decimals,
						)
					}
					dest[i], err = formatBinaryTime(data[pos:pos+int(n)], dstlen)
				case rows.stmt.conn.Mysql_Conn.parseTime:
					dest[i], err = parseBinaryDateTime(uint64(n), data[pos:], rows.stmt.conn.Mysql_Conn.loc)
				default:
					var dstlen uint8
					if columns[i].fieldtype == fieldTypeDate {
						dstlen = 10
					} else {
						switch decimals := columns[i].decimals; decimals {
						case 0x00, 0x1f:
							dstlen = 19
						case 1, 2, 3, 4, 5, 6:
							dstlen = 19 + 1 + decimals
						default:
							return fmt.Errorf(
								"protocol error, illegal decimals value %d",
								columns[i].decimals,
							)
						}
					}
					dest[i], err = formatBinaryDateTime(data[pos:pos+int(n)], dstlen)
				}

				if err == nil {
					pos += int(n)
					continue
				} else {
					return err
				}

			// Please report if this happens!
			default:
				return fmt.Errorf("unknown field type %d", columns[i].fieldtype)
			}
		}
	}
	rows.line++
	return
}
