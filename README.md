# mysql
一个随便写的mysql

import _ "github.com/luyu6056/mysql" 以代替其他mysql

例如gorm下调用此驱动
```go
import (
	"fmt"

	"github.com/jinzhu/gorm"
	_ "github.com/luyu6056/mysql"
)

func main() {

	gorm.Open("mysql", "yy:123456@tcp(172.26.45.140:3306)/bbs?charset=utf8")

	
}
```
# 优化内容
* 优化驱动与mysql传输时，语句的封装和网络数据读取，能减少所有场景下本地cpu占用。
* 当语句不存在?时，跳过预处理Prepare与close流程，在Execute阶段，直接提交原始语句到mysql，减少2句提交，能降低mysql服务器的cpu占用，减少网络通信数据。
* 加入stmt（预处理）缓冲map，短时间内频繁提交同一句预处理语句时，重复使用同一个stmt结构体，能缓解高并发下占用过高的预处理资源，导致mysql报max_prepared_stmt_count错误。

# 可能存在的问题
* 目前只做了mysql_native_password和caching_sha2_password密码协商套件，能保证mysql5与mysql8默认设置下正常通讯，使用其他加密套件将会无法连接。
* go-sql-driver/mysql里面对dsn定义了多个扩展参数，目前只能解析loc=xxxx，对于其他扩展参数将会无效
