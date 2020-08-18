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
