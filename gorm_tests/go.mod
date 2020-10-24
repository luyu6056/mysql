module gorm.io/gorm/tests

go 1.14

require (
	github.com/google/uuid v1.1.1
	github.com/jinzhu/now v1.1.1
	github.com/lib/pq v1.6.0
	github.com/luyu6056/mysql v1.0.2
	gorm.io/driver/mysql v1.0.3
	gorm.io/driver/postgres v1.0.5
	gorm.io/driver/sqlite v1.1.3
	gorm.io/driver/sqlserver v1.0.5
	gorm.io/gorm v1.20.5
)

replace github.com/luyu6056/mysql => ../
replace gorm.io/driver/mysql => ./mysql