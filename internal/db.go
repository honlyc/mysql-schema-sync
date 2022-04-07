package internal

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2" // mysql driver
	_ "github.com/go-sql-driver/mysql"         // mysql driver
)

const (
	DRIVER_MYSQL = iota
	DRIVER_CLICKHOUSE
)

// MyDb db struct
type MyDb struct {
	Db       *sql.DB
	dbType   string
	dbDriver int
	cluster  string
}

// NewMyDb parse dsn
func NewMyDb(dsn string, dbDriver string, dbType string, cluster string) *MyDb {
	driver := "mysql"
	driverType := DRIVER_MYSQL
	switch dbDriver {
	case "mysql":
		driver = "mysql"
		driverType = DRIVER_MYSQL
		break
	case "clickhouse":
		driver = dbDriver
		driverType = DRIVER_CLICKHOUSE
		break
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		panic(fmt.Sprintf("connected to db [%s] failed,err=%s", dsn, err))
	}
	return &MyDb{
		Db:       db,
		dbType:   dbType,
		dbDriver: driverType,
		cluster:  cluster,
	}
}

var GET_TABLE_SQLS = map[int]string{
	DRIVER_MYSQL:      "show table status",
	DRIVER_CLICKHOUSE: "show tables",
}

// GetTableNames table names
func (db *MyDb) GetTableNames() []string {
	rs, err := db.Query(GET_TABLE_SQLS[db.dbDriver])
	if err != nil {
		panic("show tables failed:" + err.Error())
	}
	defer rs.Close()

	var tables []string
	columns, _ := rs.Columns()
	for rs.Next() {
		var values = make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rs.Scan(valuePtrs...); err != nil {
			panic("show tables failed when scan," + err.Error())
		}
		var valObj = make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			valObj[col] = v
		}
		if db.dbDriver == DRIVER_MYSQL {
			if valObj["Engine"] != nil {
				tables = append(tables, valObj["Name"].(string))
			}
		} else {
			tables = append(tables, valObj["name"].(string))
		}
	}
	return tables
}

// GetTableSchema table schema
func (db *MyDb) GetTableSchema(name string) (schema string) {
	rs, err := db.Query(fmt.Sprintf("show create table `%s`", name))
	if err != nil {
		log.Println(err)
		return
	}
	defer rs.Close()
	for rs.Next() {
		if db.dbDriver == DRIVER_MYSQL {
			var vname string
			if err := rs.Scan(&vname, &schema); err != nil {
				panic(fmt.Sprintf("get table %s 's schema failed, %s", name, err))
			}
		} else {
			if err := rs.Scan(&schema); err != nil {
				panic(fmt.Sprintf("get table %s 's schema failed, %s", name, err))
			}
		}
	}
	return
}

// GetDescTableSchema desc table schema
func (db *MyDb) GetDescTableSchema(database, name string) (schema string) {
	rs, err := db.Query(fmt.Sprintf("desc table `%s`.`%s`", database, name))
	if err != nil {
		log.Println(err)
		return
	}
	defer rs.Close()
	columns, _ := rs.Columns()
	var fields []string
	for rs.Next() {
		if db.dbDriver == DRIVER_MYSQL {
			var vname string
			if err := rs.Scan(&vname, &schema); err != nil {
				panic(fmt.Sprintf("get table %s 's schema failed, %s", name, err))
			}
		} else {
			var values = make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}
			if err := rs.Scan(valuePtrs...); err != nil {
				panic("show tables failed when scan," + err.Error())
			}
			var valObj = make(map[string]interface{})
			for i, col := range columns {
				var v interface{}
				val := values[i]
				b, ok := val.([]byte)
				if ok {
					v = string(b)
				} else {
					v = val
				}
				valObj[col] = v
			}
			fields = append(fields, parseField(valObj))
		}
	}
	schema = strings.Join(fields, "\n")
	return
}

func parseField(obj map[string]interface{}) string {
	str := "`%s` %s %s %s"
	sprintf := fmt.Sprintf(str, obj["name"], obj["type"], obj["default_type"],
		obj["default_expression"])
	return sprintf
}

// Query execute sql query
func (db *MyDb) Query(query string, args ...interface{}) (*sql.Rows, error) {
	log.Println("[SQL]", "["+db.dbType+"]", query, args)
	return db.Db.Query(query, args...)
}
