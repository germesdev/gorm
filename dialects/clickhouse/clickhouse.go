package clickhouse

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/germesdev/clickhouse"
	"github.com/germesdev/gorm"
)

func init() {
	gorm.RegisterDialect("clickhouse", &clickhouse{})
}

type clickhouse struct {
	db gorm.SQLCommon
	gorm.DefaultForeignKeyNamer
}

func (clickhouse) GetName() string {
	return "clickhouse"
}

func (s *clickhouse) SetDB(db gorm.SQLCommon) {
	s.db = db
}

func (clickhouse) BindVar(i int) string {
	return "$$$" // ?
}

func (clickhouse) Quote(key string) string {
	return fmt.Sprintf(`"%s"`, key)
}

func (s *clickhouse) fieldCanAutoIncrement(field *gorm.StructField) bool {
	if value, ok := field.TagSettings["AUTO_INCREMENT"]; ok {
		return strings.ToLower(value) != "false"
	}
	return field.IsPrimaryKey
}

func (s *clickhouse) DataTypeOf(field *gorm.StructField) string {
	var dataValue, sqlType, _, additionalType = gorm.ParseFieldStructForDialect(field, s)

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "Uint8"

		case reflect.Int8:
			sqlType = "Int8"
		case reflect.Int, reflect.Int32:
			sqlType = "Int32"
		case reflect.Int64:
			sqlType = "Int64"
		case reflect.Int16:
			sqlType = "Int16"

		case reflect.Uint8:
			sqlType = "UInt8"
		case reflect.Uint, reflect.Uint32, reflect.Uintptr:
			sqlType = "UInt32"
		case reflect.Uint64:
			sqlType = "UInt64"
		case reflect.Uint16:
			sqlType = "UInt16"

		case reflect.Float32:
			sqlType = "Float32"
		case reflect.Float64:
			sqlType = "Float64"

		case reflect.String:
			sqlType = "String"

		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "DateTime"
			}
		default:
			if _, ok := dataValue.Interface().([]byte); ok {
				sqlType = "String"
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) for clickhouse", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s clickhouse) HasIndex(tableName string, indexName string) bool {
	return false
}

func (s clickhouse) RemoveIndex(tableName string, indexName string) error {
	return nil
}

func (s clickhouse) HasForeignKey(tableName string, foreignKeyName string) bool {
	return false
}

func (s clickhouse) HasTable(tableName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count() FROM system.tables WHERE database = ? AND name = ?", currentDatabase, tableName).Scan(&count)
	return count > 0
}

func (s clickhouse) HasColumn(tableName string, columnName string) bool {
	var count int
	currentDatabase, tableName := currentDatabaseAndTable(&s, tableName)
	s.db.QueryRow("SELECT count() FROM system.columns WHERE database = ? AND table = ? AND name = ?", currentDatabase, tableName, columnName).Scan(&count)
	return count > 0
}

func (s clickhouse) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %v MODIFY COLUMN %v %v", tableName, columnName, typ))
	return err
}

func (s clickhouse) CurrentDatabase() (name string) {
	s.db.QueryRow("SELECT currentDatabse()").Scan(&name)
	return
}

func (clickhouse) LimitAndOffsetSQL(limit, offset interface{}) (sql string) {
	if limit != nil {
		if parsedLimit, err := strconv.ParseInt(fmt.Sprint(limit), 0, 0); err == nil && parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)
		}
	}
	if offset != nil {
		if parsedOffset, err := strconv.ParseInt(fmt.Sprint(offset), 0, 0); err == nil && parsedOffset >= 0 {
			sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
		}
	}
	return
}

func (clickhouse) SelectFromDummyTable() string {
	return ""
}

func (clickhouse) LastInsertIDReturningSuffix(tableName, columnName string) string {
	return ""
}

func (clickhouse) DefaultValueStr() string {
	return "DEFAULT VALUES"
}

// IsByteArrayOrSlice returns true of the reflected value is an array or slice
func IsByteArrayOrSlice(value reflect.Value) bool {
	return (value.Kind() == reflect.Array || value.Kind() == reflect.Slice) && value.Type().Elem() == reflect.TypeOf(uint8(0))
}

func currentDatabaseAndTable(dialect gorm.Dialect, tableName string) (string, string) {
	if strings.Contains(tableName, ".") {
		splitStrings := strings.SplitN(tableName, ".", 2)
		return splitStrings[0], splitStrings[1]
	}
	return dialect.CurrentDatabase(), tableName
}
