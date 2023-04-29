package jgpg

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type Helper struct {
	DB *gorm.DB
}

func NewHelper(db *gorm.DB) *Helper {
	return &Helper{DB: db}
}

// query version of database
func (h *Helper) QueryDBVersion() (string, error) {
	var version string
	err := h.DB.Raw("select version()").Scan(&version).Error
	if err != nil {
		return "", err
	}
	return version, nil
}

// query charset of database
func (h *Helper) QueryDBCharset() (string, error) {
	var records = make([]map[string]interface{}, 0)
	err := h.DB.Raw("SHOW SERVER_ENCODING").Scan(&records).Error
	if err != nil {
		return "", err
	}
	return records[0]["server_encoding"].(string), nil
}

// query collate of database
func (h *Helper) QueryDBCollate() (string, error) {
	var records = make([]map[string]interface{}, 0)
	err := h.DB.Raw("select datcollate from pg_database WHERE datname = current_database()").Scan(&records).Error
	if err != nil {
		return "", err
	}
	return records[0]["datcollate"].(string), nil
}

// query all tables of database
func (h *Helper) QueryAllTables() ([]string, error) {
	var tables = make([]map[string]interface{}, 0)
	err := h.DB.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'").Scan(&tables).Error
	if err != nil {
		return nil, err
	}
	var ret = make([]string, 0)
	for _, table := range tables {
		for _, v := range table {
			ret = append(ret, v.(string))
		}
	}
	return ret, nil
}

// query create table sql
func (h *Helper) QueryCreateTableSql(tableName string) (string, error) {
	var buf strings.Builder
	var table map[string]interface{}
	err := h.DB.Raw(fmt.Sprintf("select cast(obj_description(relfilenode,'pg_class') as varchar) as comment from pg_class c where relname ='%s';", tableName)).Scan(&table).Error
	if err != nil {
		return "", err
	}
	if len(table) == 0 {
		return "", fmt.Errorf("table %s not exists", tableName)
	}
	tableComment := getString(table["comment"])
	var cols = make([]map[string]interface{}, 0)
	err = h.DB.Raw(fmt.Sprintf("SELECT ordinal_position,column_name,udt_name AS data_type,numeric_precision,datetime_precision,numeric_scale,character_maximum_length AS data_length,is_nullable,column_name as check,column_name as check_constraint,column_default,column_name AS foreign_key,pg_catalog.col_description((select oid from pg_class where relname='%s'),ordinal_position) as comment FROM information_schema.columns WHERE table_name='%s'AND table_schema='public'", tableName, tableName)).Scan(&cols).Error
	if err != nil || len(cols) == 0 {
		return "", err
	}
	var colName2Col = make(map[string]map[string]interface{})
	for _, col := range cols {
		// fmt.Printf("%#v\n", jgstr.JsonEncode(col))
		colName2Col[col["column_name"].(string)] = col
	}
	var indexes = make([]map[string]interface{}, 0)
	err = h.DB.Raw(fmt.Sprintf("SELECT ix.relname as index_name, upper(am.amname) AS index_algorithm, indisunique as is_unique, pg_get_indexdef(indexrelid) as index_definition, replace(regexp_replace(regexp_replace(pg_get_indexdef(indexrelid), ' WHERE .+', ''), '.*\\((.*)\\)', '\\1'), ' ', '') as column_name, CASE WHEN position(' WHERE 'in pg_get_indexdef(indexrelid))>0 THEN regexp_replace(pg_get_indexdef(indexrelid),'.+WHERE ','')ELSE''END AS condition,pg_catalog.obj_description(i.indexrelid,'pg_class')as comment FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid JOIN pg_class ix ON ix.oid = i.indexrelid JOIN pg_namespace n ON t.relnamespace = n.oid JOIN pg_am as am ON ix.relam = am.oid WHERE t.relname = '%s' AND n.nspname = 'public';", tableName)).Scan(&indexes).Error
	if err != nil {
		return "", err
	}
	var indexName2Index = make(map[string]map[string]interface{})
	for _, index := range indexes {
		// fmt.Printf("%#v\n", jgstr.JsonEncode(index))
		indexName2Index[index["index_name"].(string)] = index
	}
	pkey := ""
	if _, ok := indexName2Index[tableName+"_pkey"]; ok {
		pkey = indexName2Index[tableName+"_pkey"]["column_name"].(string)
	}

	// create table
	buf.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", tableName))
	for idx, col := range cols {
		parts := make([]string, 0)
		colName := getString(col["column_name"])
		parts = append(parts, colName)
		dft := getString(col["column_default"])
		typ := getColumnType(getString(col["data_type"]))
		if strings.Contains(dft, "_seq'::regclass") {
			typ = convertInteger2Serial(typ)
			dft = ""
		}

		// for decimal type, consider numeric_precision and numeric_scale
		if typ == t_decimal {
			precision := getInt(col["numeric_precision"])
			scale := getInt(col["numeric_scale"])
			if precision > 0 && scale > 0 {
				typ = fmt.Sprintf("%s(%d,%d)", typ, precision, scale)
			} else if precision > 0 {
				typ = fmt.Sprintf("%s(%d)", typ, precision)
			}
		}
		// for varchar and char type, consider character_maximum_length
		if typ == t_varchar || typ == t_char {
			length := getInt(col["data_length"])
			if length > 0 {
				typ = fmt.Sprintf("%s(%d)", typ, length)
			}
		}

		parts = append(parts, strings.ToUpper(typ))
		attr := ""
		if colName == pkey {
			attr = "PRIMARY KEY"
		} else if getString(col["is_nullable"]) == "NO" {
			attr = "NOT NULL"
		}
		if attr != "" {
			parts = append(parts, attr)
		}
		if dft != "" {
			parts = append(parts, "DEFAULT "+dft)
		}
		buf.WriteString("  " + strings.Join(parts, " "))
		if idx == len(cols)-1 {
			buf.WriteString("\n")
		} else {
			buf.WriteString(",\n")
		}
	}
	buf.WriteString(");\n")
	// create index
	for _, index := range indexes {
		indexName := getString(index["index_name"])
		indexAlgorithm := getString(index["index_algorithm"])
		isUnique := getString(index["is_unique"])
		columnName := getString(index["column_name"])
		if indexName == tableName+"_pkey" {
			continue
		}
		if indexAlgorithm == "BTREE" {
			if isUnique == "true" {
				buf.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX ON %s USING BTREE (%s);\n", tableName, columnName))
			} else {
				buf.WriteString(fmt.Sprintf("CREATE INDEX ON %s USING BTREE (%s);\n", tableName, columnName))
			}
		} else if indexAlgorithm == "HASH" {
			if isUnique == "true" {
				buf.WriteString(fmt.Sprintf("CREATE UNIQUE INDEX ON %s USING HASH (%s);\n", tableName, columnName))
			} else {
				buf.WriteString(fmt.Sprintf("CREATE INDEX ON %s USING HASH (%s);\n", tableName, columnName))
			}
		}
	}
	// comment
	if tableComment != "" {
		buf.WriteString(fmt.Sprintf("COMMENT ON TABLE %s IS '%s';\n", tableName, tableComment))
	}
	for _, col := range cols {
		colName := getString(col["column_name"])
		colComment := getString(col["comment"])
		if colComment != "" {
			buf.WriteString(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';\n", tableName, colName, colComment))
		}
	}
	return buf.String(), nil
}

func getString(in interface{}) string {
	r, ok := in.(string)
	if !ok {
		r = ""
	}
	return r
}

func getInt(in interface{}) int {
	r, ok := in.(int)
	if !ok {
		r = 0
	}
	return r
}

const (
	// 数值类型
	t_smallint    = "smallint"
	t_int         = "int"
	t_bigint      = "bigint"
	t_decimal     = "decimal"
	t_float       = "float"
	t_double      = "double"
	t_smallserial = "smallserial"
	t_serial      = "serial"
	t_bigserial   = "bigserial"
	t_text        = "text"
	t_char        = "char"
	t_varchar     = "varchar"
	t_date        = "date"
	t_time        = "time"
	t_timestamp   = "timestamp"
	t_bool        = "bool"
)

func convertInteger2Serial(typ string) string {
	switch typ {
	case t_smallint:
		return t_smallserial
	case t_int:
		return t_serial
	case t_bigint:
		return t_bigserial
	default:
		return typ
	}
}

func getColumnType(typ string) string {
	typ = strings.ToLower(typ)
	switch typ {
	// 数值类型
	case "int2":
		return t_smallint
	case "integer":
		return t_int
	case "int4":
		return t_int
	case "int8":
		return t_bigint
	case "numeric":
		return t_decimal
	case "money":
		return t_decimal
	case "real":
		return t_float
	case "float4":
		return t_float
	case "float8":
		return t_float
	case "double precision":
		return t_double
	case "smallserial":
		return t_smallserial
	case "serial":
		return t_serial
	case "bigserial":
		return t_bigserial
	// 字符类型
	case "text":
		return t_text
	case "character":
		return t_char
	case "bpchar":
		return t_char
	case "character varying":
		return t_varchar
	// 日期/时间类型
	case "date":
		return t_date
	case "time with time zone":
		return t_time
	case "time without time zone":
		return t_time
	case "timestamp with time zone":
		return t_timestamp
	case "timestamp without time zone":
		return t_timestamp
	// 布尔类型
	case "boolean":
		return t_bool
	}
	return typ
}
