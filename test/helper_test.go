package jgpg_test

import (
	"fmt"
	"testing"

	jgconf "github.com/Jinglever/go-config"
	jgpg "github.com/Jinglever/go-postgres"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// open connection to postgresql
func openDB() *gorm.DB {
	cfg := struct {
		Host   string `mapstructure:"host"`
		Port   string `mapstructure:"port"`
		User   string `mapstructure:"user"`
		Pass   string `mapstructure:"pass"`
		DBName string `mapstructure:"dbname"`
	}{}
	jgconf.LoadYamlConfig("./_data/conf.yml", &cfg)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.DBName)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Info),
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	})
	if err != nil {
		panic(err)
	}
	return db
}

// test helper QueryDBVersion
func TestQueryDBVersion(t *testing.T) {
	db := openDB()
	helper := jgpg.NewHelper(db)
	version, err := helper.QueryDBVersion()
	if err != nil {
		t.Errorf("query db version failed, err: %v", err)
	} else {
		t.Log(version)
	}
}

// test helper QueryDBCharset
func TestQueryDBCharset(t *testing.T) {
	db := openDB()
	helper := jgpg.NewHelper(db)
	charset, err := helper.QueryDBCharset()
	if err != nil {
		t.Errorf("query db charset failed, err: %v", err)
	} else {
		t.Log(charset)
	}
}

// test helper QueryDBCollate
func TestQueryDBCollate(t *testing.T) {
	db := openDB()
	helper := jgpg.NewHelper(db)
	collate, err := helper.QueryDBCollate()
	if err != nil {
		t.Errorf("query db collate failed, err: %v", err)
	} else {
		t.Log(collate)
	}
}

// test helper QueryAllTables
func TestQueryAllTables(t *testing.T) {
	db := openDB()
	helper := jgpg.NewHelper(db)
	tables, err := helper.QueryAllTables()
	if err != nil {
		t.Errorf("query all tables failed, err: %v", err)
	} else {
		t.Log(tables)
	}
}

// test helper QueryCreateTableSql
func TestQueryCreateTableSql(t *testing.T) {
	db := openDB()
	helper := jgpg.NewHelper(db)
	sql, err := helper.QueryCreateTableSql("user")
	if err != nil {
		t.Errorf("query create table sql failed, err: %v", err)
	} else {
		t.Log(sql)
	}
}
