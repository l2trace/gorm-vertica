package vertica

import (
	"bytes"
	"fmt"
	"testing"
	"time"
	
	"gorm.io/gorm"
)

type testTable struct {
	Id             int64   `gorm:"Column:id"`
	Name           string  `gorm:"Column:name"`
	Active         bool    `gorm:"Column:active"`
	TimestampField time.Time       `gorm:"Column:timestamp_field"`
	Float1         float64 `gorm:"Column:float_1"`
	Float2         float64 `gorm:"Column:float_2"`
	BinaryField1   string  `gorm:"Column:binary_field_1"`
	BinaryField2   string  `gorm:"Column:binary_field_2"`
	CharField1     string  `gorm:"Column:char_field_1"`
	CharField2     string  `gorm:"Column:char_field_2"`
	IntField1      int64   `gorm:"Column:int_field_1"`
	IntField2      int64   `gorm:"Column:int_field_2"`
	UuidField      string  `gorm:"Column:uuid_field"`
}

var dsn = `vertica://dbadmin:@vertica:5433/docker`

func TestConfig_Explain(t *testing.T) {
	type args struct {
		sql  string
		vars []interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "explain test",
			args: args{"select 1", nil},
			want: "select 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Dialector{}
			if got := c.Explain(tt.args.sql, tt.args.vars...); got != tt.want {
				t.Errorf("Explain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_QuoteTo(t *testing.T) {
	type args struct {
		writer *bytes.Buffer
		s      string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test quotes with text not needing quotes",
			args: args{bytes.NewBuffer([]byte{}), "test"},
			want: "test",
		},
		{
			name: "test quotes with text  needing quotes",
			args: args{bytes.NewBuffer([]byte{}), "select * from schema.database.table"},
			want: `select * from "schema"."database"."table"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Dialector{}
			c.QuoteTo(tt.args.writer, tt.args.s)

			if tt.args.writer.String() == tt.want {
				t.Errorf("Quote() = %v, want %v", tt.args.writer.String(), tt.want)
			}
		})
	}
}

func TestConnect(t *testing.T) {

	db := getConnection(t)
	data := testTable{}
	tx := db.Model(data).First(&data)
	if tx.Error != nil {
		t.Error(tx.Error)
	}
	fmt.Printf("%+v", data)

}

func TestInsert(t *testing.T) {

	data := testTable{
		Id:     2,
		Name:   "biscuit",
		Active: true,
		UuidField: "faedfd30-d14e-4e8c-a599-1b42bef91ce5",
	}

	db := getConnection(t)
	
	tx := db.Model(data).Save(&data)
	
	if tx.Error != nil {
		t.Error(tx.Error)
	}
	sql := db.Model(data).Save(&data).Debug().Statement.SQL.String()
	fmt.Println("-----------------")
	fmt.Println(sql)
	

	row := db.Raw("select count(*) from test_tables").Row()
	count := 0
	row.Scan(&count)
	
	fmt.Println(count)
}

func getConnection(t *testing.T) *gorm.DB {
	db, err := gorm.Open(Open(dsn), &gorm.Config{})

	if err != nil {
		t.Error(err)
	}
	return db
}
