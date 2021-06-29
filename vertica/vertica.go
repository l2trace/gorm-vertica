package vertica

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	
	_ "github.com/vertica/vertica-sql-go"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type Config struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
}

type Dialector struct {
	*Config
}

func (d Dialector) Name() string {
	return "vertica"
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (d Dialector) Initialize(db *gorm.DB) error {
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		UpdateClauses: []string{"UPDATE", "SET", "WHERE", "ORDER BY", "LIMIT"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "ORDER BY", "LIMIT"},
	})
	
	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		var err error
		db.ConnPool, err = sql.Open("vertica", d.Config.DSN)
		if err != nil {
			return err
		}
	}
	
	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return nil
}

//TODO Implement this
func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	//return Migrator{
	//	Migrator: migrator.Migrator{
	//		Config: migrator.Config{
	//			DB:        db,
	//			Dialector: d,
	//		},
	//	},
	//	Dialector: d,
	//}
	return nil
}

//TODO
func (d Dialector) DataTypeOf(field *schema.Field) string {
	fmt.Println("----------------------")
	fmt.Sprintf("%+v", field)
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		sqlType := "bigint"
		switch {
		case field.Size <= 8:
			sqlType = "tinyint"
		case field.Size <= 16:
			sqlType = "smallint"
		case field.Size <= 24:
			sqlType = "mediumint"
		case field.Size <= 32:
			sqlType = "int"
		}
		
		if field.DataType == schema.Uint {
			sqlType += " unsigned"
		}
		
		if field.AutoIncrement {
			sqlType += " AUTO_INCREMENT"
		}
		return sqlType
	case schema.Float:
		if field.Precision > 0 {
			return fmt.Sprintf("decimal(%d, %d)", field.Precision, field.Scale)
		}
		
		if field.Size <= 32 {
			return "float"
		}
		return "double"
	case schema.String:
		size := field.Size
		defaultSize := 0  //TODO
		if size == 0 {
			if defaultSize > 0 {
				size = int(defaultSize)
			} else {
				hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
				// TEXT, GEOMETRY or JSON column can't have a default value
				if field.PrimaryKey || field.HasDefaultValue || hasIndex {
					size = 191 // utf8mb4
				}
			}
		}
		
		if size >= 65536 && size <= int(math.Pow(2, 24)) {
			return "mediumtext"
		} else if size > int(math.Pow(2, 24)) || size <= 0 {
			return "longtext"
		}
		return fmt.Sprintf("varchar(%d)", size)
	case schema.Time:
		precision := ""
		
		if field.Precision > 0 {
			precision = fmt.Sprintf("(%d)", field.Precision)
		}
		
		if field.NotNull || field.PrimaryKey {
			return "datetime" + precision
		}
		return "datetime" + precision + " NULL"
	case schema.Bytes:
		if field.Size > 0 && field.Size < 65536 {
			return fmt.Sprintf("varbinary(%d)", field.Size)
		}
		
		if field.Size >= 65536 && field.Size <= int(math.Pow(2, 24)) {
			return "mediumblob"
		}
		
		return "longblob"
	}
	
	return string(field.DataType)
}

func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (d Dialector) QuoteTo(writer clause.Writer, s string) {
	writer.WriteByte('"')
	if strings.Contains(s, ".") {
		for idx, str := range strings.Split(s, ".") {
			if idx > 0 {
				writer.WriteString(".\"")
			}
			writer.WriteString(str)
			writer.WriteByte('"')
		}
	} else {
		writer.WriteString(s)
		writer.WriteByte('"')
	}
}

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

//TODO List all the other clauses
func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	clauseBuilders := map[string]clause.ClauseBuilder{
		"VALUES": func(c clause.Clause, builder clause.Builder) {
			if values, ok := c.Expression.(clause.Values); ok && len(values.Columns) == 0 {
				builder.WriteString("VALUES()")
				return
			}
			c.Build(builder)
		},
	}
	
	return clauseBuilders
}
