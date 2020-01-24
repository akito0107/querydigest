package querydigest

import (
	"bytes"
	"log"
	"time"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/sqlastutil"

	"github.com/akito0107/querydigest/dialect"
)

func ReplaceWithZeroValue(src []byte) (string, error) {
	parser, err := xsqlparser.NewParser(bytes.NewBuffer(src), &dialect.MySQLDialect{})
	if err != nil {
		return "", err
	}
	stmt, err := parser.ParseStatement()
	if err != nil {
		log.Printf("Parse failed: invalied sql: %s \n", src)
		return "", err
	}

	res := sqlastutil.Apply(stmt, func(cursor *sqlastutil.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlast.LongValue:
			cursor.Replace(sqlast.NewLongValue(0))
		case *sqlast.DoubleValue:
			cursor.Replace(sqlast.NewDoubleValue(0))
		case *sqlast.BooleanValue:
			cursor.Replace(sqlast.NewBooleanValue(true))
		case *sqlast.SingleQuotedString:
			cursor.Replace(sqlast.NewSingleQuotedString(""))
		case *sqlast.TimestampValue:
			cursor.Replace(sqlast.NewTimestampValue(time.Date(1970, 1, 1, 0, 0, 0, 0, nil)))
		case *sqlast.TimeValue:
			cursor.Replace(sqlast.NewTimeValue(time.Date(1970, 1, 1, 0, 0, 0, 0, nil)))
		case *sqlast.DateTimeValue:
			cursor.Replace(sqlast.NewDateTimeValue(time.Date(1970, 1, 1, 0, 0, 0, 0, nil)))
		case *sqlast.InList:
			cursor.Replace(&sqlast.InList{
				Expr: node.Expr,
				Negated: node.Negated,
				RParen: node.RParen,
			})
		}
		return true
	}, nil)

	return res.ToSQLString(), nil
}
