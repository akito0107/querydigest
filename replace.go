package querydigest

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
	"github.com/akito0107/xsqlparser/sqlastutil"
	"github.com/akito0107/xsqlparser/sqltoken"
)

var tokensPool = sync.Pool{
	New: func() interface{} {
		return make([]*sqltoken.Token, 0, 2048)
	},
}

var tokenizerPool = sync.Pool{
	New: func() interface{} {
		return sqltoken.NewTokenizerWithOptions(nil, sqltoken.Dialect(&dialect.MySQLDialect{}), sqltoken.DisableParseComment())
	},
}

func ReplaceWithZeroValue(src []byte) (string, error) {
	tokenizer := tokenizerPool.Get().(*sqltoken.Tokenizer)
	tokenizer.Line = 1
	tokenizer.Col = 1
	tokenizer.Scanner.Init(bytes.NewReader(src))
	defer tokenizerPool.Put(tokenizer)

	tokset := tokensPool.Get().([]*sqltoken.Token)
	tokset = tokset[:0]
	defer func() {
		tokensPool.Put(tokset)
	}()

	for {
		var tok *sqltoken.Token
		if len(tokset) < cap(tokset) {
			tok = tokset[:len(tokset)+1][len(tokset)]
		}
		if tok == nil {
			tok = &sqltoken.Token{}
		}
		t, err := tokenizer.Scan(tok)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("tokenize failed src: %s : %w", string(src), err)
		}
		if t == nil {
			continue
		}
		tokset = append(tokset, tok)
	}

	parser := xsqlparser.NewParserWithOptions()
	parser.SetTokens(tokset)

	stmt, err := parser.ParseStatement()
	if err != nil {
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
				Expr:    node.Expr,
				Negated: node.Negated,
				RParen:  node.RParen,
			})
		}
		return true
	}, nil)
	return res.ToSQLString(), nil
}
