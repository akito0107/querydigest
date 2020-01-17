package dialect

import "github.com/akito0107/xsqlparser/dialect"

type MySQLDialect struct {
	dialect.GenericSQLDialect
}

func NewMySQLDialect() *MySQLDialect {
	return &MySQLDialect{}
}

func (m *MySQLDialect) IsDelimitedIdentifierStart(r rune) bool {
	return r == '"' || r == '`'
}
