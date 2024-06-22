package tinysql

import (
	"fmt"
	"strings"
)

type Location struct {
	Line uint
	Col  uint
}

type Keyword string

const (
	SelectKeyword Keyword = "select"
	FromKeyword   Keyword = "from"
	AsKeyword     Keyword = "as"
	TableKeyword  Keyword = "table"
	CreateKeyword Keyword = "create"
	InsertKeyword Keyword = "insert"
	IntoKeyword   Keyword = "into"
	ValuesKeyword Keyword = "values"
	IntKeyword    Keyword = "int"
	TextKeyword   Keyword = "text"
	DropKeyword   Keyword = "drop"
)

type Symbol string

const (
	SemicolonSymbol        Symbol = ";"
	AsteriskSymbol         Symbol = "*"
	CommaSymbol            Symbol = ","
	LeftParenthesisSymbol  Symbol = "("
	RightParanthesisSymbol Symbol = ")"
)

type TokenKind uint

const (
	KeywordKind TokenKind = iota
	SymbolKind
	IdentifierKind
	StringKind
	NumericKind
)

type Token struct {
	Value string
	Kind  TokenKind
	Loc   Location
}

func (t *Token) Equals(otherToken *Token) bool {
	return t.Kind == otherToken.Kind && t.Value == otherToken.Value
}

type Cursor struct {
	Pointer uint
	Loc     Location
}

func longestMatch(sqlCode string, initialCursor Cursor, options []string) string {
	var value []byte
	var skiplist []int
	var match string

	cursor := initialCursor

	for ; cursor.Pointer < uint(len(sqlCode)); cursor.Pointer++ {
		value = append(value, strings.ToLower(string(sqlCode[cursor.Pointer]))...)

	match:
		for i, option := range options {
			for _, skip := range skiplist {
				if i == skip {
					continue match
				}
			}

			if option == string(value) {
				skiplist = append(skiplist, i)
				if len(option) > len(match) {
					match = option
				}
				continue
			}

			tooLong := len(value) > len(option)
			sharesPrefix := string(value) == option[:cursor.Pointer-initialCursor.Pointer]
			if tooLong || !sharesPrefix {
				skiplist = append(skiplist, i)
			}
		}
		if len(skiplist) == len(options) {
			break
		}
	}
	return match
}

type Lexer func(string, Cursor) (*Token, Cursor, bool)

func lexNumeric(sqlCode string, initalCursor Cursor) (*Token, Cursor, bool) {
	cursor := initalCursor
	periodFound := false
	expMarkerFound := false

	for ; cursor.Pointer < uint(len(sqlCode)); cursor.Pointer++ {
		character := sqlCode[cursor.Pointer]
		cursor.Loc.Col++
		isDigit := character >= '0' && character <= '9'
		isPeriod := character == '.'
		isExpMarker := character == 'e'

		if cursor.Pointer == initalCursor.Pointer {
			if !isDigit && !isPeriod {
				return nil, initalCursor, false
			}
			periodFound = isPeriod
			continue
		}

		if isPeriod {
			if periodFound {
				return nil, initalCursor, false
			}
			periodFound = true
			continue
		}

		if isExpMarker {
			if expMarkerFound {
				return nil, initalCursor, false
			}

			periodFound = true
			expMarkerFound = true

			if cursor.Pointer == uint(len(sqlCode))-1 {
				return nil, initalCursor, false
			}

			nextCharacter := sqlCode[cursor.Pointer+1]
			if nextCharacter == '-' || nextCharacter == '+' {
				cursor.Pointer++
				cursor.Loc.Col++
			}
			continue
		}

		if !isDigit {
			break
		}
	}

	if cursor.Pointer == initalCursor.Pointer {
		return nil, initalCursor, false
	}
	return &Token{
		Value: sqlCode[initalCursor.Pointer:cursor.Pointer],
		Kind:  NumericKind,
		Loc:   initalCursor.Loc,
	}, cursor, true
}

func lexCharacterDelimited(sqlCode string, initalCursor Cursor, delimiter byte) (*Token, Cursor, bool) {
	cursor := initalCursor

	if len(sqlCode[cursor.Pointer:]) == 0 {
		return nil, initalCursor, false
	}

	if sqlCode[cursor.Pointer] != delimiter {
		return nil, initalCursor, false
	}

	cursor.Pointer++
	cursor.Loc.Col++

	var value []byte
	for ; cursor.Pointer < uint(len(sqlCode)); cursor.Pointer++ {
		character := sqlCode[cursor.Pointer]

		if character == delimiter {
			if cursor.Pointer+1 >= uint(len(sqlCode)) || sqlCode[cursor.Pointer+1] != delimiter {
				cursor.Pointer++
				cursor.Loc.Col++
				return &Token{
					Value: string(value),
					Loc:   initalCursor.Loc,
					Kind:  StringKind,
				}, cursor, true
			}
			value = append(value, character)
			cursor.Pointer++
			cursor.Loc.Col++

		}
		value = append(value, character)
		cursor.Pointer++
		cursor.Loc.Col++

	}
	return nil, initalCursor, false
}

func lexString(sqlCode string, initialCursor Cursor) (*Token, Cursor, bool) {
	return lexCharacterDelimited(sqlCode, initialCursor, '/')
}

func lexIdentifiers(sqlCode string, initialCursor Cursor) (*Token, Cursor, bool) {
	if token, newCursor, ok := lexCharacterDelimited(sqlCode, initialCursor, '"'); ok {
		token.Kind = IdentifierKind
		return token, newCursor, true
	}

	cursor := initialCursor
	character := sqlCode[cursor.Pointer]
	isAlphabetical := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
	if !isAlphabetical {
		return nil, initialCursor, false
	}

	cursor.Pointer++
	cursor.Loc.Col++

	value := []byte{character}

	for ; cursor.Pointer < uint(len(sqlCode)); cursor.Pointer++ {
		character = sqlCode[cursor.Pointer]
		isAlphabetical = (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
		isNumeric := (character >= '0' && character <= '9')

		if isAlphabetical || isNumeric || character == '$' || character == '_' {
			value = append(value, character)
			cursor.Pointer++
			cursor.Loc.Col++
			continue
		}
		break
	}

	return &Token{
		Value: string(value),
		Kind:  IdentifierKind,
		Loc:   initialCursor.Loc,
	}, cursor, true
}

func lexSymbol(sqlCode string, initialCursor Cursor) (*Token, Cursor, bool) {
	cursor := initialCursor
	character := sqlCode[cursor.Pointer]

	cursor.Pointer++
	cursor.Loc.Col++

	switch character {
	case '\n':
		cursor.Loc.Line++
		cursor.Loc.Col = 0
		fallthrough
	case '\t':
		fallthrough
	case ' ':
		return nil, cursor, true
	}

	symbols := []Symbol{
		CommaSymbol,
		LeftParenthesisSymbol,
		RightParanthesisSymbol,
		AsteriskSymbol,
		SemicolonSymbol,
	}

	var options []string
	for _, s := range symbols {
		options = append(options, string(s))
	}

	match := longestMatch(sqlCode, initialCursor, options)
	if match == "" {
		return nil, initialCursor, false
	}
	cursor.Pointer = initialCursor.Pointer + uint(len(match))
	cursor.Loc.Col = initialCursor.Loc.Col + uint(len(match))

	return &Token{
		Value: match,
		Loc:   initialCursor.Loc,
		Kind:  SymbolKind,
	}, cursor, true
}

func lexKeyword(sqlCode string, initialCursor Cursor) (*Token, Cursor, bool) {
	cursor := initialCursor

	keywords := []Keyword{
		SelectKeyword,
		FromKeyword,
		AsKeyword,
		TableKeyword,
		CreateKeyword,
		InsertKeyword,
		IntoKeyword,
		ValuesKeyword,
		IntKeyword,
		TextKeyword,
		DropKeyword,
	}

	var options []string
	for _, k := range keywords {
		options = append(options, string(k))
	}

	match := longestMatch(sqlCode, initialCursor, options)
	if match == "" {
		return nil, initialCursor, false
	}
	cursor.Pointer = initialCursor.Pointer + uint(len(match))
	cursor.Loc.Col = initialCursor.Loc.Col + uint(len(match))

	return &Token{
		Value: match,
		Loc:   initialCursor.Loc,
		Kind:  KeywordKind,
	}, cursor, true
}

func Lex(sqlCode string) ([]*Token, error) {
	tokens := []*Token{}
	cursor := Cursor{}

lex:
	for cursor.Pointer < uint(len(sqlCode)) {
		lexers := []Lexer{lexSymbol, lexKeyword, lexNumeric, lexString, lexIdentifiers}
		for _, lexerFunc := range lexers {
			if token, newCursor, ok := lexerFunc(sqlCode, cursor); ok {
				cursor = newCursor

				if token != nil {
					tokens = append(tokens, token)
				}

				continue lex
			}
		}
		hint := ""
		if len(tokens) > 0 {
			hint = " after " + tokens[len(tokens)-1].Value
		}
		return nil, fmt.Errorf("Unable to lex token%s, at %d:%d", hint, cursor.Loc.Line, cursor.Loc.Col)
	}
	return tokens, nil
}
