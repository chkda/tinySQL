package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_UNRECOGNISED_COMMAND
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_UNRECOGNISED_COMMAND
	PREPARE_SYNTAX_ERROR
)

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Statement struct {
	typ         StatementType
	rowToInsert Row
}

const (
	COLUMN_USERNAME_SIZE = 32   // varchar(32)
	COLUMN_EMAIL_SIZE    = 255  // varchar(255)
	PAGE_SIZE            = 4096 // Page size is normally 4Kb for all systems
	TABLE_MAX_PAGES      = 100
)

var (
	ID_SIZE         = 4 // uint32 -> 4 bytes
	USERNAME_SIZE   = COLUMN_USERNAME_SIZE
	EMAIL_SIZE      = COLUMN_EMAIL_SIZE
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Row struct {
	id       uint32
	username string
	email    string
}

type Table struct {
	numRows uint32
	pages   [TABLE_MAX_PAGES][]byte
}

func newTable() *Table {
	return &Table{}
}

func freeTable(table *Table) {
	for i := range table.pages {
		table.pages[i] = nil
	}
}

func serializeRow(source *Row, destination []byte) {
	binary.LittleEndian.PutUint32(destination[ID_OFFSET:], source.id)
	copy(destination[USERNAME_OFFSET:], []byte(source.username))
	copy(destination[EMAIL_OFFSET:], []byte(source.email))
}

func deserializeRow(destination *Row, source []byte) {
	destination.id = binary.LittleEndian.Uint32(source[ID_OFFSET:])
	destination.username = string(source[USERNAME_OFFSET : USERNAME_OFFSET+USERNAME_SIZE])
	destination.email = string(source[EMAIL_OFFSET : EMAIL_OFFSET+EMAIL_SIZE])
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / uint32(ROWS_PER_PAGE)
	page := table.pages[pageNum]
	if page == nil {
		page = make([]byte, PAGE_SIZE)
		table.pages[pageNum] = page
	}
	rowOffset := rowNum % uint32(ROWS_PER_PAGE)
	byteOffset := rowOffset * uint32(ROW_SIZE)
	return page[byteOffset:]
}

type InputBuffer struct {
	buffer       string
	bufferLength int
	inputLength  int
}

func newInputBuffer() *InputBuffer {
	return &InputBuffer{}
}

func printPrompt() {
	fmt.Print("tinySQL >")
}

func readInput(inputBuffer *InputBuffer) {
	reader := bufio.NewReader(os.Stdin)
	buffer, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input")
		os.Exit(1)
	}
	inputBuffer.bufferLength = len(buffer) - 1
	// buffer = strings.TrimPrefix(buffer, "\n")
	// buffer = strings.TrimSuffix(buffer, "\n")
	buffer = strings.TrimSpace(buffer)
	inputBuffer.buffer = buffer
}

func doMetaCommand(inputBuffer *InputBuffer) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		os.Exit(0)
		return META_COMMAND_SUCCESS
	}
	return META_UNRECOGNISED_COMMAND
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.HasPrefix(inputBuffer.buffer, "insert") {
		statement.typ = STATEMENT_INSERT
		_, err := fmt.Sscanf(inputBuffer.buffer, "insert %d %s %s", &statement.rowToInsert.id, &statement.rowToInsert.username, &statement.rowToInsert.email)
		if err != nil {
			return PREPARE_SYNTAX_ERROR
		}
		return PREPARE_SUCCESS
	}
	if strings.HasPrefix(inputBuffer.buffer, "select") {
		statement.typ = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNISED_COMMAND
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.numRows >= uint32(TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL
	}
	serializeRow(&statement.rowToInsert, rowSlot(table, table.numRows))
	table.numRows++
	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	var row Row
	for i := uint32(0); i < table.numRows; i++ {
		deserializeRow(&row, rowSlot(table, i))
		fmt.Printf("(%d %s %s)\n", row.id, row.username, row.email)
	}
	return EXECUTE_SUCCESS
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.typ {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		return executeSelect(statement, table)
	}
	return EXECUTE_SUCCESS
}

func main() {
	table := newTable()
	inputBuffer := newInputBuffer()
	for {
		printPrompt()
		readInput(inputBuffer)
		if strings.HasPrefix(inputBuffer.buffer, ".") {
			switch doMetaCommand(inputBuffer) {
			case META_COMMAND_SUCCESS:
				continue
			case META_UNRECOGNISED_COMMAND:
				fmt.Printf("Unrecognised Command: %s\n", inputBuffer.buffer)
				continue
			}
		}

		statement := &Statement{}
		switch prepareStatement(inputBuffer, statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Printf("Syntax error. couldn't parse statement\n")
			continue
		case PREPARE_UNRECOGNISED_COMMAND:
			fmt.Printf("Unrecognised Command: %s\n", inputBuffer.buffer)
			continue
		}

		switch executeStatement(statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("executed.")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Table full")
		}
	}
}
