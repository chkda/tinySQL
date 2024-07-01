package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
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
	PREPARE_NEGATIVE_ID
	PREPARE_SYNTAX_TOO_LONG
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

type Pager struct {
	fileDescriptor int
	fileLength     uint32
	pages          [TABLE_MAX_PAGES][]byte
}

func pagerOpen(filename string) *Pager {
	fd, err := syscall.Open(filename, syscall.O_RDWR|syscall.O_CREAT, 0600)
	if err != nil {
		fmt.Printf("Uanble to open file: %s\n", filename)
		os.Exit(1)
	}
	fileInfo := &syscall.Stat_t{}
	err = syscall.Stat(filename, fileInfo)
	if err != nil {
		fmt.Println(err)
		fmt.Printf("Unable to get file info: %s\n", filename)
		os.Exit(1)
	}
	fileLength := uint32(fileInfo.Size)
	return &Pager{
		fileDescriptor: fd,
		fileLength:     fileLength,
	}
}

func getPage(pager *Pager, pageNum uint32) []byte {
	if pageNum > TABLE_MAX_PAGES {
		fmt.Printf("Page number out of bounds:%d\n", pageNum)
		os.Exit(1)
	}

	if pager.pages[pageNum] == nil {
		page := make([]byte, PAGE_SIZE)
		numPages := pager.fileLength / PAGE_SIZE

		if pager.fileLength%PAGE_SIZE != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			offset := int64(pageNum * PAGE_SIZE)
			_, err := syscall.Pread(pager.fileDescriptor, page, offset)
			if err != nil {
				fmt.Printf("Error reading file: %s\n", err)
				os.Exit(1)
			}
		}

		pager.pages[pageNum] = page
	}
	return pager.pages[pageNum]
}

func pagerFlush(pager *Pager, pageNum uint32, size uint32) {
	if pager.pages[pageNum] == nil {
		fmt.Println("Tried to flush null page")
		os.Exit(1)
	}

	offset := int64(pageNum * PAGE_SIZE)
	_, err := syscall.Pwrite(pager.fileDescriptor, pager.pages[pageNum][:size], offset)
	if err != nil {
		fmt.Printf("Error writing: %s\n", err)
		os.Exit(1)
	}
}

func dbClose(table *Table) {
	pager := table.pager
	numFullPages := table.numRows / uint32(ROWS_PER_PAGE)

	for i := uint32(0); i < numFullPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pagerFlush(pager, i, PAGE_SIZE)
		pager.pages[i] = nil
	}

	numAdditionalRows := table.numRows % uint32(ROWS_PER_PAGE)
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAdditionalRows*uint32(ROW_SIZE))
			pager.pages[pageNum] = nil
		}
	}

	syscall.Close(pager.fileDescriptor)
	for i := uint32(0); i < TABLE_MAX_PAGES; i++ {
		if pager.pages[i] != nil {
			pager.pages[i] = nil
		}
	}
}

func dbOpen(filename string) *Table {
	pager := pagerOpen(filename)
	numRows := pager.fileLength / uint32(ROW_SIZE)

	table := &Table{
		pager:   pager,
		numRows: numRows,
	}
	return table
}

type Table struct {
	numRows uint32
	pager   *Pager
}

// func newTable() *Table {
// 	return &Table{}
// }

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
	page := getPage(table.pager, pageNum)
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

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		dbClose(table)
		os.Exit(0)
		return META_COMMAND_SUCCESS
	}
	return META_UNRECOGNISED_COMMAND
}

func prepareInsert(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	parts := strings.Fields(inputBuffer.buffer)
	if len(parts) < 4 {
		return PREPARE_SYNTAX_ERROR
	}

	if len(parts) > 4 {
		return PREPARE_SYNTAX_TOO_LONG
	}

	id, err := strconv.Atoi(parts[1])
	if err != nil || id < 0 {
		return PREPARE_NEGATIVE_ID
	}

	username := parts[2]
	email := parts[3]

	if len(username) > COLUMN_USERNAME_SIZE {
		return PREPARE_SYNTAX_TOO_LONG
	}

	if len(email) > COLUMN_EMAIL_SIZE {
		return PREPARE_SYNTAX_TOO_LONG
	}

	statement.typ = STATEMENT_INSERT
	statement.rowToInsert.id = uint32(id)
	statement.rowToInsert.username = username
	statement.rowToInsert.email = email

	return PREPARE_SUCCESS
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.HasPrefix(inputBuffer.buffer, "insert") {
		return prepareInsert(inputBuffer, statement)
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
	if len(os.Args) < 2 {
		fmt.Println("Must supply a database file name")
		os.Exit(1)
	}
	filename := os.Args[1]
	table := dbOpen(filename)
	inputBuffer := newInputBuffer()
	for {
		printPrompt()
		readInput(inputBuffer)
		if strings.HasPrefix(inputBuffer.buffer, ".") {
			switch doMetaCommand(inputBuffer, table) {
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
		case PREPARE_SYNTAX_TOO_LONG:
			fmt.Printf("Syntax error. syntax too long\n")
			continue
		case PREPARE_NEGATIVE_ID:
			fmt.Printf("Syntax error. illegal id\n")
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
