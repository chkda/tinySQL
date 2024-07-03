package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
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
	EXECUTE_DUPLICATE_KEY
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

type Cursor struct {
	table      *Table
	pageNum    uint32
	cellNum    uint32
	endOfTable bool
}

type Nodetype uint8

func getNodeType(node []byte) Nodetype {
	value := *(*uint)(unsafe.Pointer(&node[NODE_TYPE_OFFSET]))
	return Nodetype(value)
}

func setNodeType(node []byte, typ Nodetype) {
	*(*uint8)(unsafe.Pointer(&node[NODE_TYPE_OFFSET])) = uint8(typ)
}

const (
	NODE_INTERNAL Nodetype = iota
	NODE_LEAF
)

const (
	NODE_TYPE_SIZE             = 1
	NODE_TYPE_OFFSET           = 0
	IS_ROOT_SIZE               = 1
	IS_ROOT_OFFSET             = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE        = 4
	PARENT_POINTER_OFFSET      = IS_ROOT_OFFSET + IS_ROOT_SIZE
	COMMON_NODE_HEADER_SIZE    = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
	LEAF_NODE_NUM_CELLS_SIZE   = 4
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
	LEAF_NODE_KEY_SIZE         = 4
	LEAF_NODE_KEY_OFFSET       = 0
	LEAF_NODE_VALUE_OFFSET     = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
)

var (
	LEAF_NODE_VALUE_SIZE        = ROW_SIZE
	LEAF_NODE_CELL_SIZE         = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS   = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS         = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
	LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
	LEAF_NODE_LEFT_SPLIT_COUNT  = LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT
)

func leafNodeNumcells(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LEAF_NODE_NUM_CELLS_OFFSET]))
}

func leafNodeCell(node []byte, cellNum uint32) []byte {
	return node[LEAF_NODE_HEADER_SIZE+cellNum*uint32(LEAF_NODE_CELL_SIZE):]
}

func leafNodeKey(node []byte, cellNum uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&leafNodeCell(node, cellNum)[LEAF_NODE_KEY_OFFSET]))
}

func leafNodeValue(node []byte, cellNum uint32) []byte {
	return leafNodeCell(node, cellNum)[LEAF_NODE_VALUE_OFFSET:]
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) *Cursor {
	node := getPage(table.pager, pageNum)
	numCells := *leafNodeNumcells(node)

	cursor := &Cursor{
		table:   table,
		pageNum: pageNum,
	}

	minIndex := uint32(0)
	onePastMaxIndex := numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := *leafNodeKey(node, index)
		if key == keyAtIndex {
			cursor.cellNum = index
			return cursor
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	cursor.cellNum = minIndex
	return cursor
}

func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) {
	oldNode := getPage(cursor.table.pager, cursor.pageNum)
	newPageNum := getUnusedPageNum(cursor.table.pager)
	newNode := getPage(cursor.table.pager, newPageNum)
	initializeLeafNode(newNode)

	for i := int32(LEAF_NODE_MAX_CELLS); i >= 0; i-- {
		var destinationNode []byte
		if i >= int32(LEAF_NODE_LEFT_SPLIT_COUNT) {
			destinationNode = newNode
		} else {
			destinationNode = oldNode
		}

		indexWithNode := i % int32(LEAF_NODE_LEFT_SPLIT_COUNT)
		destination := leafNodeCell(destinationNode, uint32(indexWithNode))

		if i == int32(cursor.cellNum) {
			serializeRow(value, destination)
		} else if i > int32(cursor.cellNum) {
			copy(destination, leafNodeCell(oldNode, uint32(i-1)))
		} else {
			copy(destination, leafNodeCell(oldNode, uint32(i)))
		}
	}

	*leafNodeNumcells(oldNode) = uint32(LEAF_NODE_LEFT_SPLIT_COUNT)
	*leafNodeNumcells(newNode) = uint32(LEAF_NODE_RIGHT_SPLIT_COUNT)

	if isNodeRoot(oldNode) {
		createNewRoot(cursor.table, newPageNum)
	} else {
		fmt.Println("TODO: Update parent adter split")
		os.Exit(1)
	}
}

func printConstant() {
	fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS:  %d\n", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}

func printLeafNode(node []byte) {
	numCells := *leafNodeNumcells(node)
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		key := *leafNodeKey(node, i)
		fmt.Printf("   -  %d : %d\n", i, key)
	}
}

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Printf(" ")
	}
}

func printTree(pager *Pager, pageNum uint32, indentationLevel uint32) {
	node := getPage(pager, pageNum)
	var numKeys uint32
	var child uint32

	switch getNodeType(node) {
	case NODE_LEAF:
		numKeys = *leafNodeNumcells(node)
		indent(indentationLevel)
		fmt.Printf(" - leaf(size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			indent(indentationLevel + 1)
			fmt.Printf(" -%d\n", *leafNodeKey(node, i))
		}
	case NODE_INTERNAL:
		numKeys = *internalNodeNumKeys(node)
		indent(indentationLevel)
		fmt.Printf(" - internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child = *internalNodeChild(node, i)
			printTree(pager, child, indentationLevel+1)
			indent(indentationLevel + 1)
			fmt.Printf(" - key %d\n", *internalNodeKey(node, i))
		}
		child = *internalNodeRightChild(node)
		printTree(pager, child, indentationLevel+1)
	}
}

func initializeLeafNode(node []byte) {
	setNodeType(node, NODE_LEAF)
	*leafNodeNumcells(node) = 0
}

const (
	INTERNAL_NODE_NUM_KEYS_SIZE      = 4
	INTERNAL_NODE_NUM_KEYS_OFFSET    = COMMON_NODE_HEADER_SIZE
	INTERNAL_NODE_RIGHT_CHILD_SIZE   = 4
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
	INTERNAL_NODE_HEADER_SIZE        = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE
	INTERNAL_NODE_KEY_SIZE           = 4
	INTERNAL_NODE_CHILD_SIZE         = 4
	INTERNAL_NODE_CELL_SIZE          = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
)

func internalNodeNumKeys(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[INTERNAL_NODE_NUM_KEYS_OFFSET]))
}

func internalNodeRightChild(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[INTERNAL_NODE_RIGHT_CHILD_OFFSET]))
}

func internalNodeCellI(node []byte, cellNum uint32) []byte {
	return node[INTERNAL_NODE_HEADER_SIZE+cellNum*INTERNAL_NODE_CELL_SIZE:]
}

func internalNodeChild(node []byte, childNum uint32) *uint32 {
	numKeys := *internalNodeNumKeys(node)
	if childNum > numKeys {
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys)
		os.Exit(1)
	}

	if childNum == numKeys {
		return internalNodeRightChild(node)
	}

	return (*uint32)(unsafe.Pointer(&internalNodeCellI(node, childNum)[0]))
}

func internalNodeKey(node []byte, keyNum uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&internalNodeCellI(node, keyNum)[INTERNAL_NODE_CHILD_SIZE]))
}

func getNodeMaxKey(node []byte) uint32 {
	switch getNodeType(node) {
	case NODE_INTERNAL:
		return *internalNodeKey(node, *internalNodeNumKeys(node)-1)
	case NODE_LEAF:
		return *leafNodeKey(node, *leafNodeNumcells(node)-1)
	}

	return 0
}

func isNodeRoot(node []byte) bool {
	value := *(*uint8)(unsafe.Pointer(&node[IS_ROOT_OFFSET]))
	return value != 0
}

func setNodeRoot(node []byte, isRoot bool) {
	var value uint8
	if isRoot {
		value = 1
	} else {
		value = 0
	}
	*(*uint8)(unsafe.Pointer(&node[IS_ROOT_OFFSET])) = value
}

func initializeInternalNode(node []byte) {
	setNodeType(node, NODE_INTERNAL)
	setNodeRoot(node, false)
	*internalNodeNumKeys(node) = 0
}

func createNewRoot(table *Table, rightChildPageNum uint32) {
	root := getPage(table.pager, table.rootPageNum)
	rightChild := getPage(table.pager, rightChildPageNum)
	leftChildPageNum := getUnusedPageNum(table.pager)
	leftChild := getPage(table.pager, leftChildPageNum)

	if getNodeType(root) == NODE_INTERNAL {
		initializeInternalNode(rightChild)
		initializeInternalNode(leftChild)
	}

	copy(leftChild, root)
	setNodeRoot(leftChild, false)

	initializeInternalNode(root)
	setNodeRoot(root, true)
	*internalNodeNumKeys(root) = 1
	*internalNodeChild(root, 0) = leftChildPageNum
	leftChildMaxKey := getNodeMaxKey(leftChild)
	*internalNodeKey(root, 0) = leftChildMaxKey
	*internalNodeRightChild(root) = rightChildPageNum
}

type Pager struct {
	fileDescriptor int
	fileLength     uint32
	numPages       uint32
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
	numPages := fileLength / PAGE_SIZE
	if fileLength%PAGE_SIZE != 0 {
		numPages++
	}

	pager := &Pager{
		fileDescriptor: fd,
		fileLength:     fileLength,
		numPages:       numPages,
	}

	for i := 0; i < TABLE_MAX_PAGES; i++ {
		pager.pages[i] = nil
	}

	return pager
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
			numPages++
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
		if pageNum >= pager.numPages {
			pager.numPages = pageNum + 1
		}
	}
	return pager.pages[pageNum]
}

func pagerFlush(pager *Pager, pageNum uint32) {
	if pager.pages[pageNum] == nil {
		fmt.Println("Tried to flush null page")
		os.Exit(1)
	}

	offset := int64(pageNum * PAGE_SIZE)
	_, err := syscall.Pwrite(pager.fileDescriptor, pager.pages[pageNum], offset)
	if err != nil {
		fmt.Printf("Error writing: %s\n", err)
		os.Exit(1)
	}
}

func getUnusedPageNum(pager *Pager) uint32 {
	return pager.numPages
}

func dbClose(table *Table) {
	pager := table.pager

	for i := uint32(0); i < pager.numPages; i++ {
		if pager.pages[i] == nil {
			continue
		}
		pagerFlush(pager, i)
		pager.pages[i] = nil
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

	table := &Table{
		pager:       pager,
		rootPageNum: 0,
	}

	if pager.numPages == 0 {
		rootNode := getPage(pager, 0)
		initializeLeafNode(rootNode)
	}

	return table
}

type Table struct {
	rootPageNum uint32
	pager       *Pager
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

func cursorValue(cursor *Cursor) []byte {
	node := getPage(cursor.table.pager, cursor.pageNum)
	return leafNodeValue(node, cursor.cellNum)
}

func cursorAdvance(cursor *Cursor) {
	node := getPage(cursor.table.pager, cursor.pageNum)
	cursor.cellNum++
	if cursor.cellNum >= *leafNodeNumcells(node) {
		cursor.endOfTable = true
	}
}

func tableStart(table *Table) *Cursor {
	cursor := &Cursor{
		table:   table,
		pageNum: table.rootPageNum,
		cellNum: 0,
	}
	rootNode := getPage(table.pager, table.rootPageNum)
	numCells := *leafNodeNumcells(rootNode)
	cursor.endOfTable = (numCells == 0)
	return cursor
}

func tableFind(table *Table, key uint32) *Cursor {
	rootPageNum := table.rootPageNum
	rootNode := getPage(table.pager, rootPageNum)
	if getNodeType(rootNode) == NODE_LEAF {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		return internalNodeFind(table, key, rootPageNum)
	}
	return nil
}

func internalNodeFind(table *Table, key uint32, pageNum uint32) *Cursor {
	node := getPage(table.pager, pageNum)
	numKeys := *internalNodeNumKeys(node)

	minIndex := uint32(0)
	maxIndex := numKeys

	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := *internalNodeKey(node, index)
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	childNum := *internalNodeChild(node, minIndex)
	child := getPage(table.pager, childNum)

	switch getNodeType(child) {
	case NODE_INTERNAL:
		return internalNodeFind(table, key, childNum)
	case NODE_LEAF:
		return leafNodeFind(table, childNum, key)
	}
	return nil
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
	buffer = strings.TrimSpace(buffer)
	inputBuffer.buffer = buffer
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	switch inputBuffer.buffer {
	case ".exit":
		dbClose(table)
		os.Exit(0)
	case ".btree":
		fmt.Println("Tree:")
		printTree(table.pager, 0, 0)
		return META_COMMAND_SUCCESS
	case ".constants":
		fmt.Println("Constants:")
		printConstant()
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

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) {
	node := getPage(cursor.table.pager, cursor.pageNum)
	numCells := *leafNodeNumcells(node)
	if numCells >= uint32(LEAF_NODE_MAX_CELLS) {
		leafNodeSplitAndInsert(cursor, key, value)
		return
	}

	if cursor.cellNum < numCells {
		for i := numCells; i > cursor.cellNum; i-- {
			copy(leafNodeCell(node, i), leafNodeCell(node, i-1))
		}
	}
	*leafNodeNumcells(node)++
	*leafNodeKey(node, cursor.cellNum) = key
	serializeRow(value, leafNodeValue(node, cursor.cellNum))
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	node := getPage(table.pager, table.rootPageNum)
	numCells := *leafNodeNumcells(node)

	rowToInsert := &statement.rowToInsert
	keyToInsert := rowToInsert.id
	cursor := tableFind(table, keyToInsert)

	if cursor.cellNum < numCells {
		keyAtIndex := *leafNodeKey(node, cursor.cellNum)
		if keyAtIndex == keyToInsert {
			return EXECUTE_DUPLICATE_KEY
		}
	}

	leafNodeInsert(cursor, rowToInsert.id, rowToInsert)
	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	cursor := tableStart(table)
	var row Row
	for !cursor.endOfTable {
		deserializeRow(&row, cursorValue(cursor))
		fmt.Printf("(%d %s %s)\n", row.id, row.username, row.email)
		cursorAdvance(cursor)
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
			fmt.Println("Error:Table full")
		case EXECUTE_DUPLICATE_KEY:
			fmt.Println("Error: Duplicate key")
		}
	}
}
