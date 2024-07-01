package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

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
	buffer = strings.TrimPrefix(buffer, "\n")
	buffer = strings.TrimSuffix(buffer, "\n")
	inputBuffer.buffer = buffer
}

func main() {
	inputBuffer := newInputBuffer()
	for {
		printPrompt()
		readInput(inputBuffer)
		if inputBuffer.buffer == ".exit" {
			break
		} else {
			fmt.Printf("Unrecognised command: %s\n", inputBuffer.buffer)
		}
	}
}
