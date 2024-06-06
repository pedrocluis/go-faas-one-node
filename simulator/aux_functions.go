package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func openFile(filename string) *os.File {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func closeFile(f *os.File) {
	err := f.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// Atoi function with error checking
func atoi(s string) int {
	ret, err := strconv.Atoi(s)
	if err != nil {
		log.Fatal(err)
	}
	return ret
}

func insert(a []*ExecutingFunction, index int, value *ExecutingFunction) []*ExecutingFunction {
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
}

func remove(slice []*ExecutingFunction, s int) []*ExecutingFunction {
	return append(slice[:s], slice[s+1:]...)
}

func createFile(fileName string) *os.File {

	if _, err := os.Stat(fileName); err == nil {
		if err1 := os.Remove(fileName); err1 != nil {
			log.Fatal(err1)
		}
	}

	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	return f
}

func closeFileCheck(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func writeToFile(file *os.File, minute int, n int) {
	_, err := fmt.Fprintf(file, "%d,%d\n", minute, n)
	if err != nil {
		log.Fatal(err)
	}
}

func shiftEnd[T any](s []T, x int) []T {
	if x < 0 {
		return s
	}
	if x >= len(s)-1 {
		return s
	}
	tmp := s[x]
	// No allocation since the new slice fits capacity
	s = append(s[:x], s[x+1:]...)
	// append to the end
	// no allocation, the new slice fits the capacity
	s = append(s, tmp)
	return s
}
