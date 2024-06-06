package main

import (
	"encoding/csv"
	"io"
	"log"
)

type Node struct {
	ramMemory          int
	currentMs          int
	executingFunctions []*ExecutingFunction
	ramCache           *RAMCache
	diskCache          *DiskCache
	netCache           *NetCache
	concInvocations    int
}

type Invocation struct {
	hashOwner    string
	hashFunction string
	memory       int
	duration     int
	latency      int
	timestamp    int
}

type ExecutingFunction struct {
	hashFunction string
	memory       int
	end          int
	nHits        int
}

func getNextInvocation(reader *csv.Reader, isFirstLine *bool) (*Invocation, bool) {
	rec, err := reader.Read()
	if err == io.EOF {
		return nil, true
	}
	if err != nil {
		log.Fatal(err)
	}
	if *(isFirstLine) {
		*(isFirstLine) = false
		return nil, false
	}
	//Create the object
	invocation := new(Invocation)
	invocation.hashOwner = rec[0]
	invocation.hashFunction = rec[1]
	invocation.memory = atoi(rec[2])
	invocation.duration = atoi(rec[3])
	invocation.timestamp = atoi(rec[4])

	return invocation, false
}

func createNode(props *Properties) *Node {
	n := new(Node)
	n.ramMemory = props.ramMemory * 1000
	n.currentMs = 0
	n.executingFunctions = make([]*ExecutingFunction, 0)
	n.ramCache = createRamCache()
	n.diskCache = createDiskCache(props)
	n.concInvocations = 0
	if props.netCache {
		n.netCache = createNetCache(props)
		importNetCache(n)
	} else {
		n.netCache = nil
	}
	return n
}

func executeFunction(node *Node, invocation *Invocation) {
	element := new(ExecutingFunction)
	element.end = node.currentMs + invocation.latency + invocation.duration
	element.hashFunction = invocation.hashFunction
	element.memory = invocation.memory

	for i, el := range node.executingFunctions {
		if element.end < el.end {
			node.executingFunctions = insert(node.executingFunctions, i, element)
			node.concInvocations += 1
			return
		}
	}
	node.executingFunctions = insert(node.executingFunctions, len(node.executingFunctions), element)
	node.concInvocations += 1
}

func updateNode(node *Node, ms int, capacity int, threshold float64) {

	if node.netCache != nil {
		updateNetCache(node, ms)
	}
	updateDiskCache(node, ms)
	updateRamCache(node, threshold, capacity, ms)

	nToRemove := 0

	for _, el := range node.executingFunctions {
		if ms < el.end {
			break
		}
		node.concInvocations -= 1
		addToRamCache(node.ramCache, el)
		nToRemove++
	}

	node.executingFunctions = node.executingFunctions[nToRemove:]
	node.currentMs = ms

}

func reserveMemory(node *Node, memory int) bool {

	if node.ramMemory >= memory {
		return true
	}

	if freeWriteBuffer(node, memory-node.ramMemory) {
		return true
	}

	if freeRamCache(node, memory-node.ramMemory) {
		return true
	}

	return false
}
