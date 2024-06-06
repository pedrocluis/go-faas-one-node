package main

import (
	"encoding/csv"
	"fmt"
)

func main() {

	//Get command line flags
	props := getProperties()

	//Start main loop
	mainLoop(props)

}

func mainLoop(props *Properties) {

	//Open the input file
	f := openFile(props.inputFile)
	//When the function ends close the file
	defer closeFile(f)

	//Initialize file reader
	csvReader := csv.NewReader(f)
	isFirstLine := true

	//Create the node
	node := createNode(props)

	firstTime := -1
	lastTime := -1

	//Initialize stats
	stats := createStats()
	//When the function ends close the stat files
	defer closeStatsFiles(stats)

	for {
		invocation, eof := getNextInvocation(csvReader, &isFirstLine)

		if invocation == nil {
			if eof {
				if node.netCache != nil {
					freeDiskCache(node, 9999999999999, 0)
					updateNetCache(node, 9999999999999)
					exportNetCache(node.netCache)
				}
				break
			}
			continue
		}
		if firstTime == -1 {
			firstTime = invocation.timestamp
		}

		/*if strings.Contains(invocation.hashFunction, "5315") {
			continue
		}*/

		if node.concInvocations >= props.maxConcurrency {
			invocation.timestamp = node.executingFunctions[0].end + 1
		} else {
			if invocation.timestamp < node.currentMs {
				invocation.timestamp = node.currentMs
			}
		}
		lastTime = invocation.timestamp

		updateNode(node, invocation.timestamp, props.ramMemory*1000, float64(props.threshold)*0.01)
		allocateInvocation(node, invocation, stats)
	}

	printTotals(stats)
	timeTook := (lastTime - firstTime) / 1000 / 60
	fmt.Printf("Minutes: %d\n", timeTook)

}

func allocateInvocation(node *Node, invocation *Invocation, stats *Stats) {

	checkMinute(stats, invocation.timestamp)

	if tryWarm(node, invocation.hashFunction) {
		invocation.latency = 0
		registerWarm(stats)
		saveLatency(stats, invocation.latency, "w")
		executeFunction(node, invocation)
		return
	}

	if !reserveMemory(node, invocation.memory) {
		registerFailed(stats)
		println("Failed invocation")
		return
	}

	node.ramMemory -= invocation.memory

	lukewarmLat := -1
	remoteLat := -1
	if node.netCache != nil {
		if float64(node.netCache.lastRead)+(float64(invocation.memory)/node.netCache.bandwidth) < float64(node.diskCache.lastRead)+(float64(invocation.memory)/node.diskCache.readSpeed) {
			remoteLat = tryRemote(node.netCache, invocation.hashFunction, invocation.timestamp)
			if remoteLat == -1 {
				lukewarmLat = tryLukewarm(node.diskCache, invocation.hashFunction, invocation.timestamp)
			}
		} else {
			lukewarmLat = tryLukewarm(node.diskCache, invocation.hashFunction, invocation.timestamp)
			if lukewarmLat == -1 {
				remoteLat = tryRemote(node.netCache, invocation.hashFunction, invocation.timestamp)
			}
		}
	} else {
		lukewarmLat = tryLukewarm(node.diskCache, invocation.hashFunction, invocation.timestamp)
	}

	if lukewarmLat > -1 {
		registerLuke(stats)
		invocation.latency = lukewarmLat
		saveLatency(stats, invocation.latency, "l")
	} else {
		if remoteLat > -1 {
			registerRemote(stats)
			invocation.latency = remoteLat
			saveLatency(stats, invocation.latency, "r")
		} else {
			registerCold(stats)
			invocation.latency = node.diskCache.coldLatency
			saveLatency(stats, invocation.latency, "c")
		}
	}

	executeFunction(node, invocation)
}
