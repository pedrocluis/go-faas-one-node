package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
)

type RAMCache struct {
	occupied         int
	orderedFunctions []*ExecutingFunction
}

type QueueItem struct {
	hashFunction string
	memory       int
	end          int
}

type DiskCache struct {
	available         int
	orderedFunctions  []*ExecutingFunction
	lfuMap            map[int][]*ExecutingFunction
	readQueue         []*QueueItem
	writeQueue        []*QueueItem
	lastRead          int
	lastWrite         int
	readSpeed         float64
	writeSpeed        float64
	coldLatency       int
	policy            string
	restoreLatency    int
	checkpointLatency int
}

type NetCache struct {
	startFunctions map[string]int
	newFunctions   map[string]int
	bandwidth      float64
	readQueue      []*QueueItem
	writeQueue     []*QueueItem
	lastRead       int
	lastWrite      int
	coldLatency    int
	restoreLatency int
}

func createRamCache() *RAMCache {
	cache := new(RAMCache)
	cache.occupied = 0
	cache.orderedFunctions = make([]*ExecutingFunction, 0)

	return cache
}

func addToRamCache(ramCache *RAMCache, function *ExecutingFunction) {

	ramCache.orderedFunctions = append(ramCache.orderedFunctions, function)
	ramCache.occupied += function.memory

}

func updateRamCache(node *Node, threshold float64, capacity int, ms int) {

	ramCache := node.ramCache

	if float64(ramCache.occupied)/float64(capacity) >= threshold {
		i := 0
		for i < len(ramCache.orderedFunctions) {
			if float64(ramCache.occupied)/float64(capacity) < threshold {
				break
			}
			addToWriteQueue(node, ramCache.orderedFunctions[i], ms)
			node.ramMemory += ramCache.orderedFunctions[i].memory
			ramCache.occupied -= ramCache.orderedFunctions[i].memory
			i++
		}
		ramCache.orderedFunctions = ramCache.orderedFunctions[i:]
	}
}

func freeRamCache(node *Node, memory int) bool {

	freed := 0
	i := 0
	for i < len(node.ramCache.orderedFunctions) {
		if freed >= memory {
			break
		}
		freed += node.ramCache.orderedFunctions[i].memory
		node.ramMemory += node.ramCache.orderedFunctions[i].memory
		node.ramCache.occupied -= node.ramCache.orderedFunctions[i].memory
		i++
	}
	node.ramCache.orderedFunctions = node.ramCache.orderedFunctions[i:]

	if freed >= memory {
		return true
	} else {
		return false
	}

}

func tryWarm(node *Node, hashFunction string) bool {
	ramCache := node.ramCache

	for i, element := range ramCache.orderedFunctions {
		if element.hashFunction == hashFunction {
			ramCache.occupied -= element.memory
			ramCache.orderedFunctions = remove(ramCache.orderedFunctions, i)
			return true
		}
	}
	return false
}

func createDiskCache(props *Properties) *DiskCache {
	diskCache := new(DiskCache)
	diskCache.available = props.diskMemory * 1000
	diskCache.orderedFunctions = make([]*ExecutingFunction, 0)
	diskCache.readSpeed = props.readBandwidth
	diskCache.writeSpeed = props.writeBandwidth
	diskCache.readQueue = make([]*QueueItem, 0)
	diskCache.writeQueue = make([]*QueueItem, 0)
	diskCache.lastWrite = 0
	diskCache.lastRead = 0
	diskCache.coldLatency = props.coldLatency
	diskCache.policy = props.policy
	diskCache.restoreLatency = props.restoreLatency
	diskCache.checkpointLatency = props.checkpointLatency

	if props.policy == "lfu" {
		diskCache.lfuMap = make(map[int][]*ExecutingFunction)
		diskCache.lfuMap[0] = make([]*ExecutingFunction, 0)
	}

	return diskCache
}

func updateDiskCache(node *Node, ms int) {

	diskCache := node.diskCache

	i := 0
	for i < len(diskCache.readQueue) {
		invocation := diskCache.readQueue[i]
		if invocation.end > ms {
			break
		}
		i++
	}
	diskCache.readQueue = diskCache.readQueue[i:]

	i = 0
	for i < len(diskCache.writeQueue) {
		invocation := diskCache.writeQueue[i]
		if invocation.end > ms {
			break
		}
		addToDiskCache(node, invocation, ms)
		i++
	}
	diskCache.writeQueue = diskCache.writeQueue[i:]
}

func addToDiskCache(node *Node, item *QueueItem, ms int) {
	diskCache := node.diskCache
	invocation := new(ExecutingFunction)
	invocation.hashFunction = item.hashFunction
	invocation.memory = item.memory

	if diskCache.available-invocation.memory < 0 {
		possible := freeDiskCache(node, invocation.memory, ms)
		if !possible {
			return
		}
	}

	if diskCache.policy == "lru" || diskCache.policy == "fifo" {
		diskCache.orderedFunctions = append(diskCache.orderedFunctions, invocation)
	}

	if diskCache.policy == "lfu" {
		diskCache.lfuMap[0] = append(diskCache.lfuMap[0], invocation)
	}

	diskCache.available -= invocation.memory
}

func freeDiskCache(node *Node, memory int, ms int) bool {

	diskCache := node.diskCache
	netCache := node.netCache

	if diskCache.policy == "lfu" {
		hits := 0
		for {
			items, ok := diskCache.lfuMap[hits]
			if !ok {
				break
			}
			if diskCache.available >= memory {
				break
			}
			i := 0
			for ; i < len(items); i++ {
				if diskCache.available >= memory {
					break
				}
				if netCache != nil && ms != -1 {
					addToWriteNetQueue(node, items[i], ms)
				}
				diskCache.available += items[i].memory
			}
			diskCache.lfuMap[hits] = items[i:]
			hits++
		}
	}

	if diskCache.policy == "fifo" || diskCache.policy == "lru" {
		i := 0
		for i < len(diskCache.orderedFunctions) {
			if diskCache.available >= memory {
				break
			}
			if netCache != nil && ms != -1 {
				addToWriteNetQueue(node, diskCache.orderedFunctions[i], ms)
			}
			diskCache.available += diskCache.orderedFunctions[i].memory
			i++
		}
		diskCache.orderedFunctions = diskCache.orderedFunctions[i:]
	}

	if diskCache.available-memory > 0 {
		return true
	} else {
		return false
	}
}

func tryLukewarm(diskCache *DiskCache, hashFunction string, ms int) int {

	latency := -1

	if diskCache.policy == "lru" || diskCache.policy == "fifo" {
		for i, inv := range diskCache.orderedFunctions {
			if inv.hashFunction == hashFunction {
				if diskCache.policy == "lru" {
					diskCache.orderedFunctions = shiftEnd(diskCache.orderedFunctions, i)
				}
				return addToReadQueue(diskCache, inv, ms)
			}
		}
	}

	if diskCache.policy == "lfu" {
		hits := 0
		for {
			items, ok := diskCache.lfuMap[hits]
			if !ok {
				break
			}
			i := 0
			for ; i < len(items); i++ {
				if items[i].hashFunction == hashFunction {
					latency = addToReadQueue(diskCache, items[i], ms)
					temp := items[i]
					diskCache.lfuMap[hits] = remove(items, i)

					val, exists := diskCache.lfuMap[hits+1]
					if exists {
						diskCache.lfuMap[hits+1] = append(val, temp)
					} else {
						diskCache.lfuMap[hits+1] = make([]*ExecutingFunction, 0)
						diskCache.lfuMap[hits+1] = append(diskCache.lfuMap[hits+1], temp)
					}
					return addToReadQueue(diskCache, temp, ms)
				}
			}
			hits++
		}
	}

	return latency
}

func addToWriteQueue(node *Node, invocation *ExecutingFunction, ms int) {

	diskCache := node.diskCache

	transfer := int(float64(invocation.memory) / diskCache.writeSpeed)

	newElement := new(QueueItem)
	newElement.hashFunction = invocation.hashFunction
	newElement.memory = invocation.memory

	//Check if function is already being written to disk
	for _, el := range diskCache.writeQueue {
		if newElement.hashFunction == el.hashFunction {
			node.ramMemory += invocation.memory
			return
		}
	}
	//Check if function is already in disk
	if diskCache.policy == "lru" || diskCache.policy == "fifo" {
		for _, el := range diskCache.orderedFunctions {
			if newElement.hashFunction == el.hashFunction {
				node.ramMemory += invocation.memory
				return
			}
		}
	}

	if diskCache.policy == "lfu" {
		hits := 0
		for {
			items, ok := diskCache.lfuMap[hits]
			if !ok {
				break
			}
			i := 0
			for ; i < len(items); i++ {
				if items[i].hashFunction == newElement.hashFunction {
					node.ramMemory += invocation.memory
					return
				}
			}
			hits++
		}
	}

	if len(diskCache.writeQueue) == 0 {
		newElement.end = ms + transfer + diskCache.checkpointLatency
	} else {
		newElement.end = transfer + diskCache.lastWrite + diskCache.checkpointLatency
	}

	diskCache.writeQueue = append(diskCache.writeQueue, newElement)
	diskCache.lastWrite = newElement.end
}

func addToReadQueue(diskCache *DiskCache, invocation *ExecutingFunction, ms int) int {
	transfer := int(float64(invocation.memory) / diskCache.readSpeed)

	newElement := new(QueueItem)
	newElement.hashFunction = invocation.hashFunction
	newElement.memory = invocation.memory

	if diskCache.lastRead < ms {
		newElement.end = ms + transfer
	} else {
		newElement.end = transfer + diskCache.lastRead
	}

	latency := newElement.end - ms
	if latency+diskCache.restoreLatency > diskCache.coldLatency {
		//Lukewarm not worth it
		return -1
	} else {
		//Lukewarm worth it
		diskCache.readQueue = append(diskCache.readQueue, newElement)
		diskCache.lastRead = newElement.end
		return latency + diskCache.restoreLatency
	}
}

func freeWriteBuffer(node *Node, memory int) bool {
	freed := 0
	i := 0
	for i < len(node.diskCache.writeQueue) {
		if freed >= memory {
			break
		}
		freed += node.diskCache.writeQueue[i].memory
		node.ramMemory += node.diskCache.writeQueue[i].memory
		i++
	}

	node.diskCache.writeQueue = node.diskCache.writeQueue[i:]

	if freed >= memory {
		return true
	} else {
		return false
	}
}

func createNetCache(props *Properties) *NetCache {
	cache := new(NetCache)
	//Convert from Gbit/s to MB/s
	cache.bandwidth = props.netBandwidth / 8.0
	cache.startFunctions = make(map[string]int)
	cache.newFunctions = make(map[string]int)
	cache.readQueue = make([]*QueueItem, 0)
	cache.writeQueue = make([]*QueueItem, 0)
	cache.lastWrite = 0
	cache.lastRead = 0
	cache.coldLatency = props.coldLatency
	cache.restoreLatency = props.restoreLatency
	return cache
}

func getNextNetCacheItem(reader *csv.Reader) (*QueueItem, bool) {
	rec, err := reader.Read()
	if err == io.EOF {
		return nil, true
	}
	if err != nil {
		log.Fatal(err)
	}

	//Create the object
	function := new(QueueItem)
	function.hashFunction = rec[0]
	function.memory = atoi(rec[1])

	return function, false
}

func addToNetCache(netCache *NetCache, item *QueueItem) {
	netCache.newFunctions[item.hashFunction] = item.memory
}

func importNetCache(node *Node) {
	cacheFile := openFile("cache.txt")
	defer closeFile(cacheFile)
	csvReader := csv.NewReader(cacheFile)

	for {
		function, finished := getNextNetCacheItem(csvReader)
		if finished {
			break
		}
		addToDiskCache(node, function, -1)
		node.netCache.startFunctions[function.hashFunction] = function.memory
	}
}

func exportNetCache(netCache *NetCache) {
	cacheFile, err := os.OpenFile("cache.txt", os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeFile(cacheFile)

	for name, memory := range netCache.newFunctions {
		_, err := fmt.Fprintf(cacheFile, "%s,%d\n", name, memory)
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}

func addToWriteNetQueue(node *Node, invocation *ExecutingFunction, ms int) {

	netCache := node.netCache

	transfer := int(float64(invocation.memory) / netCache.bandwidth)

	newElement := new(QueueItem)
	newElement.hashFunction = invocation.hashFunction
	newElement.memory = invocation.memory

	//Check if function is already being written to net cache
	for _, el := range netCache.writeQueue {
		if newElement.hashFunction == el.hashFunction {
			return
		}
	}
	//Check if function is already in net cache
	_, existsStart := netCache.startFunctions[invocation.hashFunction]
	_, existsNew := netCache.startFunctions[invocation.hashFunction]
	if existsStart || existsNew {
		return
	}

	if len(netCache.writeQueue) == 0 {
		newElement.end = ms + transfer
	} else {
		newElement.end = transfer + netCache.lastWrite
	}

	netCache.writeQueue = append(netCache.writeQueue, newElement)
	netCache.lastWrite = newElement.end
}

func addToReadNetQueue(netCache *NetCache, function string, memory int, ms int) int {
	transfer := int(float64(memory) / netCache.bandwidth)

	newElement := new(QueueItem)
	newElement.hashFunction = function
	newElement.memory = memory

	if netCache.lastRead < ms {
		newElement.end = ms + transfer
	} else {
		newElement.end = transfer + netCache.lastRead
	}

	latency := newElement.end - ms
	if latency+netCache.restoreLatency > netCache.coldLatency {
		//remote not worth it
		return -1
	} else {
		//remote worth it
		netCache.readQueue = append(netCache.readQueue, newElement)
		netCache.lastRead = newElement.end
		return latency + netCache.restoreLatency
	}
}

func updateNetCache(node *Node, ms int) {

	netCache := node.netCache

	i := 0
	for i < len(netCache.readQueue) {
		invocation := netCache.readQueue[i]
		if invocation.end > ms {
			break
		}
		i++
	}
	netCache.readQueue = netCache.readQueue[i:]

	i = 0
	for i < len(netCache.writeQueue) {
		invocation := netCache.writeQueue[i]
		if invocation.end > ms {
			break
		}
		addToNetCache(netCache, invocation)
		i++
	}
	netCache.writeQueue = netCache.writeQueue[i:]
}

func tryRemote(netCache *NetCache, hashFunction string, ms int) int {

	latency := -1
	memStart, existsStart := netCache.startFunctions[hashFunction]
	memNew, existsNew := netCache.newFunctions[hashFunction]
	if existsStart {
		latency = addToReadNetQueue(netCache, hashFunction, memStart, ms)
	}
	if existsNew {
		latency = addToReadNetQueue(netCache, hashFunction, memNew, ms)
	}
	return latency
}
