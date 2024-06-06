package main

import (
	"fmt"
	"log"
	"os"
)

type Stats struct {
	coldStarts     int
	warmStarts     int
	lukewarmStarts int
	failedStarts   int
	remoteStarts   int
	totalCold      int
	totalWarm      int
	totalLuke      int
	totalFailed    int
	totalRemote    int

	minute        int
	lastTimestamp int

	fileCold   *os.File
	fileWarm   *os.File
	fileLuke   *os.File
	fileFailed *os.File
	fileRemote *os.File

	fileLatency *os.File
}

func createStats() *Stats {
	stats := new(Stats)

	stats.coldStarts = 0
	stats.warmStarts = 0
	stats.lukewarmStarts = 0
	stats.failedStarts = 0
	stats.remoteStarts = 0
	stats.totalCold = 0
	stats.totalWarm = 0
	stats.totalLuke = 0
	stats.totalFailed = 0
	stats.totalRemote = 0

	stats.minute = 0
	stats.lastTimestamp = 0

	stats.fileCold = createFile("stats/cold.txt")
	stats.fileWarm = createFile("stats/warm.txt")
	stats.fileLuke = createFile("stats/lukewarm.txt")
	stats.fileFailed = createFile("stats/failed.txt")
	stats.fileRemote = createFile("stats/remote.txt")

	stats.fileLatency = createFile("stats/latency.txt")

	return stats
}

func closeStatsFiles(stats *Stats) {
	closeFileCheck(stats.fileCold)
	closeFileCheck(stats.fileWarm)
	closeFileCheck(stats.fileLuke)
	closeFileCheck(stats.fileFailed)
	closeFileCheck(stats.fileLatency)
	closeFileCheck(stats.fileRemote)
}

func registerWarm(stats *Stats) {
	stats.warmStarts += 1
	stats.totalWarm += 1
}

func registerCold(stats *Stats) {
	stats.coldStarts += 1
	stats.totalCold += 1
}

func registerLuke(stats *Stats) {
	stats.lukewarmStarts += 1
	stats.totalLuke += 1
}

func registerFailed(stats *Stats) {
	stats.failedStarts += 1
	stats.totalFailed += 1
}

func registerRemote(stats *Stats) {
	stats.remoteStarts += 1
	stats.totalRemote += 1
}

func checkMinute(stats *Stats, ms int) {

	if stats.lastTimestamp == 0 {
		stats.lastTimestamp = ms
		return
	}

	if ms > stats.lastTimestamp+1000*60 {
		saveMinute(stats)
		stats.lastTimestamp = ms
	}
}

func saveLatency(stats *Stats, latency int, startType string) {
	_, err := fmt.Fprintf(stats.fileLatency, "%d,%s\n", latency, startType)
	if err != nil {
		log.Fatal(err)
	}
}

func saveMinute(stats *Stats) {
	writeToFile(stats.fileCold, stats.minute, stats.coldStarts)
	writeToFile(stats.fileWarm, stats.minute, stats.warmStarts)
	writeToFile(stats.fileLuke, stats.minute, stats.lukewarmStarts)
	writeToFile(stats.fileFailed, stats.minute, stats.failedStarts)
	writeToFile(stats.fileRemote, stats.minute, stats.remoteStarts)

	stats.coldStarts = 0
	stats.warmStarts = 0
	stats.lukewarmStarts = 0
	stats.failedStarts = 0
	stats.remoteStarts = 0

	stats.minute += 1
}

func printTotals(stats *Stats) {
	fmt.Printf("Warm: %d\n", stats.totalWarm)
	fmt.Printf("Lukewarm: %d\n", stats.totalLuke)
	fmt.Printf("Cold: %d\n", stats.totalCold)
	fmt.Printf("Failed: %d\n", stats.totalFailed)
	fmt.Printf("Remote: %d\n", stats.totalRemote)
}
