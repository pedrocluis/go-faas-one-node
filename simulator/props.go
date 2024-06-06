package main

import "flag"

type Properties struct {
	inputFile         string
	ramMemory         int
	diskMemory        int
	threshold         int
	coldLatency       int
	readBandwidth     float64
	writeBandwidth    float64
	noDisk            bool
	netCache          bool
	netBandwidth      float64
	maxConcurrency    int
	policy            string
	restoreLatency    int
	checkpointLatency int
}

func getProperties() *Properties {
	props := new(Properties)

	flag.IntVar(&props.ramMemory, "ram", 16, "RAM capacity")
	flag.IntVar(&props.diskMemory, "disk", 80, "Disk cache capacity")
	flag.IntVar(&props.coldLatency, "cold_lat", 250, "Cold start latency")
	flag.Float64Var(&props.readBandwidth, "read_speed", 2.9, "Disk read bandwidth")
	flag.Float64Var(&props.writeBandwidth, "write_speed", 2.0, "Disk write bandwidth")
	flag.BoolVar(&props.noDisk, "no_disk", false, "Don't use disk cache")
	flag.IntVar(&props.threshold, "threshold", 50, "Max percent of memory RAM cache can occupy")
	flag.StringVar(&props.inputFile, "input", "", "Input file")
	flag.BoolVar(&props.netCache, "net_cache", false, "Enable net cache")
	flag.Float64Var(&props.netBandwidth, "net_bandwidth", 4, "Net bandwidth (Gbit/s)")
	flag.IntVar(&props.maxConcurrency, "max_concurrent", 1000, "Max concurrency to use")
	flag.StringVar(&props.policy, "policy", "fifo", "Cache removal policy (fifo/lru/lfu)")
	flag.IntVar(&props.restoreLatency, "restore_latency", 100, "Restore checkpoint latency (ms)")
	flag.IntVar(&props.checkpointLatency, "checkpoint_latency", 500, "Checkpoint latency (ms)")

	flag.Parse()
	return props

}
