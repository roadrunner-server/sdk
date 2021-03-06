package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/roadrunner-server/api/v2/plugins/informer"
)

const (
	readySt   string = "ready"
	workingSt string = "working"
)

type StatsExporter struct {
	TotalMemoryDesc  *prometheus.Desc
	StateDesc        *prometheus.Desc
	WorkerMemoryDesc *prometheus.Desc
	TotalWorkersDesc *prometheus.Desc

	WorkersReady   *prometheus.Desc
	WorkersWorking *prometheus.Desc
	WorkersInvalid *prometheus.Desc

	Workers informer.Informer
}

func (s *StatsExporter) Describe(d chan<- *prometheus.Desc) {
	// send description
	d <- s.TotalWorkersDesc
	d <- s.TotalMemoryDesc
	d <- s.StateDesc
	d <- s.WorkerMemoryDesc

	d <- s.WorkersReady
	d <- s.WorkersWorking
	d <- s.WorkersInvalid
}

func (s *StatsExporter) Collect(ch chan<- prometheus.Metric) {
	// get the copy of the processes
	workers := s.Workers.Workers()

	// cumulative RSS memory in bytes
	var cum float64

	var ready float64
	var working float64
	var invalid float64

	// collect the memory
	for i := 0; i < len(workers); i++ {
		cum += float64(workers[i].MemoryUsage)

		ch <- prometheus.MustNewConstMetric(s.StateDesc, prometheus.GaugeValue, 0, workers[i].Status, strconv.Itoa(workers[i].Pid))
		ch <- prometheus.MustNewConstMetric(s.WorkerMemoryDesc, prometheus.GaugeValue, float64(workers[i].MemoryUsage), strconv.Itoa(workers[i].Pid))

		// sync with sdk/worker/state.go
		switch workers[i].Status {
		case readySt:
			ready++
		case workingSt:
			working++
		default:
			invalid++
		}
	}

	ch <- prometheus.MustNewConstMetric(s.WorkersReady, prometheus.GaugeValue, ready)
	ch <- prometheus.MustNewConstMetric(s.WorkersWorking, prometheus.GaugeValue, working)
	ch <- prometheus.MustNewConstMetric(s.WorkersInvalid, prometheus.GaugeValue, invalid)

	// send the values to the prometheus
	ch <- prometheus.MustNewConstMetric(s.TotalWorkersDesc, prometheus.GaugeValue, float64(len(workers)))
	ch <- prometheus.MustNewConstMetric(s.TotalMemoryDesc, prometheus.GaugeValue, cum)
}
