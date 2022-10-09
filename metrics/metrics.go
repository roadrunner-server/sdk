package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/roadrunner-server/sdk/v3/state/process"
	"github.com/roadrunner-server/sdk/v3/worker/fsm"
)

type Informer interface {
	Workers() []*process.State
}

type StatsExporter struct {
	TotalMemoryDesc  *prometheus.Desc
	StateDesc        *prometheus.Desc
	WorkerMemoryDesc *prometheus.Desc
	TotalWorkersDesc *prometheus.Desc

	WorkersReady   *prometheus.Desc
	WorkersWorking *prometheus.Desc
	WorkersInvalid *prometheus.Desc

	Workers Informer
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
	workerStates := s.Workers.Workers()

	// cumulative RSS memory in bytes
	var cum float64

	var ready float64
	var working float64
	var invalid float64

	// collect the memory
	for i := 0; i < len(workerStates); i++ {
		cum += float64(workerStates[i].MemoryUsage)

		ch <- prometheus.MustNewConstMetric(s.StateDesc, prometheus.GaugeValue, 0, workerStates[i].StatusStr, strconv.Itoa(int(workerStates[i].Pid)))
		ch <- prometheus.MustNewConstMetric(s.WorkerMemoryDesc, prometheus.GaugeValue, float64(workerStates[i].MemoryUsage), strconv.Itoa(int(workerStates[i].Pid)))

		// sync with sdk/worker/state.go
		switch workerStates[i].Status {
		case fsm.StateReady:
			ready++
		case fsm.StateWorking:
			working++
		default:
			invalid++
		}
	}

	ch <- prometheus.MustNewConstMetric(s.WorkersReady, prometheus.GaugeValue, ready)
	ch <- prometheus.MustNewConstMetric(s.WorkersWorking, prometheus.GaugeValue, working)
	ch <- prometheus.MustNewConstMetric(s.WorkersInvalid, prometheus.GaugeValue, invalid)

	// send the values to the prometheus
	ch <- prometheus.MustNewConstMetric(s.TotalWorkersDesc, prometheus.GaugeValue, float64(len(workerStates)))
	ch <- prometheus.MustNewConstMetric(s.TotalMemoryDesc, prometheus.GaugeValue, cum)
}
