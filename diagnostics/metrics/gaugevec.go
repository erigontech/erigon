package metrics

import "github.com/prometheus/client_golang/prometheus"

// Since we wrapped prometheus.Gauge into a custom interface, we need to wrap prometheus.GaugeVec into a custom struct
type GaugeVec struct {
	*prometheus.GaugeVec
}

func (gv *GaugeVec) WithLabelValues(lvs ...string) Gauge {
	return &gauge{gv.GaugeVec.WithLabelValues(lvs...)}
}
