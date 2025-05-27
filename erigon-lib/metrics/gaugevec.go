package metrics

import "github.com/prometheus/client_golang/prometheus"

// Since we wrapped prometheus.Gauge into a custom interface, we need to wrap prometheus.GaugeVec into a custom struct
type GaugeVec struct {
	*prometheus.GaugeVec
}

func (gv *GaugeVec) Collect(ch chan<- prometheus.Metric) {
	gv.MetricVec.Collect(ch)
}

func (gv *GaugeVec) CurryWith(labels prometheus.Labels) (*GaugeVec, error) {
	gv2, err := gv.GaugeVec.CurryWith(labels)
	return &GaugeVec{gv2}, err

}

func (gv *GaugeVec) Delete(labels prometheus.Labels) bool {
	return gv.GaugeVec.MetricVec.Delete(labels)
}

func (gv *GaugeVec) DeleteLabelValues(lvs ...string) bool {
	return gv.GaugeVec.MetricVec.DeleteLabelValues(lvs...)
}

func (gv *GaugeVec) DeletePartialMatch(labels prometheus.Labels) int {
	return gv.GaugeVec.MetricVec.DeletePartialMatch(labels)
}

func (gv *GaugeVec) Describe(ch chan<- *prometheus.Desc) {
	gv.GaugeVec.MetricVec.Describe(ch)
}

func (gv *GaugeVec) GetMetricWith(labels prometheus.Labels) (Gauge, error) {
	g, err := gv.GaugeVec.GetMetricWith(labels)
	return &gauge{g}, err
}

func (gv *GaugeVec) GetMetricWithLabelValues(lvs ...string) (Gauge, error) {
	g, err := gv.GaugeVec.GetMetricWithLabelValues(lvs...)
	return &gauge{g}, err
}

func (gv *GaugeVec) MustCurryWith(labels prometheus.Labels) *GaugeVec {
	return &GaugeVec{gv.GaugeVec.MustCurryWith(labels)}
}

func (gv *GaugeVec) Reset() {
	gv.GaugeVec.MetricVec.Reset()
}

func (gv *GaugeVec) With(labels prometheus.Labels) Gauge {
	return &gauge{gv.GaugeVec.With(labels)}
}

func (gv *GaugeVec) WithLabelValues(lvs ...string) Gauge {
	return &gauge{gv.GaugeVec.WithLabelValues(lvs...)}
}
