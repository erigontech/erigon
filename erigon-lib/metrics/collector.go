package metrics

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/metrics"
)

var (
	typeGaugeTpl                     = "\n# TYPE %s gauge\n"
	typeCounterTpl                   = "\n# TYPE %s counter\n"
	typeSummaryTpl                   = "\n# TYPE %s summary\n"
	keyValueTpl                      = "%s %v\n"
	keyCounterTpl                    = "%s %v\n"
	keyQuantileTagValueTpl           = "%s {quantile=\"%s\"} %v\n"
	keyQuantileTagValueWithLabelsTpl = "%s,quantile=\"%s\"} %v\n"
)

// collector is a collection of byte buffers that aggregate Prometheus reports
// for different metric types.
type collector struct {
	buff *bytes.Buffer
}

// newCollector creates a new Prometheus metric aggregator.
func newCollector() *collector {
	return &collector{
		buff: &bytes.Buffer{},
	}
}

func (c *collector) writeFloatCounter(name string, m *metrics.FloatCounter, withType bool) {
	c.writeCounter(name, m.Get(), withType)
}

func (c *collector) writeHistogram(name string, m *metrics.Histogram, withType bool) {
	if withType {
		c.buff.WriteString(fmt.Sprintf(typeSummaryTpl, stripLabels(name)))
	}

	c.writeSummarySum(name, fmt.Sprintf("%f", m.GetSum()))
	c.writeSummaryCounter(name, len(m.GetDecimalBuckets()))
}

func (c *collector) writeTimer(name string, m *metrics.Summary, withType bool) {
	pv := m.GetQuantiles()
	ps := m.GetQuantileValues()

	var sum float64 = 0
	if withType {
		c.buff.WriteString(fmt.Sprintf(typeSummaryTpl, stripLabels(name)))
	}

	for i := range pv {
		c.writeSummaryPercentile(name, strconv.FormatFloat(pv[i], 'f', -1, 64), ps[i])
		sum += ps[i]
	}

	c.writeSummaryTime(name, fmt.Sprintf("%f", m.GetTime().Seconds()))
	c.writeSummarySum(name, fmt.Sprintf("%f", sum))
	c.writeSummaryCounter(name, len(ps))
}

func (c *collector) writeGauge(name string, value interface{}, withType bool) {
	if withType {
		c.buff.WriteString(fmt.Sprintf(typeGaugeTpl, stripLabels(name)))
	}
	c.buff.WriteString(fmt.Sprintf(keyValueTpl, name, value))
}

func (c *collector) writeCounter(name string, value interface{}, withType bool) {
	if withType {
		c.buff.WriteString(fmt.Sprintf(typeCounterTpl, stripLabels(name)))
	}
	c.buff.WriteString(fmt.Sprintf(keyValueTpl, name, value))
}

func stripLabels(name string) string {
	if labelsIndex := strings.IndexByte(name, '{'); labelsIndex >= 0 {
		return name[0:labelsIndex]
	}

	return name
}

func splitLabels(name string) (string, string) {
	if labelsIndex := strings.IndexByte(name, '{'); labelsIndex >= 0 {
		return name[0:labelsIndex], name[labelsIndex:]
	}

	return name, ""
}

func (c *collector) writeSummaryCounter(name string, value interface{}) {
	name, labels := splitLabels(name)
	name = name + "_count"
	c.buff.WriteString(fmt.Sprintf(keyCounterTpl, name+labels, value))
}

func (c *collector) writeSummaryPercentile(name, p string, value interface{}) {
	name, labels := splitLabels(name)

	if len(labels) > 0 {
		c.buff.WriteString(fmt.Sprintf(keyQuantileTagValueWithLabelsTpl, name+strings.TrimSuffix(labels, "}"), p, value))
	} else {
		c.buff.WriteString(fmt.Sprintf(keyQuantileTagValueTpl, name, p, value))
	}
}

func (c *collector) writeSummarySum(name string, value string) {
	name, labels := splitLabels(name)
	name = name + "_sum"
	c.buff.WriteString(fmt.Sprintf(keyCounterTpl, name+labels, value))
}

func (c *collector) writeSummaryTime(name string, value string) {
	name, labels := splitLabels(name)
	name = name + "_time"
	c.buff.WriteString(fmt.Sprintf(keyCounterTpl, name+labels, value))
}
