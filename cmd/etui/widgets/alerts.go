package widgets

import (
	"strings"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
)

// AlertsView displays a scrollable list of recent alerts with color-coded severity.
type AlertsView struct {
	*tview.TextView
}

// NewAlertsView creates the alerts panel.
func NewAlertsView() *AlertsView {
	tv := tview.NewTextView().SetDynamicColors(true).SetScrollable(true)
	tv.SetBorder(true).SetTitle(" Alerts ")
	tv.SetText("[::d]No alerts[-]")
	return &AlertsView{TextView: tv}
}

// UpdateAlerts renders the most recent alerts into the view.
// maxLines controls how many alerts to display (fits the available height).
func (v *AlertsView) UpdateAlerts(alerts []datasource.Alert) {
	if len(alerts) == 0 {
		v.SetText("[::d]No alerts[-]")
		return
	}
	var b strings.Builder
	for i, a := range alerts {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(a.FormattedLine())
	}
	v.SetText(b.String())
	v.ScrollToEnd()
}
