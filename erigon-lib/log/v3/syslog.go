//go:build !windows && !plan9
// +build !windows,!plan9

package log

import (
	"context"
	"log/syslog"
	"strings"
)

// SyslogHandler opens a connection to the system syslog daemon by calling
// syslog.New and writes all records to it.
func SyslogHandler(priority syslog.Priority, tag string, fmtr Format) (Handler, error) {
	wr, err := syslog.New(priority, tag)
	if err != nil {
		return nil, err
	}
	return sharedSyslog(fmtr, wr), nil
}

// SyslogNetHandler opens a connection to a log daemon over the network and writes
// all log records to it.
func SyslogNetHandler(net, addr string, priority syslog.Priority, tag string, fmtr Format) (Handler, error) {
	wr, err := syslog.Dial(net, addr, priority, tag)
	if err != nil {
		return nil, err
	}
	return sharedSyslog(fmtr, wr), nil
}

type syslogHandler struct {
	fmtr  Format
	sysWr *syslog.Writer
}

func (h syslogHandler) Log(r *Record) error {
	var syslogFn func(string) error
	switch r.Lvl {
	case LvlCrit:
		syslogFn = h.sysWr.Crit
	case LvlError:
		syslogFn = h.sysWr.Err
	case LvlWarn:
		syslogFn = h.sysWr.Warning
	case LvlInfo:
		syslogFn = h.sysWr.Info
	case LvlDebug:
		syslogFn = h.sysWr.Debug
	default:
		syslogFn = h.sysWr.Info
	}

	s := strings.TrimSpace(string(h.fmtr.Format(r)))
	return syslogFn(s)
}

func (h syslogHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return true
}

func sharedSyslog(fmtr Format, sysWr *syslog.Writer) Handler {
	h := syslogHandler{fmtr: fmtr, sysWr: sysWr}
	return LazyHandler(&closingHandler{sysWr, h})
}

func (m muster) SyslogHandler(priority syslog.Priority, tag string, fmtr Format) Handler {
	return must(SyslogHandler(priority, tag, fmtr))
}

func (m muster) SyslogNetHandler(net, addr string, priority syslog.Priority, tag string, fmtr Format) Handler {
	return must(SyslogNetHandler(net, addr, priority, tag, fmtr))
}
