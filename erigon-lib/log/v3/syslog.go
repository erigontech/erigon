//go:build !windows && !plan9
// +build !windows,!plan9

package log

import (
	"log/syslog"
	"strings"
)

// NewSyslogHandler opens a connection to the system syslog daemon by calling
// syslog.New and writes all records to it.
func NewSyslogHandler(priority syslog.Priority, tag string, fmtr Format) (SyslogHandler, error) {
	wr, err := syslog.New(priority, tag)
	if err != nil {
		return SyslogHandler{}, err
	}
	return sharedSyslog(fmtr, wr), nil
}

// NewSyslogNetHandler opens a connection to a log daemon over the network and writes
// all log records to it.
func NewSyslogNetHandler(net, addr string, priority syslog.Priority, tag string, fmtr Format) (SyslogHandler, error) {
	wr, err := syslog.Dial(net, addr, priority, tag)
	if err != nil {
		return SyslogHandler{}, err
	}
	return sharedSyslog(fmtr, wr), nil
}

type SyslogHandler struct {
	h Handler
}

func (h SyslogHandler) Log(r *Record) error {
	return h.h.Log(r)
}

func (h SyslogHandler) LogLvl() Lvl {
	return h.h.LogLvl()
}

type syslogInnerHandler struct {
	fmtr  Format
	sysWr *syslog.Writer
}

func (h syslogInnerHandler) Log(r *Record) error {
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

func (h syslogInnerHandler) LogLvl() Lvl {
	return LvlTrace
}

func sharedSyslog(fmtr Format, sysWr *syslog.Writer) SyslogHandler {
	h := syslogInnerHandler{fmtr: fmtr, sysWr: sysWr}
	return SyslogHandler{h: NewLazyHandler(&closingHandler{sysWr, h})}
}

func (m muster) SyslogHandler(priority syslog.Priority, tag string, fmtr Format) Handler {
	return must(NewSyslogHandler(priority, tag, fmtr))
}

func (m muster) SyslogNetHandler(net, addr string, priority syslog.Priority, tag string, fmtr Format) Handler {
	return must(NewSyslogNetHandler(net, addr, priority, tag, fmtr))
}
