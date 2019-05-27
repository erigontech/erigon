package util

import (
	"strconv"
	"time"
	"unicode"
)

var (
	// Time is a namespace for time utility functions.
	Time = timeUtil{}
)

type timeUtil struct{}

func (tu timeUtil) CurrentTimeMillis() int64 {
	return tu.UnixMillis(time.Now().UTC())
}

// FromMillis returns a time from an unix time in millis.
func (tu timeUtil) FromMillis(millis int64) time.Time {
	seconds := time.Duration(millis) / (time.Second / time.Millisecond)
	nanoSeconds := ((time.Duration(millis) * time.Millisecond) - (time.Duration(seconds) * time.Second)) / time.Nanosecond
	return time.Unix(int64(seconds), int64(nanoSeconds)).UTC()
}

func (tu timeUtil) UnixMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

// NoonOn is a shortcut for On(Time(12,0,0), cd) a.k.a. noon on a given date.
func (tu timeUtil) NoonOn(cd time.Time) time.Time {
	return time.Date(cd.Year(), cd.Month(), cd.Day(), 12, 0, 0, 0, cd.Location())
}

// IsWeekDay returns if the day is a monday->friday.
func (tu timeUtil) IsWeekDay(day time.Weekday) bool {
	return !tu.IsWeekendDay(day)
}

// IsWeekendDay returns if the day is a monday->friday.
func (tu timeUtil) IsWeekendDay(day time.Weekday) bool {
	return day == time.Saturday || day == time.Sunday
}

func (tu timeUtil) Millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

const (
	_secondsPerDay = 60 * 60 * 24
)

func (tu timeUtil) DaysDiff(t1, t2 time.Time) (days int64) {
	t1n := t1.Unix()
	t2n := t2.Unix()
	diff := t1n - t2n
	return diff / (_secondsPerDay)
}

// On returns the clock components of clock (hour,minute,second) on the date components of d.
func (tu timeUtil) OnDate(clock, date time.Time) time.Time {
	tzAdjusted := date.In(clock.Location())
	return time.Date(tzAdjusted.Year(), tzAdjusted.Month(), tzAdjusted.Day(), clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), clock.Location())
}

// ExplodeDuration returns all the constituent parts of a time.Duration.
func (tu timeUtil) ExplodeDuration(duration time.Duration) (
	hours time.Duration,
	minutes time.Duration,
	seconds time.Duration,
	milliseconds time.Duration,
	microseconds time.Duration,
) {
	hours = duration / time.Hour
	hoursRemainder := duration - (hours * time.Hour)
	minutes = hoursRemainder / time.Minute
	minuteRemainder := hoursRemainder - (minutes * time.Minute)
	seconds = minuteRemainder / time.Second
	secondsRemainder := minuteRemainder - (seconds * time.Second)
	milliseconds = secondsRemainder / time.Millisecond
	millisecondsRemainder := secondsRemainder - (milliseconds * time.Millisecond)
	microseconds = millisecondsRemainder / time.Microsecond
	return
}

// RoundDuration rounds a duration to the given place.
func (tu timeUtil) RoundDuration(duration, roundTo time.Duration) time.Duration {
	hours, minutes, seconds, milliseconds, microseconds := tu.ExplodeDuration(duration)
	hours = hours * time.Hour
	minutes = minutes * time.Minute
	seconds = seconds * time.Second
	milliseconds = milliseconds * time.Millisecond
	microseconds = microseconds * time.Microsecond

	var total time.Duration
	if hours >= roundTo {
		total = total + hours
	}
	if minutes >= roundTo {
		total = total + minutes
	}
	if seconds >= roundTo {
		total = total + seconds
	}
	if milliseconds >= roundTo {
		total = total + milliseconds
	}
	if microseconds >= roundTo {
		total = total + microseconds
	}

	return total
}

// ParseDuration reverses `FormatDuration`.
func (tu timeUtil) ParseDuration(duration string) time.Duration {
	integerValue, err := strconv.ParseInt(duration, 10, 64)
	if err == nil {
		return time.Duration(integerValue)
	}

	var hours int64
	var minutes int64
	var seconds int64
	var milliseconds int64
	var microseconds int64

	state := 0
	lastIndex := len([]rune(duration)) - 1

	var numberValue string
	var labelValue string

	var consumeValues = func() {
		switch labelValue {
		case "h":
			hours = Parse.Int64(numberValue)
		case "m":
			minutes = Parse.Int64(numberValue)
		case "s":
			seconds = Parse.Int64(numberValue)
		case "ms":
			milliseconds = Parse.Int64(numberValue)
		case "µs":
			microseconds = Parse.Int64(numberValue)
		}
	}

	for index, c := range []rune(duration) {
		switch state {
		case 0:
			if unicode.IsDigit(c) {
				numberValue = numberValue + string(c)
			} else {
				labelValue = string(c)
				if index == lastIndex {
					consumeValues()
				} else {
					state = 1
				}
			}
		case 1:
			if unicode.IsDigit(c) {
				consumeValues()
				numberValue = string(c)
				state = 0
			} else if index == lastIndex {
				labelValue = labelValue + string(c)
				consumeValues()
			} else {
				labelValue = labelValue + string(c)
			}
		}
	}

	return (time.Duration(hours) * time.Hour) +
		(time.Duration(minutes) * time.Minute) +
		(time.Duration(seconds) * time.Second) +
		(time.Duration(milliseconds) * time.Millisecond) +
		(time.Duration(microseconds) * time.Microsecond)
}
