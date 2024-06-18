package log

import "testing"

var lvlCases = []struct {
	in  string
	out Lvl
}{
	{"warn", LvlWarn},
	{"eror", LvlError},
	{"error", LvlError},
	{"EROR", LvlError},
	{"ERROR", LvlError},
	{"UNK", -1},
}

func TestLvlFromString(t *testing.T) {
	for _, tt := range lvlCases {
		lvl, err := LvlFromString(tt.in)
		if err != nil {
			if tt.out != -1 {
				t.Errorf("expected level %q but got err %v", tt.out, err)
			}
		} else {
			if lvl != tt.out {
				t.Errorf("LvlFromString(%q): want %q got %q", tt.in, tt.out, lvl)
			}
		}
	}
}
