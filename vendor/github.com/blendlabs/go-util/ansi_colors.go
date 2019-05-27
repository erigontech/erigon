package util

// AnsiColorCode represents an ansi color code fragment.
type AnsiColorCode string

func (acc AnsiColorCode) escaped() string {
	return "\033[" + string(acc)
}

// Apply returns a string with the color code applied.
func (acc AnsiColorCode) Apply(text string) string {
	return acc.escaped() + text + ColorReset.escaped()
}

const (
	// ColorBlack is the posix escape code fragment for black.
	ColorBlack AnsiColorCode = "30m"

	// ColorRed is the posix escape code fragment for red.
	ColorRed AnsiColorCode = "31m"

	// ColorGreen is the posix escape code fragment for green.
	ColorGreen AnsiColorCode = "32m"

	// ColorYellow is the posix escape code fragment for yellow.
	ColorYellow AnsiColorCode = "33m"

	// ColorBlue is the posix escape code fragment for blue.
	ColorBlue AnsiColorCode = "34m"

	// ColorPurple is the posix escape code fragement for magenta (purple)
	ColorPurple AnsiColorCode = "35m"

	// ColorCyan is the posix escape code fragement for cyan.
	ColorCyan AnsiColorCode = "36m"

	// ColorWhite is the posix escape code fragment for white.
	ColorWhite AnsiColorCode = "37m"

	// ColorLightBlack is the posix escape code fragment for black.
	ColorLightBlack AnsiColorCode = "90m"

	// ColorLightRed is the posix escape code fragment for red.
	ColorLightRed AnsiColorCode = "91m"

	// ColorLightGreen is the posix escape code fragment for green.
	ColorLightGreen AnsiColorCode = "92m"

	// ColorLightYellow is the posix escape code fragment for yellow.
	ColorLightYellow AnsiColorCode = "93m"

	// ColorLightBlue is the posix escape code fragment for blue.
	ColorLightBlue AnsiColorCode = "94m"

	// ColorLightPurple is the posix escape code fragement for magenta (purple)
	ColorLightPurple AnsiColorCode = "95m"

	// ColorLightCyan is the posix escape code fragement for cyan.
	ColorLightCyan AnsiColorCode = "96m"

	// ColorLightWhite is the posix escape code fragment for white.
	ColorLightWhite AnsiColorCode = "97m"

	// ColorGray is an alias to ColorLightWhite to preserve backwards compatibility.
	ColorGray AnsiColorCode = ColorLightBlack

	// ColorReset is the posix escape code fragment to reset all formatting.
	ColorReset AnsiColorCode = "0m"
)
