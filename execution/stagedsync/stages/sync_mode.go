package stages

type Mode int8 // in which staged sync can run

const (
	ModeUnknown Mode = iota
	ModeBlockProduction
	ModeForkValidation
	ModeApplyingBlocks
)

func (m Mode) String() string {
	switch m {
	case ModeBlockProduction:
		return "ModeBlockProduction"
	case ModeForkValidation:
		return "ModeForkValidation"
	case ModeApplyingBlocks:
		return "ModeApplyingBlocks"
	default:
		return "UnknownMode"
	}
}
