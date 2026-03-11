package stages

type Mode int8 // in which staged sync can run

const (
	ModeUnknown Mode = iota
	ModeForkValidation
	ModeApplyingBlocks
)

func (m Mode) String() string {
	switch m {
	case ModeForkValidation:
		return "ModeForkValidation"
	case ModeApplyingBlocks:
		return "ModeApplyingBlocks"
	default:
		return "UnknownMode"
	}
}
