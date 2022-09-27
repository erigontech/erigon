package p2p

type SignedBeaconBlock interface {
	isSignedBeaconBlock()
}

func (s *SignedBeaconBlockAltair) isSignedBeaconBlock() {
}

func (s *SignedBeaconBlockPhase0) isSignedBeaconBlock() {
}

func (s *SignedBeaconBlockBellatrix) isSignedBeaconBlock() {
}

type BeaconBlock interface {
	isBeaconBlock()
}

func (s *BeaconBlockAltair) isBeaconBlock() {
}

func (s *BeaconBlockPhase0) isBeaconBlock() {
}

func (s *BeaconBlockBellatrix) isBeaconBlock() {
}

type BeaconState interface {
	isBeaconState()
}

func (s *BeaconStatePhase0) isBeaconState() {
}

func (s *BeaconStateAltair) isBeaconState() {
}

func (s *BeaconStateBellatrix) isBeaconState() {
}
