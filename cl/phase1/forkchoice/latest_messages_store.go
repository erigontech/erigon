package forkchoice

import "sync"

// latestMessagesStore tracks each validator's latest attestation vote by
// validator index. The zero LatestMessage means "no vote seen": a real vote
// always carries a nonzero block root.
type latestMessagesStore struct {
	latestMessages []LatestMessage
	mu             sync.RWMutex
}

func newLatestMessagesStore(size int) *latestMessagesStore {
	return &latestMessagesStore{
		latestMessages: make([]LatestMessage, size),
	}
}

func (l *latestMessagesStore) set(idx int, m LatestMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if idx >= len(l.latestMessages) {
		if idx >= cap(l.latestMessages) {
			tmp := make([]LatestMessage, idx+1, ((idx+1)*3)/2)
			copy(tmp, l.latestMessages)
			l.latestMessages = tmp
		}
		l.latestMessages = l.latestMessages[:idx+1]
	}
	l.latestMessages[idx] = m
}

func (l *latestMessagesStore) get(idx int) (LatestMessage, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if idx < 0 || idx >= len(l.latestMessages) {
		return LatestMessage{}, false
	}
	msg := l.latestMessages[idx]
	return msg, msg != (LatestMessage{})
}

func (l *latestMessagesStore) latestMessagesCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.latestMessages)
}
