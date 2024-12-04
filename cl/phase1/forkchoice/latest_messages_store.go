package forkchoice

import "sync"

// latestMessagesStore keeps in memory an inverted index
type latestMessagesStore struct {
	messageByIdx    map[uint16]LatestMessage
	idxByMessage    map[LatestMessage]uint16
	countOfMessages map[LatestMessage]int
	latestMessages  []uint16
	currID          uint16 // can overflow by design
	mu              sync.RWMutex
}

func newLatestMessagesStore(size int) *latestMessagesStore {
	return &latestMessagesStore{
		messageByIdx:    map[uint16]LatestMessage{},
		idxByMessage:    map[LatestMessage]uint16{},
		countOfMessages: map[LatestMessage]int{},
		latestMessages:  make([]uint16, size, (size*3)/2),
	}
}

func (l *latestMessagesStore) set(idx int, m LatestMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, isPresent := l.idxByMessage[m]
	if !isPresent {
		l.currID++
		l.idxByMessage[m] = l.currID
		l.messageByIdx[l.currID] = m
	}
	messageID := l.idxByMessage[m]
	if idx >= len(l.latestMessages) {
		if idx >= cap(l.latestMessages) {
			l.latestMessages = make([]uint16, idx+1, ((idx+1)*3)/2)
		}
		l.latestMessages = l.latestMessages[:idx+1]
	}

	prev := l.messageByIdx[l.latestMessages[idx]]
	if prev != (LatestMessage{}) && l.countOfMessages[prev] > 0 {
		l.countOfMessages[prev]--
	}
	l.latestMessages[idx] = messageID
	l.countOfMessages[m]++

	l.doPrune()
}

func (l *latestMessagesStore) doPrune() {
	pruneMessages := []LatestMessage{}
	// try to clean up old stuff
	for message, count := range l.countOfMessages {
		if count == 0 {
			pruneMessages = append(pruneMessages, message)
		}
	}
	for _, msg := range pruneMessages {
		idx := l.idxByMessage[msg]
		delete(l.messageByIdx, idx)
		delete(l.idxByMessage, msg)
		delete(l.countOfMessages, msg)
	}
}

func (l *latestMessagesStore) get(idx int) (LatestMessage, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if idx >= len(l.latestMessages) {
		return LatestMessage{}, false
	}
	msg, ok := l.messageByIdx[l.latestMessages[idx]]
	return msg, ok
}

func (l *latestMessagesStore) latestMessagesCount() int {
	return len(l.latestMessages)
}
