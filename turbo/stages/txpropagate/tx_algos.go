package txpropagate

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

func (tp *TxPropagate) DeliverTransactions(peerID string, txs []types.Transaction, direct bool) {
	// Keep track of all the propagated transactions
	//if direct {
	//	txReplyInMeter.Mark(int64(len(txs)))
	//} else {
	//	txBroadcastInMeter.Mark(int64(len(txs)))
	//}
	// Push all the transactions into the pool, tracking underpriced ones to avoid
	// re-requesting them and dropping the peer in case of malicious transfers.
	var (
		duplicate   int64
		underpriced int64
		otherreject int64
	)
	errs := tp.txpool.AddRemotes(txs)
	for i, err := range errs {
		if err != nil {
			// Track the transaction hash if the price is too low for us.
			// Avoid re-request this transaction when we receive another
			// announcement.
			if err == core.ErrUnderpriced || err == core.ErrReplaceUnderpriced {
				for tp.underpriced.Cardinality() >= maxTxUnderpricedSetSize {
					tp.underpriced.Pop()
				}
				tp.underpriced.Add(txs[i].Hash())
			}
			// Track a few interesting failure types
			switch err {
			case nil: // Noop, but need to handle to not count these

			case core.ErrAlreadyKnown:
				duplicate++

			case core.ErrUnderpriced, core.ErrReplaceUnderpriced:
				underpriced++

			default:
				otherreject++
			}
		}

		// TODO: remove this transaction from all other maps
		/*
			case delivery := <-f.cleanup:
				// Independent if the delivery was direct or broadcast, remove all
				// traces of the hash from internal trackers
				for _, hash := range delivery.hashes {
					if _, ok := f.waitlist[hash]; ok {
						for peer, txset := range f.waitslots {
							delete(txset, hash)
							if len(txset) == 0 {
								delete(f.waitslots, peer)
							}
						}
						delete(f.waitlist, hash)
						delete(f.waittime, hash)
					} else {
						for peer, txset := range f.announces {
							delete(txset, hash)
							if len(txset) == 0 {
								delete(f.announces, peer)
							}
						}
						delete(f.announced, hash)
						delete(f.alternates, hash)

						// If a transaction currently being fetched from a different
						// origin was delivered (delivery stolen), mark it so the
						// actual delivery won't double schedule it.
						if origin, ok := f.fetching[hash]; ok && (origin != delivery.origin || !delivery.direct) {
							stolen := f.requests[origin].stolen
							if stolen == nil {
								f.requests[origin].stolen = make(map[common.Hash]struct{})
								stolen = f.requests[origin].stolen
							}
							stolen[hash] = struct{}{}
						}
						delete(f.fetching, hash)
					}
				}
				// In case of a direct delivery, also reschedule anything missing
				// from the original query
				if delivery.direct {
					// Mark the reqesting successful (independent of individual status)
					//txRequestDoneMeter.Mark(int64(len(delivery.hashes)))

					// Make sure something was pending, nuke it
					req := f.requests[delivery.origin]
					if req == nil {
						log.Warn("Unexpected transaction delivery", "peer", delivery.origin)
						break
					}
					delete(f.requests, delivery.origin)

					// Anything not delivered should be re-scheduled (with or without
					// this peer, depending on the response cutoff)
					delivered := make(map[common.Hash]struct{})
					for _, hash := range delivery.hashes {
						delivered[hash] = struct{}{}
					}
					cutoff := len(req.hashes) // If nothing is delivered, assume everything is missing, don't retry!!!
					for i, hash := range req.hashes {
						if _, ok := delivered[hash]; ok {
							cutoff = i
						}
					}
					// Reschedule missing hashes from alternates, not-fulfilled from alt+self
					for i, hash := range req.hashes {
						// Skip rescheduling hashes already delivered by someone else
						if req.stolen != nil {
							if _, ok := req.stolen[hash]; ok {
								continue
							}
						}
						if _, ok := delivered[hash]; !ok {
							if i < cutoff {
								delete(f.alternates[hash], delivery.origin)
								delete(f.announces[delivery.origin], hash)
								if len(f.announces[delivery.origin]) == 0 {
									delete(f.announces, delivery.origin)
								}
							}
							if len(f.alternates[hash]) > 0 {
								if _, ok := f.announced[hash]; ok {
									panic(fmt.Sprintf("announced tracker already contains alternate item: %v", f.announced[hash]))
								}
								f.announced[hash] = f.alternates[hash]
							}
						}
						delete(f.alternates, hash)
						delete(f.fetching, hash)
					}
					// Something was delivered, try to rechedule requests
					f.scheduleFetches(timeoutTimer, timeoutTrigger, nil) // Partial delivery may enable others to deliver too
				}
		*/
	}
	//if direct {
	//	txReplyKnownMeter.Mark(duplicate)
	//	txReplyUnderpricedMeter.Mark(underpriced)
	//	txReplyOtherRejectMeter.Mark(otherreject)
	//} else {
	//	txBroadcastKnownMeter.Mark(duplicate)
	//	txBroadcastUnderpricedMeter.Mark(underpriced)
	//	txBroadcastOtherRejectMeter.Mark(otherreject)
	//}
}
func (tp *TxPropagate) DeliverAnnounces(peerID string, hashes []common.Hash) {
	// Skip any transaction announcements that we already know of, or that we've
	// previously marked as cheap and discarded. This check is of course racey,
	// because multiple concurrent notifies will still manage to pass it, but it's
	// still valuable to check here because it runs concurrent  to the internal
	// loop, so anything caught here is time saved internally.
	var (
		duplicate, underpriced int64
	)
	for _, hash := range hashes {
		if tp.txpool.Has(hash) {
			duplicate++
			continue
		}
		if tp.underpriced.Contains(hash) {
			underpriced++
			continue
		}

		tp.deliveredAnnounces[peerID][hash] = struct{}{}
	}
	//txAnnounceKnownMeter.Mark(duplicate)
	//txAnnounceUnderpricedMeter.Mark(underpriced)
}

func (tp *TxPropagate) RequestTransactions(peerID string, hashes []common.Hash) error {
	tp.requests = append(tp.requests, TxsRequest{PeerID: []byte(peerID), Hashes: hashes})
	return nil
}
