package tournament

import (
	"sort"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
)

// bps returns floor(x * b / 10000) without overflowing an int64 for any x
// up to the module's money bounds: the quotient and remainder of x by
// 10000 are scaled separately.
func bps(x ledger.Money, b uint32) ledger.Money {
	q, r := x/10000, x%10000
	return q*ledger.Money(b) + r*ledger.Money(b)/10000
}

// ComputeStandings ranks the entries of t. Scored entries come first,
// ordered by Score descending then SubmitSeq ascending; unscored entries
// follow in JoinSeq order. Place is the 1-based position; under Split,
// scored entries with equal scores share the place of the first of them.
// The result depends only on t.Entries and t.Rules.TieBreak.
func ComputeStandings(t *Tournament) []Standing {
	var scored, unscored []Entry
	for _, e := range t.Entries {
		if e.Scored {
			scored = append(scored, e)
		} else {
			unscored = append(unscored, e)
		}
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score != scored[j].Score {
			return scored[i].Score > scored[j].Score
		}
		return scored[i].SubmitSeq < scored[j].SubmitSeq
	})
	sort.SliceStable(unscored, func(i, j int) bool { return unscored[i].JoinSeq < unscored[j].JoinSeq })
	out := make([]Standing, 0, len(t.Entries))
	for i, e := range scored {
		place := i + 1
		if t.Rules.TieBreak == Split && i > 0 && e.Score == scored[i-1].Score {
			place = out[i-1].Place
		}
		out = append(out, Standing{Place: place, Player: e.Player.ID, Score: e.Score, Scored: true, SubmitSeq: e.SubmitSeq})
	}
	for _, e := range unscored {
		out = append(out, Standing{Place: len(out) + 1, Player: e.Player.ID})
	}
	return out
}

// ComputePool returns the entry fees, the rake and the prize pool of t in
// integer arithmetic. When the tournament voids (fewer scored entrants than
// MinEntrants) the rake is 0 and the pool equals the fees, because
// everything is refunded.
func ComputePool(t *Tournament) (fees, rake, pool ledger.Money) {
	fees = t.Rules.EntryFee * ledger.Money(len(t.Entries))
	if t.Voids() {
		return fees, 0, fees
	}
	rake = bps(fees, t.Rules.RakeBps)
	return fees, rake, fees - rake
}

// ComputePayouts returns the payouts of t under the exclusion list ex. For
// a tournament that voids it is one refund of the entry fee per entry in
// JoinSeq order. Otherwise it is one payout per prize place: shares are
// pool * PrizeBps[i] / 10000 with the rounding remainder added to first
// place; under Split the shares of a group of equal scores are pooled and
// divided equally, the remainder going one unit each to the earliest
// submitters; a payout whose player's jurisdiction is on ex is marked
// Withheld. The amounts always sum to the pool (or to the fees when
// voided).
func ComputePayouts(t *Tournament, ex Exclusions) []Payout {
	tid := string(t.ID)
	if t.Voids() {
		out := make([]Payout, 0, len(t.Entries))
		for _, e := range t.Entries {
			pid := string(e.Player.ID)
			out = append(out, Payout{
				Player: e.Player.ID, Amount: t.Rules.EntryFee,
				ExclusionVersion: ex.Version, Key: ledger.RefundKey(tid, pid),
			})
		}
		return out
	}
	_, _, pool := ComputePool(t)
	standings := t.Standings
	if len(standings) == 0 {
		standings = ComputeStandings(t)
	}
	n := len(t.Rules.PrizeBps)
	if n > len(standings) {
		// Cannot happen when Voids is false (MinEntrants >= len(PrizeBps)
		// scored entrants); guard so the function is total.
		n = len(standings)
	}
	share := make([]ledger.Money, n)
	var sum ledger.Money
	for i := range share {
		share[i] = bps(pool, t.Rules.PrizeBps[i])
		sum += share[i]
	}
	if n > 0 {
		share[0] += pool - sum
	}
	amounts := make([]ledger.Money, n)
	if t.Rules.TieBreak == Split {
		for i := 0; i < n; {
			j := i
			for j+1 < n && standings[j+1].Score == standings[i].Score && standings[j+1].Scored == standings[i].Scored {
				j++
			}
			var total ledger.Money
			for k := i; k <= j; k++ {
				total += share[k]
			}
			cnt := ledger.Money(j - i + 1)
			each, rem := total/cnt, total%cnt
			for k := i; k <= j; k++ {
				amounts[k] = each
				if ledger.Money(k-i) < rem {
					amounts[k]++
				}
			}
			i = j + 1
		}
	} else {
		copy(amounts, share)
	}
	out := make([]Payout, 0, n)
	for i := 0; i < n; i++ {
		s := standings[i]
		pid := string(s.Player)
		p := Payout{Player: s.Player, Place: s.Place, Amount: amounts[i], ExclusionVersion: ex.Version}
		e, _ := t.Entry(s.Player)
		if ex.Contains(e.Player.Jurisdiction) {
			p.Withheld = true
			p.Reason = string(JurisdictionExcluded)
			p.Key = ledger.WithheldKey(tid, pid, s.Place)
		} else {
			p.Key = ledger.PrizeKey(tid, pid, s.Place)
		}
		out = append(out, p)
	}
	return out
}
