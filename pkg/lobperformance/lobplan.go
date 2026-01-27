package lobperformance

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SpreadBucket defines a bcket for a spread for LOB data size
type SpreadBucket struct {
	Percent float64 // e.g. 30.0
	Size    int64   // bytes per row in this bucket (0 allowed)
	Raw     string
}

// BuildLOBPlan builds a LOB plan to be used for generating LOB data
func BuildLOBPlan(totalBytes int64, lobType string, buckets []SpreadBucket,
	emptyLobs int64) ([]LOBRowPlan, error) {
	if totalBytes < 0 {
		return nil, errors.New("totalBytes must be >= 0")
	}
	if emptyLobs < 0 {
		return nil, errors.New("emptyLobs must be >= 0")
	}
	if len(buckets) == 0 {
		if totalBytes == 0 && emptyLobs >= 0 {
			plan := make([]LOBRowPlan, 0, emptyLobs)
			for i := int64(0); i < emptyLobs; i++ {
				plan = append(plan, LOBRowPlan{RowIndex: i, LobType: lobType, LobBytes: 0})
			}
			return plan, nil
		}
		return nil, errors.New("at least one --spread is required when totalBytes > 0")
	}

	// Ensure spreads sum to 100% (within tolerance)
	var sumPct float64
	for _, b := range buckets {
		if b.Size <= 0 {
			return nil, fmt.Errorf("spread size must be > 0 (use --empty-lobs), got %d", b.Size)
		}
		sumPct += b.Percent
	}
	const superSmallFloat = 0.0001
	if math.Abs(sumPct-100.0) > superSmallFloat {
		return nil, fmt.Errorf("spreads must sum to 100%%, got %.4f%%", sumPct)
	}

	// Allocate bytes per bucket (rounding fixed on last bucket)
	type alloc struct {
		size       int64
		targetByte int64
		rows       int64
		usedByte   int64
	}
	allocs := make([]alloc, 0, len(buckets))

	var assigned int64
	for i, b := range buckets {
		target := int64(math.Round((b.Percent / 100.0) * float64(totalBytes)))
		if i == len(buckets)-1 {
			target = totalBytes - assigned
		}
		assigned += target

		rows := target / b.Size
		used := rows * b.Size

		allocs = append(allocs, alloc{size: b.Size, targetByte: target, rows: rows, usedByte: used})
	}

	// Distribute remaining bytes by adding rows where possible (fit-only)
	var usedTotal int64
	for _, a := range allocs {
		usedTotal += a.usedByte
	}
	left := totalBytes - usedTotal

	// Prefer smaller sizes first for smoother distribution
	sort.Slice(allocs, func(i, j int) bool { return allocs[i].size < allocs[j].size })

	for left > 0 {
		added := false
		for i := 0; i < len(allocs); i++ {
			if allocs[i].size <= left {
				allocs[i].rows++
				allocs[i].usedByte += allocs[i].size
				left -= allocs[i].size
				added = true
				if left == 0 {
					break
				}
			}
		}
		if !added {
			// Can't fit any size into remainder; stop. (Optionally: create 1 remainder row)
			break
		}
	}

	// Build plan
	plan := []LOBRowPlan{}
	var idx int64
	for _, a := range allocs {
		for i := int64(0); i < a.rows; i++ {
			plan = append(plan, LOBRowPlan{RowIndex: idx, LobType: lobType, LobBytes: a.size})
			idx++
		}
	}

	// Append empty LOB rows
	for i := int64(0); i < emptyLobs; i++ {
		plan = append(plan, LOBRowPlan{RowIndex: idx, LobType: lobType, LobBytes: 0})
		idx++
	}

	return plan, nil
}

// ShuffledIndices is used to shuffle the indexes of the rows to insert data is spread
// randomly in the end.
func ShuffledIndices(n int, seed int64) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}

	var r *rand.Rand
	if seed == 0 {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		r = rand.New(rand.NewSource(seed))
	}

	// https://www.geeksforgeeks.org/dsa/shuffle-a-given-array-using-fisher-yates-shuffle-algorithm/
	for i := n - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		idx[i], idx[j] = idx[j], idx[i]
	}
	return idx
}

var spreadRe = regexp.MustCompile(`^\s*([0-9]+(?:\.[0-9]+)?)\s*%\s*:\s*([a-zA-Z0-9]+)\s*$`)

// ParseSpread parses a spread definition and returns a bucket
func ParseSpread(s string) (SpreadBucket, error) {
	m := spreadRe.FindStringSubmatch(s)
	if m == nil {
		return SpreadBucket{}, fmt.Errorf("invalid spread %q, expected like 30%%:64kb", s)
	}
	p, err := strconv.ParseFloat(m[1], bitSize64)
	if err != nil {
		return SpreadBucket{}, fmt.Errorf("invalid percent in %q: %w", s, err)
	}
	size, err := ParseByteSize(m[2])
	if err != nil {
		return SpreadBucket{}, fmt.Errorf("invalid size in %q: %w", s, err)
	}
	if p <= 0 || p > maxPercent {
		return SpreadBucket{}, fmt.Errorf("percent out of range in %q", s)
	}
	if size <= 0 {
		return SpreadBucket{}, fmt.Errorf("spread size must be > 0 (use --empty-lobs for empty LOB rows), got %q", s)
	}
	return SpreadBucket{Percent: p, Size: size, Raw: s}, nil
}

// ParseByteSize parses a ByteSize and returns an absolute int64
func ParseByteSize(s string) (int64, error) {
	ss := strings.TrimSpace(strings.ToLower(s))
	if ss == "0" {
		return 0, nil
	}

	// split number + suffix
	numPart := ss
	suffix := ""
	for i := 0; i < len(ss); i++ {
		ch := ss[i]
		if (ch < '0' || ch > '9') && ch != '.' {
			numPart = ss[:i]
			suffix = strings.TrimSpace(ss[i:])
			break
		}
	}

	v, err := strconv.ParseFloat(numPart, bitSize64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("cannot parse number from spread size argument %q", s)
	}

	var mult float64
	switch suffix {
	case "", "b":
		mult = 1
	case "k", "kb":
		mult = kilo
	case "m", "mb":
		mult = megaBytes
	case "g", "gb":
		mult = gigaBytes
	case "t", "tb":
		mult = terraBytes
	default:
		return 0, fmt.Errorf("unsopported size suffix %q in %q", suffix, s)
	}

	out := v * mult
	if out > float64(math.MaxInt64) {
		return 0, fmt.Errorf("size too large: %q", s)
	}
	return int64(out + 0.5), nil
}
