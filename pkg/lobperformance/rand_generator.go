package lobperformance

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// SafeRandGenerator is a random adata generator
type SafeRandGenerator struct {
	mu  sync.Mutex
	rng *RandGenerator
}

// NewSafeRandGenerator returns a fershly initialized random generator
func NewSafeRandGenerator(rng *RandGenerator) *SafeRandGenerator {
	return &SafeRandGenerator{rng: rng}
}

// NextRand returns the next random number
func (s *SafeRandGenerator) NextRand() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.NextRand()
}

// RandMode defines the mode for generating random ID's
type RandMode string

const (
	// RandSequential means generating sequential ID's
	RandSequential RandMode = "sequential"
	// RandScattered means generating scattered ID's
	RandScattered RandMode = "scattered"
)

// RandGenerator is a random generator
type RandGenerator struct {
	min  int
	max  int
	mode RandMode

	// scattered mode
	rng *rand.Rand

	// sequential mode
	seq   []int
	index int
}

// NewRandGenerator creates a new Random number generator. When sequential, all possible numbers are hit at least once.
func NewRandGenerator(minValue, maxValue int, mode RandMode, seed int64) (*RandGenerator, error) {
	if minValue > maxValue {
		return nil, errors.New("min must be <= max")
	}
	if mode != RandSequential && mode != RandScattered {
		return nil, fmt.Errorf("invalid mode %q", mode)
	}

	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	rg := &RandGenerator{
		min:  minValue,
		max:  maxValue,
		mode: mode,
	}

	switch mode {
	case RandSequential:
		size := maxValue - minValue + 1
		seq := make([]int, size)
		for i := 0; i < size; i++ {
			seq[i] = minValue + i
		}

		r := rand.New(rand.NewSource(seed))
		for i := size - 1; i > 0; i-- {
			j := r.Intn(i + 1)
			seq[i], seq[j] = seq[j], seq[i]
		}

		rg.seq = seq
		rg.index = 0

	case RandScattered:
		rg.rng = rand.New(rand.NewSource(seed))
	}

	return rg, nil
}

// NextRand returns the next random value
func (rg *RandGenerator) NextRand() int {
	switch rg.mode {
	case RandSequential:
		v := rg.seq[rg.index]
		rg.index++
		if rg.index >= len(rg.seq) {
			rg.index = 0
		}
		return v

	case RandScattered:
		return rg.rng.Intn(rg.max-rg.min+1) + rg.min

	default:
		panic("unsupported RandMode")
	}
}
