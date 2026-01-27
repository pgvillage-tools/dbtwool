package lobperformance

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type SafeRandGenerator struct {
	mu  sync.Mutex
	rng *RandGenerator
}

func NewSafeRandGenerator(rng *RandGenerator) *SafeRandGenerator {
	return &SafeRandGenerator{rng: rng}
}

func (s *SafeRandGenerator) NextRand() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.NextRand()
}

type RandMode string

const (
	RandSequential RandMode = "sequential"
	RandScattered  RandMode = "scattered"
)

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

// Create a new Random number generator. When sequential, all possible numbers are hit at least once.
func NewRandGenerator(min, max int, mode RandMode, seed int64) (*RandGenerator, error) {
	if min > max {
		return nil, fmt.Errorf("min must be <= max")
	}
	if mode != RandSequential && mode != RandScattered {
		return nil, fmt.Errorf("invalid mode %q", mode)
	}

	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	rg := &RandGenerator{
		min:  min,
		max:  max,
		mode: mode,
	}

	switch mode {
	case RandSequential:
		size := max - min + 1
		seq := make([]int, size)
		for i := 0; i < size; i++ {
			seq[i] = min + i
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
