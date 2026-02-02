package ruperformance

import (
	"github.com/rs/zerolog/log"
)

var logger = log.With().Logger()

const (
	kilo       = 1024
	kiloBytes  = kilo
	megaBytes  = kiloBytes * kilo
	gigaBytes  = megaBytes * kilo
	terraBytes = gigaBytes * kilo
)

const (
	decimalSystem        = 10
	bitSize64            = 64
	maxPercent           = 100
	defaultWarmupTime    = 10
	defaultExecutionTime = 20
)
