package ruperformance

import (
	"github.com/rs/zerolog/log"
)

var logger = log.With().Logger()

const (
	kilo      = 1024
	kiloBytes = kilo
	megaBytes = kiloBytes * kilo
	gigaBytes = megaBytes * kilo
)
