package pg

import "github.com/rs/zerolog/log"

var logger = log.With().Logger()

const (
	maxPoolSizeDefault = 33
	minPoolSizeDefault = 1
)
