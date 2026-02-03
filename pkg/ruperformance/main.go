package ruperformance

import (
	"github.com/rs/zerolog/log"
)

var logger = log.With().Logger()

const (
	// stringBufferallocation is the estimated amount of charactars in a row for the stringbuffer.
	stringBufferallocation = 220

	// descriptionLength is the fixed length of generated descriptions.
	descriptionLength = 100

	// hotAccountPercentage defines how many transactions hit hot accounts.
	hotAccountPercentage = 60

	// hotAccountCount is the number of frequently accessed accounts.
	hotAccountCount = 50

	// totalAccountCount is the total simulated account population.
	totalAccountCount = 10000

	// amountRangeHalfCents defines half the symmetric cent range for amounts.
	amountRangeHalfCents = 500_000

	// amountScale converts integer cents to decimal currency.
	amountScale = 100.0

	// amountZeroThreshold prevents printing "-0.00".
	amountZeroThreshold = 0.005
)
