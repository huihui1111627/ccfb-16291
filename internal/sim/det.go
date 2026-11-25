package sim

import "math"

// Deterministic random streams.
//
// Every draw is keyed by (seed, request id, span sequence, purpose), so the
// same request sees the same latencies in the propagating run and in the
// non-propagating baseline. That makes the two runs a paired comparison
// instead of two independent samples.

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func mix(seed int64, keys ...uint64) uint64 {
	h := splitmix64(uint64(seed) ^ 0x6a09e667f3bcc909)
	for _, k := range keys {
		h = splitmix64(h ^ (k + 0x9e3779b97f4a7c15))
	}
	return h
}

// U returns a uniform value in [0,1).
func U(seed int64, keys ...uint64) float64 {
	return float64(mix(seed, keys...)>>11) / float64(1<<53)
}

// N returns a standard normal value via Box-Muller using two independent
// uniform streams.
func N(seed int64, keys ...uint64) float64 {
	k1 := append(append([]uint64{}, keys...), 0x5f356495)
	k2 := append(append([]uint64{}, keys...), 0x2c1b3c6d)
	u1 := U(seed, k1...)
	if u1 < 1e-12 {
		u1 = 1e-12
	}
	u2 := U(seed, k2...)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

// Sample draws one value from the distribution, in milliseconds, keyed by the
// supplied coordinates.
func (d Dist) Sample(seed int64, keys ...uint64) float64 {
	var v float64
	switch d.Kind {
	case DistUniform:
		v = d.Min + U(seed, keys...)*(d.Max-d.Min)
	case DistLognormal:
		v = math.Exp(math.Log(d.Median) + d.Sigma*N(seed, keys...))
	default:
		v = d.Median
	}
	if d.Min > 0 && v < d.Min {
		v = d.Min
	}
	if d.Max > 0 && v > d.Max {
		v = d.Max
	}
	return v
}
