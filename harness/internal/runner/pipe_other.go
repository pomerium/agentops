//go:build !linux

package runner

const pipeDrainMax = 1 << 20

func pipeBuffered(int) (int, error) { return pipeDrainMax, nil }
