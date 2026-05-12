package stream

import (
	"fmt"
	"time"
)

// LeaderLocator selects how the broker places the stream queue leader
// (stream argument queue-leader-locator).
type LeaderLocator string

const (
	// LeaderLocatorClientLocal places the leader on the node the declaring client is connected to.
	LeaderLocatorClientLocal LeaderLocator = "client-local"

	// LeaderLocatorBalanced prefers nodes with fewer leaders; see RabbitMQ stream/quorum queue docs.
	LeaderLocatorBalanced LeaderLocator = "balanced"

	// LeaderLocatorLeastLeaders is accepted by the broker but treated like LeaderLocatorBalanced.
	//
	// Deprecated: use LeaderLocatorBalanced.
	LeaderLocatorLeastLeaders LeaderLocator = "least-leaders"

	// LeaderLocatorRandom is accepted by the broker but treated like LeaderLocatorBalanced.
	//
	// Deprecated: use LeaderLocatorBalanced.
	LeaderLocatorRandom LeaderLocator = "random"
)

// defaultStreamLeaderLocator is sent when StreamOptions.LeaderLocator is unset.
// It preserves the historical client default.
const defaultStreamLeaderLocator LeaderLocator = "least-leaders"

type StreamOptions struct {
	MaxAge              time.Duration
	MaxLengthBytes      *ByteCapacity
	MaxSegmentSizeBytes *ByteCapacity
	LeaderLocator       LeaderLocator
}

func (s *StreamOptions) SetMaxAge(maxAge time.Duration) *StreamOptions {
	s.MaxAge = maxAge
	return s
}

func (s *StreamOptions) SetMaxLengthBytes(maxLength *ByteCapacity) *StreamOptions {
	s.MaxLengthBytes = maxLength
	return s
}

func (s *StreamOptions) SetMaxSegmentSizeBytes(segmentSize *ByteCapacity) *StreamOptions {
	s.MaxSegmentSizeBytes = segmentSize
	return s
}

func (s *StreamOptions) SetLeaderLocator(loc LeaderLocator) *StreamOptions {
	s.LeaderLocator = loc
	return s
}

func (s StreamOptions) buildParameters() (map[string]string, error) {
	loc := s.LeaderLocator
	if loc == "" {
		loc = defaultStreamLeaderLocator
	}
	res := map[string]string{queueLeaderLocator: string(loc)}

	if s.MaxLengthBytes != nil {
		if s.MaxLengthBytes.error != nil {
			return nil, s.MaxLengthBytes.error
		}

		if s.MaxLengthBytes.bytes > 0 {
			res[maxLengthBytes] = fmt.Sprintf("%d", s.MaxLengthBytes.bytes)
		}
	}

	if s.MaxSegmentSizeBytes != nil {
		if s.MaxSegmentSizeBytes.error != nil {
			return nil, s.MaxSegmentSizeBytes.error
		}

		if s.MaxSegmentSizeBytes.bytes > 0 {
			res[streamMaxSegmentSizeBytes] = fmt.Sprintf("%d", s.MaxSegmentSizeBytes.bytes)
		}
	}

	if s.MaxAge > 0 {
		res[maxAge] = fmt.Sprintf("%.0fs", s.MaxAge.Seconds())
	}
	return res, nil
}

func NewStreamOptions() *StreamOptions {
	return &StreamOptions{}
}
