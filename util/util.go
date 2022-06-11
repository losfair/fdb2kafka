package util

import (
	"encoding/json"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/pkg/errors"
)

func ParseTuple(raw string) (tuple.Tuple, error) {
	var segments []string
	err := json.Unmarshal([]byte(raw), &segments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse tuple")
	}

	ret := make(tuple.Tuple, len(segments))
	for i, segment := range segments {
		ret[i] = segment
	}

	return ret, nil
}
