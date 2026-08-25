// Package idgen produces stable, collision-resistant identifiers.
package idgen

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Generator issues sequential identifiers with a time prefix.
type Generator struct {
	seq atomic.Uint64
}

// Next returns a unique identifier for a named entity.
func (g *Generator) Next(prefix string) string {
	n := g.seq.Add(1)
	sum := xxhash.Sum64String(prefix + strconv.FormatUint(n, 10) + strconv.FormatInt(time.Now().UnixNano(), 10))
	return prefix + "-" + strconv.FormatUint(n, 10) + "-" + strconv.FormatUint(sum&0xffff, 16)
}
