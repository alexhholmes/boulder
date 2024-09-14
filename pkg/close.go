package boulder

import (
	"io"
)

type Close func()

var _ io.Closer = (*Close)(nil)

func (c Close) Close() error {
	c()
	return nil
}
