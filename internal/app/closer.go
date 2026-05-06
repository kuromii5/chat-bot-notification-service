package app

import (
	"context"

	"github.com/sirupsen/logrus"
)

type Closer struct {
	funcs []func(ctx context.Context) error
}

func (c *Closer) Add(f func(ctx context.Context) error) {
	c.funcs = append(c.funcs, f)
}

func (c *Closer) Close(ctx context.Context) {
	for i := len(c.funcs) - 1; i >= 0; i-- {
		if err := c.funcs[i](ctx); err != nil {
			logrus.WithError(err).Error("shutdown error")
		}
	}
}
