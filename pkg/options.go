package pkg

import "boulder/internal/db"

type Option interface {
	apply(*db.DB)
}

type OptionFunc func(*db.DB)

func (f OptionFunc) Apply(db *db.DB) {
	f(db)
}
