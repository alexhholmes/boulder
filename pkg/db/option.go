package db

type Option interface {
	apply(*DB)
}

type OptionFunc func(*DB)

func (f OptionFunc) Apply(db *DB) {
	f(db)
}
