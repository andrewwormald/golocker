package golocker

import "github.com/luno/jettison/errors"

var ErrLeaseHasNotExpired = errors.New("lease has not expired yet")
