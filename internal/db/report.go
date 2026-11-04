package db

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

type Report struct {
	BackendPID        int32
	Hypopg            bool
	HypotheticalFound int64
	HypotheticalAfter int64
	HiddenFound       int64
	HiddenAfter       int64
	HypopgRecounted   bool
	AdvisoryReleased  int
	SessionTerminated bool
	MayHaveLeft       bool
	ConnectionLost    bool
	CleanupError      error
}

const mayRemain = "hypothetical indexes, hidden indexes or session advisory locks from this call may remain on the gateway's pooled connection"

func (r Report) Notes() []string {
	var notes []string
	switch {
	case r.Hypopg && r.HypopgRecounted:
		notes = append(notes, fmt.Sprintf("hypopg cleanup: %d hypothetical and %d hidden indexes left on this session after reset", r.HypotheticalAfter, r.HiddenAfter))
	case r.HypopgRecounted && r.HypotheticalAfter == 0 && r.HiddenAfter == 0:
		notes = append(notes, fmt.Sprintf("hypopg cleanup: removed %d hypothetical indexes and restored %d hidden indexes created inside database objects", r.HypotheticalFound, r.HiddenFound))
	}
	if r.AdvisoryReleased > 0 {
		notes = append(notes, fmt.Sprintf("released %d session advisory locks taken inside database objects", r.AdvisoryReleased))
	}
	switch {
	case r.SessionTerminated:
		notes = append(notes, fmt.Sprintf("cleanup could not be confirmed, so the server ended its Postgres session (pid %d); nothing from this call stays on the gateway's pooled connection", r.BackendPID))
	case r.MayHaveLeft && r.ConnectionLost:
		notes = append(notes, "the connection was lost before cleanup finished and the session could not be ended from a separate connection: "+mayRemain)
	case r.MayHaveLeft:
		notes = append(notes, fmt.Sprintf("cleanup failed and ending the Postgres session (pid %d) also failed while the connection was open: %s", r.BackendPID, mayRemain))
	case r.CleanupError != nil:
		notes = append(notes, "cleanup error: "+r.CleanupError.Error())
	}
	return notes
}

func isTerminated(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "57P01"
	}
	return false
}
