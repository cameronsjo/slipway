package probe

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/cameronsjo/slipway/internal/githubapp"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// Store is a single-writer foundation journal. It intentionally does not claim
// to implement the production job queue, worker allocator, or platform admission.
type Store struct {
	db   *sql.DB
	lock *os.File
}

type Attempt struct {
	ID             string               `json:"id"`
	InstallationID int64                `json:"installation_id"`
	Repo           githubapp.Repository `json:"repository"`
	Name           string               `json:"scale_set_name"`
	RunnerName     string               `json:"runner_name"`
	SetID          int                  `json:"scale_set_id,omitempty"`
	GroupID        int                  `json:"runner_group_id"`
	RunnerID       int                  `json:"runner_id,omitempty"`
	Stage          string               `json:"stage"`
	Done           bool                 `json:"cleanup_verified"`
	Updated        time.Time            `json:"updated"`
}

func Open(directory string) (*Store, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state directory must be a private directory (0700)")
	}
	openPrivate := func(path string) (*os.File, error) {
		fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		if err != nil {
			return nil, err
		}
		f := os.NewFile(uintptr(fd), path)
		stat, err := f.Stat()
		if err != nil || !stat.Mode().IsRegular() || stat.Mode().Perm()&0077 != 0 {
			f.Close()
			return nil, errors.New("state file must be private and regular")
		}
		return f, nil
	}
	lock, err := openPrivate(filepath.Join(directory, "probe.lock"))
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another probe holds the allocation lock")
	}
	f, err := openPrivate(filepath.Join(directory, "probe.sqlite"))
	if err != nil {
		lock.Close()
		return nil, err
	}
	f.Close()
	abs, err := filepath.Abs(directory)
	if err != nil {
		lock.Close()
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(abs, "probe.sqlite")}
	u.RawQuery = url.Values{"_pragma": []string{"journal_mode(WAL)", "synchronous(FULL)", "busy_timeout(5000)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		lock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, lock: lock}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS discoveries (installation INTEGER PRIMARY KEY, observed TEXT NOT NULL, repositories TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS probes (id TEXT PRIMARY KEY, done INTEGER NOT NULL CHECK(done IN (0,1)), document TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_probe ON probes(done) WHERE done=0;
CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, attempt TEXT NOT NULL, stage TEXT NOT NULL, observed TEXT NOT NULL);`)
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return errors.Join(s.db.Close(), s.lock.Close()) }

func (s *Store) Discovery(installation int64, repos []githubapp.Repository) error {
	data, err := json.Marshal(repos)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO discoveries VALUES(?,?,?) ON CONFLICT(installation) DO UPDATE SET observed=excluded.observed,repositories=excluded.repositories`, installation, time.Now().UTC().Format(time.RFC3339Nano), string(data))
	return err
}

func (s *Store) Pending() (*Attempt, error) {
	var data string
	err := s.db.QueryRow(`SELECT document FROM probes WHERE done=0`).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var attempt Attempt
	if err := json.Unmarshal([]byte(data), &attempt); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(attempt.ID); err != nil || attempt.Name != "slipway-probe-"+attempt.ID || attempt.RunnerName != attempt.Name+"-runner" {
		return nil, errors.New("invalid owned probe identity")
	}
	return &attempt, nil
}

func (s *Store) Begin(installation int64, repo githubapp.Repository) (*Attempt, error) {
	id := uuid.NewString()
	name := "slipway-probe-" + id
	a := &Attempt{ID: id, InstallationID: installation, Repo: repo, Name: name, RunnerName: name + "-runner", GroupID: 1, Stage: "reserved", Updated: time.Now().UTC()}
	data, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	_, err = s.db.Exec(`INSERT INTO probes(id,done,document) VALUES(?,0,?)`, id, string(data))
	if err != nil {
		return nil, fmt.Errorf("reserve one probe slot: %w", err)
	}
	return a, nil
}

// Save commits each intent before its API side effect. Failed cleanup does not
// release the single reservation; the next invocation reconciles this identity.
func (s *Store) Save(a *Attempt, stage string, done bool) error {
	next := *a
	next.Stage = stage
	next.Done = done
	next.Updated = time.Now().UTC()
	data, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`UPDATE probes SET done=?,document=? WHERE id=? AND done=0`, done, string(data), a.ID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return errors.New("attempt missing or reservation already released")
	}
	if _, err = tx.Exec(`INSERT INTO events(attempt,stage,observed) VALUES(?,?,?)`, a.ID, stage, next.Updated.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	*a = next
	return nil
}
