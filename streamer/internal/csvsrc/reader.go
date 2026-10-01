package csvsrc

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Reader iterates over the rows of the configMap-mounted metrics CSV.
//
// Behaviour:
//   - The first line of the file is treated as the header.
//   - Data rows are numbered 0..n-1 (currentLineNum).
//   - On reaching the end of the file the reader resets currentLineNum to 0 and
//     increments the loop counter, then starts iterating again (continuous
//     streaming of the same dataset).
//   - The file is stat'ed on ReloadCheck so a configMap rotation is picked up
//     without restarting the pod.
type Reader struct {
	path string

	mu    sync.Mutex
	rows  []record
	count int // number of data rows
	pos   int // next data row index to return
	loop  int64

	mtime time.Time
}

type record struct {
	raw []string
}

// Open loads the CSV at path and prepares it for iteration.
func Open(path string) (*Reader, error) {
	r := &Reader{path: path}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

// Row is the parsed form of one CSV data line.
type Row struct {
	// Raw holds the CSV record in the same order as Header.
	Raw []string
	// LineNum is the 0-based data row index (currentLineNum).
	LineNum int64
	// Loop is the wrap-around iteration count for this row.
	Loop int64
}

// Header returns the column names of the CSV file.
func (r *Reader) Header() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == 0 {
		return nil
	}
	return r.rows[0].raw
}

// Columns exposes the header order as constants so the event builder can index
// into Row.Raw safely.
const (
	ColTimestamp    = 0
	ColMetricName   = 1
	ColGPUID        = 2
	ColDevice       = 3
	ColUUID         = 4
	ColModelName    = 5
	ColHostname     = 6
	ColContainer    = 7
	ColPod          = 8
	ColNamespace    = 9
	ColValue        = 10
	ColLabelsRaw    = 11
	ExpectedColumns = 12
)

// Next returns the next data row. When the file is exhausted the position
// resets to 0 and Loop is incremented.
func (r *Reader) Next() (Row, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return Row{}, fmt.Errorf("csv %s contains no data rows", r.path)
	}
	if r.pos >= r.count {
		r.pos = 0
		r.loop++
	}
	// rows[0] is the header; data row i lives at rows[i+1].
	row := r.rows[r.pos+1]
	lineNum := r.pos
	r.pos++
	return Row{Raw: row.raw, LineNum: int64(lineNum), Loop: r.loop}, nil
}

// Seek positions the reader at data row offset (0-based) in a fresh loop.
// Used for crash recovery: the offset returned by the MQ's GetOffset is passed
// here so the streamer resumes exactly where it last got an ack.
func (r *Reader) SeekTo(offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if offset < 0 {
		offset = 0
	}
	if offset >= int64(r.count) && r.count > 0 {
		offset = int64(r.count - 1)
	}
	r.pos = int(offset)
	r.loop = 0
}

// LoopCount returns the current loop iteration (0 on first pass).
func (r *Reader) LoopCount() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loop
}

// Count returns the number of data rows currently loaded.
func (r *Reader) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// ReloadCheck stats the underlying file and reloads it if either it is new or
// its modification time changed (configMap rotation). Returns true when a
// reload actually occurred.
func (r *Reader) ReloadCheck() (bool, error) {
	fi, err := os.Stat(r.path)
	if err != nil {
		return false, err
	}

	r.mu.Lock()
	current := r.mtime
	r.mu.Unlock()

	if fi.ModTime().Equal(current) {
		return false, nil
	}
	if err := r.load(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Reader) load() error {
	f, err := os.Open(r.path)
	if err != nil {
		return fmt.Errorf("open csv %s: %w", r.path, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	cr := csv.NewReader(f)
	cr.FieldsPerRecord = -1

	header, err := cr.Read()
	if err != nil {
		return fmt.Errorf("read csv header: %w", err)
	}
	if len(header) != ExpectedColumns {
		return fmt.Errorf("unexpected csv header: got %d columns, expected %d: %v", len(header), ExpectedColumns, header)
	}

	var data []record
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read csv row: %w", err)
		}
		data = append(data, record{raw: rec})
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	data = append([]record{{raw: header}}, data...)
	r.rows = data
	r.count = len(data) - 1
	r.pos = 0
	r.loop = 0
	r.mtime = fi.ModTime()
	return nil
}
