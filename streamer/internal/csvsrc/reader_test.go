package csvsrc

import (
	"os"
	"path/filepath"
	"testing"
)

func openTestReader(t *testing.T) *Reader {
	t.Helper()
	r, err := Open(filepath.Join("testdata", "metrics.csv"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return r
}

func TestOpenHeader(t *testing.T) {
	r := openTestReader(t)
	h := r.Header()
	if len(h) != ExpectedColumns {
		t.Fatalf("header columns = %d, want %d", len(h), ExpectedColumns)
	}
	if h[ColMetricName] != "metric_name" {
		t.Errorf("header[%d] = %q, want metric_name", ColMetricName, h[ColMetricName])
	}
	if got := r.Count(); got != 4 {
		t.Errorf("Count = %d, want 4", got)
	}
}

func TestNextIteratesThenWraps(t *testing.T) {
	r := openTestReader(t)

	for i := 0; i < 4; i++ {
		row, err := r.Next()
		if err != nil {
			t.Fatalf("Next(%d): %v", i, err)
		}
		if got := row.LineNum; got != int64(i) {
			t.Errorf("iteration %d: LineNum = %d, want %d", i, got, i)
		}
		// the 4th row has a non-numeric value; still served raw
		if i == 3 && row.Raw[ColValue] != "not-a-number" {
			t.Errorf("row 3 value = %q, want not-a-number", row.Raw[ColValue])
		}
	}

	// File exhausted: wrap back to offset 0 and bump the loop counter.
	row, err := r.Next()
	if err != nil {
		t.Fatalf("Next after wrap: %v", err)
	}
	if row.LineNum != 0 || row.Loop != 1 {
		t.Errorf("after wrap: LineNum=%d Loop=%d, want 0/1", row.LineNum, row.Loop)
	}

	// Loop count keeps climbing each full pass. After the first wrap pos is 1
	// (line 0 was returned); consume lines 1..3, then the next call wraps.
	for i := 0; i < 3; i++ {
		_, _ = r.Next()
	}
	row, _ = r.Next()
	if row.Loop != 2 || row.LineNum != 0 {
		t.Errorf("second wrap: Loop=%d LineNum=%d, want 2/0", row.Loop, row.LineNum)
	}
}

func TestSeekToResumesAtOffset(t *testing.T) {
	r := openTestReader(t)
	r.SeekTo(2)

	row, err := r.Next()
	if err != nil {
		t.Fatalf("Next after SeekTo: %v", err)
	}
	if row.LineNum != 2 {
		t.Errorf("SeekTo(2): first row LineNum = %d, want 2", row.LineNum)
	}
	if row.Loop != 0 {
		t.Errorf("SeekTo(2): Loop = %d, want 0", row.Loop)
	}
	if row.Raw[ColGPUID] != "0" {
		t.Errorf("SeekTo(2): gpu_id = %q for row 2", row.Raw[ColGPUID])
	}
}

func TestSeekToClamps(t *testing.T) {
	r := openTestReader(t)
	r.SeekTo(99) // beyond last row -> clamped to last data row
	row, err := r.Next()
	if err != nil {
		t.Fatalf("Next after clamped SeekTo: %v", err)
	}
	if row.LineNum != 3 {
		t.Errorf("clamped SeekTo: LineNum = %d, want 3", row.LineNum)
	}
}

func TestReloadCheckDetectsRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.csv")
	header := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw\n"
	if err := os.WriteFile(path, []byte(header+"1,2,3,4,5,6,7,8,9,10,11,12\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	reloaded, err := r.ReloadCheck()
	if err != nil {
		t.Fatalf("ReloadCheck: %v", err)
	}
	if reloaded {
		t.Error("first ReloadCheck on unchanged file should report no rotation")
	}

	// simulate configMap rotation: rewrite with different content + later mtime
	if err := os.WriteFile(path, []byte(header+"9,9,9,9,9,9,9,9,9,9,9,9\n8,8,8,8,8,8,8,8,8,8,8,8\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reloaded, err = r.ReloadCheck()
	if err != nil {
		t.Fatalf("ReloadCheck after change: %v", err)
	}
	if !reloaded {
		t.Error("ReloadCheck should detect the rewritten file")
	}
	if got := r.Count(); got != 2 {
		t.Errorf("Count after reload = %d, want 2", got)
	}
}
