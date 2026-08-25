// Package store keeps tasks, execution records and snapshots.
package store

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
	"github.com/dyl-04/sched/internal/model"
)

// Snapshot is a serializable view of the task repository.
type Snapshot struct {
	Version int64            `json:"version"`
	Tasks   []*model.Task    `json:"tasks"`
	Records []*model.ExecutionRecord `json:"records"`
}

// Export serializes the current stores to a snapshot writer.
func Export(tasks *TaskStore, records *RecordStore, w io.Writer) (string, error) {
	taskList := tasks.List(100000, 0)
	recordList := []*model.ExecutionRecord{}
	for _, t := range taskList {
		recordList = append(recordList, records.List(t.ID)...)
	}
	snap := Snapshot{Version: 1, Tasks: taskList, Records: recordList}
	data, err := json.Marshal(snap)
	if err != nil {
		return "", err
	}
	if _, err := w.Write(data); err != nil {
		return "", err
	}
	return fmt.Sprintf("%016x", xxhash.Sum64(data)), nil
}

// Import loads tasks from a snapshot reader into the store.
func Import(tasks *TaskStore, records *RecordStore, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	tasks.Reset()
	for _, t := range snap.Tasks {
		if err := tasks.Save(t); err != nil {
			return err
		}
	}
	for _, rec := range snap.Records {
		if err := records.Append(rec); err != nil {
			return err
		}
	}
	return nil
}
