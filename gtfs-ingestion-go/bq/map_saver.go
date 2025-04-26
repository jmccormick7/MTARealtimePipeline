package bq

import (
    "cloud.google.com/go/bigquery"
)

type mapSaver struct {
    row      map[string]bigquery.Value
    insertID string
}

func (m *mapSaver) Save() (map[string]bigquery.Value, string, error) {
    return m.row, m.insertID, nil
}
