package dbclient

import (
	"context"
	"encoding/json"
	"fmt"

	pgx "github.com/jackc/pgx/v4"
)

const BatchSize = 500
const CLUSTER_SHARDING = false
const TOTAL_CLUSTERS = 100

// Process records using batched INSERT requests.
func batchInsert(instance string) {
	batch := &pgx.Batch{}

	for {
		record, more := <-InsertChan
		if more {
			// Marshal record.Properties to JSON
			json, err := json.Marshal(record.Properties)
			if err != nil {
				panic(fmt.Sprintf("Error Marshaling json. %v %v", err, json))
			}

			if CLUSTER_SHARDING {

				for i := 0; i < TOTAL_CLUSTERS; i++ {
					clusterName := fmt.Sprintf("cluster%d", i)

					batch.Queue("INSERT INTO '%s' values($1,$2,$3,$4)", clusterName, record.UID, record.Cluster, string(json))
				}

			} else {
				batch.Queue("INSERT into resources values($1,$2,$3,$4)", record.UID, record.Cluster, string(json))
			}
		}

		if batch.Len() == BatchSize || (!more && batch.Len() > 0) {
			fmt.Print("+")
			br := pool.SendBatch(context.Background(), batch)
			res, err := br.Exec()
			if err != nil {
				fmt.Println("res: ", res, "  err: ", err, batch.Len())
			}
			br.Close()
			batch = &pgx.Batch{}
		}
		if !more {
			break
		}
	}
}

// Process records in bulk using COPY.
// func (t *transaction) copyInsert(instance string) {
// 	t.Simulation.WG.Add(1)
// 	defer t.Simulation.WG.Done()
// 	inputRows := make([][]interface{}, t.options.BatchSize)
// 	index := 0
// 	for {
// 		record, more := <-t.Simulation.InsertChan

// 		if more {
// 			// Marshal record.Properties to JSON
// 			json, err := json.Marshal(record.Properties)
// 			if err != nil {
// 				panic(fmt.Sprintf("Error Marshaling json. %v %v", err, json))
// 			}
// 			inputRows[index] = []interface{}{record.UID, record.Cluster, json}
// 			index++
// 		}

// 		if index == t.options.BatchSize {
// 			sendUsingCopy(inputRows)
// 			inputRows = make([][]interface{}, t.options.BatchSize)
// 			index = 0
// 		} else if !more {
// 			sendUsingCopy(inputRows[0:index])
// 			break
// 		}
// 	}
// }

// // Load records using the COPY command.
// func sendUsingCopy(inputRows [][]interface{}) {
// 	// start := time.Now()

// 	// UID text PRIMARY KEY, Cluster text, NAME text, DATA JSONB
// 	copyCount, err := pool.CopyFrom(context.Background(), pgx.Identifier{tables[0]}, []string{"uid", "cluster", "data"},
// 		pgx.CopyFromRows(inputRows))

// 	if err != nil {
// 		fmt.Printf("Unexpected error for CopyFrom: %v", err)
// 	} else if int(copyCount) != len(inputRows) {
// 		fmt.Printf("Expected CopyFrom to return %d copied rows, but got %d", len(inputRows), copyCount)
// 	}

// 	fmt.Print("+")
// 	// fmt.Println("COPY Took:", time.Since(start))
// }
