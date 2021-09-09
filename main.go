package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	pgxpool "github.com/jackc/pgx/v4/pgxpool"
	"github.com/jlpadilla/benchmark-postgres/pkg/dbclient"
)

const (
	TOTAL_CLUSTERS   = 100 // Number of SNO clusters to simulate.
	PRINT_RESULTS    = true
	SINGLE_TABLE     = true // Store relationships in single table or separate table.
	UPDATE_TOTAL     = 1000 // Number of records to update.
	DELETE_TOTAL     = 1000 // Number of records to delete.
	CLUSTER_SHARDING = false
)

var lastUID string
var database *pgxpool.Pool

func main() {
	fmt.Printf("Loading %d clusters from template data.\n\n", TOTAL_CLUSTERS)

	// Open the PostgreSQL database.
	database = dbclient.GetConnection()

	// Initialize the database tables.
	var edgeStmt *sql.Stmt
	if SINGLE_TABLE {

		if CLUSTER_SHARDING {
			for i := 0; i < TOTAL_CLUSTERS; i++ {
				clusterName := fmt.Sprintf("cluster%d", i)
				dquery := fmt.Sprintf("drop table if exists %s cascade; ", clusterName)
				// fmt.Println("Dropping", clusterName)
				// fmt.Println("Dropping", dquery)

				database.Exec(context.Background(), dquery)
				database.Exec(context.Background(), "COMMIT TRANSACTION")

				cquery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB, edgesTo TEXT, edgesFrom TEXT)", clusterName)
				// fmt.Println("creating", clusterName)
				_, err := database.Exec(context.Background(), cquery)
				if err != nil {
					fmt.Println(err)
				}
			}
		} else { //  single table but not cluster sharding
			database.Exec(context.Background(), "DROP TABLE resources")
			database.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS resources (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB, edgesTo TEXT, edgesFrom TEXT))")
			// nodeStmt, _ = database.Prepare(fmt.Sprintf("INSERT INTO resources (uid, cluster, data, relatedTo) VALUES (?, ?, ?, ?)"))
		}
	} else {
		database.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS resources (uid TEXT PRIMARY KEY, data TEXT)")
		database.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS relationships (sourceId TEXT, destId TEXT)")
		// nodeStmt, _ = database.Prepare("INSERT INTO resources (uid, data) VALUES ($1, $2)")
		// edgeStmt, _ = database.Prepare("INSERT INTO relationships (sourceId, destId) VALUES ($1, $2)")
	}

	// Load data from file and unmarshall JSON only once.
	addNodes, addEdges := readTemplate()

	// Start counting here to exclude time it takes to read file and unmarshall json
	start := time.Now()

	// LESSON: When using BEGIN and COMMIT TRANSACTION saving to a file is comparable to in memory.
	for i := 0; i < TOTAL_CLUSTERS; i++ {
		tableName := "resources"
		if CLUSTER_SHARDING {
			tableName = fmt.Sprintf("cluster%d", i)
		}

		insert(tableName, addNodes, database, fmt.Sprintf("cluster%d", i))
		fmt.Sprintln("Inserting ", tableName)
		if !SINGLE_TABLE {
			insertEdges(addEdges, edgeStmt, fmt.Sprintf("cluster%d", i))
		}
		// database.Exec("COMMIT TRANSACTION")
	}

	// WORKAROUND to flush the insert channel.
	close(dbclient.InsertChan)
	time.Sleep(1 * time.Second)

	fmt.Println("\nInsert took", time.Since(start))
	fmt.Printf("Total clusters: %d \n\n", TOTAL_CLUSTERS)

	if SINGLE_TABLE {
		if CLUSTER_SHARDING {

			dbclient.BenchmarkQuery("SELECT table_name FROM information_schema.tables where table_name like 'cluster%'", PRINT_RESULTS)
			dbclient.BenchmarkQuery("SELECT COUNT(*) FROM cluster1", PRINT_RESULTS)

			fmt.Println("DESCRIPTION: Create a view.")

			createView("SELECT table_name FROM information_schema.tables where table_name like 'cluster%'")
		}
	}

	// Benchmark queries
	fmt.Println("\nBENCHMARK QUERIES")

	// if !SINGLE_TABLE && !CLUSTER_SHARDING {
	// 	fmt.Println("\nDESCRIPTION: Count all relationships")
	// 	benchmarkQuery(database, "SELECT count(*) FROM relationships", true)
	// }

	// if !SINGLE_TABLE && CLUSTER_SHARDING {
	// 	fmt.Println("\nDESCRIPTION: Count all relationships")
	// 	benchmarkQuery(database, "SELECT count(*) FROM relationship_view", true)

	// 	fmt.Println("\nDESCRIPTION: Find all relationships for uid =n")
	// 	benchmarkQuery(database, "SELECT * from relationship_view where sourceId = 'cluster0/0a6467fc-8a79-47cd-aa75-f405c4f02c36'", false)
	// }

	if SINGLE_TABLE && CLUSTER_SHARDING {

		fmt.Println("\nDESCRIPTION: Find a record using the UID")
		dbclient.BenchmarkQuery(fmt.Sprintf("SELECT uid, data FROM resource_view WHERE uid='%s'", lastUID), true)

		fmt.Println("\nDESCRIPTION: Count all resources")
		dbclient.BenchmarkQuery("SELECT count(*) from resource_view", true)

		fmt.Println("\nDESCRIPTION: Find records with a status name containing `Run`")
		dbclient.BenchmarkQuery("SELECT uid, data from resource_view where data->> 'status' = 'Running' LIMIT 10", true)

		fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace' from view")
		dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' from resource_view", true)

		//specific cluster known ^
		// fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace' from cluster1")
		// dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' from cluster1", false)

		// LESSON: Adding ORDER BY increases execution time by 2x.
		fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace' and sort in ascending order")
		dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' as namespace from resource_view ORDER BY namespace ASC", true)

		// fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace' (no-sorting)")
		// benchmarkQuery(database, "SELECT DISTINCT json_extract(data, '$.namespace') as namespace from resource_view", false)

		fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind' (without order-by)")
		dbclient.BenchmarkQuery("SELECT data->>'kind' as kind , count(data->>'kind') as count FROM resource_view GROUP BY kind", true)

		fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind'")
		dbclient.BenchmarkQuery("SELECT data->>'kind' as kind , count(data->>'kind') as count FROM resource_view GROUP BY kind ORDER BY count DESC", true)

		fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind' using subquery")
		dbclient.BenchmarkQuery("SELECT kind, count(*) as count FROM (SELECT data->>'kind' as kind FROM resource_view)A GROUP BY kind ORDER BY count DESC", true)

		fmt.Println("\nDESCRIPTION: Delete a single record.")
		dbclient.BenchmarkQuery(fmt.Sprintf("DELETE FROM cluster3 WHERE uid='cluster3/29ac6c4a-181c-499f-b059-977d7e9889dd'"), true)

		fmt.Println("\nDESCRIPTION: Delete 1000 records.")
		dbclient.BenchmarkQuery(fmt.Sprintf("DELETE FROM cluster2 WHERE ctid IN (SELECT ctid FROM cluster2 ORDER BY RANDOM() limit 1000)"), true)

		fmt.Println("\nDESCRIPTION: Update a a single record: cluster5/0c2ec6f3-fa4d-436e-803a-c3e1c1c4bce0'.")
		dbclient.BenchmarkQuery(fmt.Sprintf("UPDATE cluster5 SET kind = 'value was updated' WHERE uid = 'cluster5/0c2ec6f3-fa4d-436e-803a-c3e1c1c4bce0'"), true)
		// dbclient.BenchmarkQuery(fmt.Sprintf("UPDATE cluster5 SET data = json_set(data, '$.kind', 'value was updated') WHERE id = 'cluster5/0c2ec6f3-fa4d-436e-803a-c3e1c1c4bce0'"), true)

		fmt.Println("\nDESCRIPTION: UPDATE 1000 records.")
		dbclient.BenchmarkQuery(fmt.Sprintf("UPDATE cluster5 SET kind = 'value was updated' WHERE ctid IN (SELECT ctid FROM cluster5 ORDER BY RANDOM() limit 1000)"), true)

	}

	if SINGLE_TABLE && !CLUSTER_SHARDING {

		fmt.Println("\nDESCRIPTION: Find a record using the UID")
		dbclient.BenchmarkQuery(fmt.Sprintf("SELECT id, data FROM resources WHERE id='%s'", lastUID), true)

		fmt.Println("\nDESCRIPTION: Count all resources")
		dbclient.BenchmarkQuery("SELECT count(*) from resources", true)

		fmt.Println("\nDESCRIPTION: Find records with a status name containing `Run`")
		dbclient.BenchmarkQuery("SELECT id, data from resources where data->> 'status' = 'Running'  LIMIT 10", false)

		fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace'")
		dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' from resources", false)

		// LESSON: Adding ORDER BY increases execution time by 2x.
		fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace' and sort in ascending order")
		dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' as namespace from resources ORDER BY namespace ASC", false)

		fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind'")
		dbclient.BenchmarkQuery("SELECT data->>'kind' as kind , count(data->>'kind') as count FROM  resources GROUP BY kind ORDER BY count DESC", false)

		fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind' using subquery")
		dbclient.BenchmarkQuery("SELECT kind, count(*) as count FROM (SELECT data->>'kind' as kind FROM resources) GROUP BY kind ORDER BY count DESC", false)

		fmt.Println("\nDESCRIPTION: Delete a single record.")
		dbclient.BenchmarkQuery(fmt.Sprintf("DELETE FROM resources WHERE id='cluster3/29ac6c4a-181c-499f-b059-977d7e9889dd'"), true)

		fmt.Println("\nDESCRIPTION: Delete 1000 records.")
		dbclient.BenchmarkQuery(fmt.Sprintf("DELETE FROM resources WHERE ctid IN (SELECT ctid FROM resources ORDER BY RANDOM() limit 1000)"), true)

		fmt.Println("\nDESCRIPTION: Update a single record.")
		dbclient.BenchmarkQuery(fmt.Sprintf("UPDATE resources SET data->>'kind' as kind ='value was updated' WHERE id like 'cluster99/0c2ec6f3-fa4d-436e-803a-c3e1c1c4bce0'"), true)

		fmt.Println("\nDESCRIPTION: UPDATE 1000 records.")
		dbclient.BenchmarkQuery(fmt.Sprintf("UPDATE resources SET data->>'kind' as kind = 'value was updated' WHERE ctid IN (SELECT ctid FROM resources ORDER BY RANDOM() limit 1000)"), true)
	}

	PrintMemUsage()
	fmt.Println("\nWon't exit so I can get memory usage from OS.")
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

	// fmt.Println("\nDESCRIPTION: Find a record using the UID")
	// dbclient.BenchmarkQuery(fmt.Sprintf("SELECT uid, data FROM resources WHERE uid='%s'", lastUID), true)

	// fmt.Println("\nDESCRIPTION: Count all resources")
	// dbclient.BenchmarkQuery("SELECT count(*) from resources", true)

	// // if !SINGLE_TABLE {
	// // 	fmt.Println("\nDESCRIPTION: Count all relationships")
	// // 	benchmarkQuery(database, "SELECT count(*) FROM relationships", true)
	// // }

	// fmt.Println("\nDESCRIPTION: Find records with a status name containing `Run`")
	// dbclient.BenchmarkQuery("SELECT uid, data from resources WHERE data->>'status' = 'Running' LIMIT 10", PRINT_RESULTS)

	// fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace'")
	// dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' from resources", PRINT_RESULTS)

	// // LESSON: Adding ORDER BY increases execution time by 2x.
	// fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace' and sort in ascending order")
	// dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' as namespace from resources ORDER BY namespace ASC", PRINT_RESULTS)

	// fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind'")
	// dbclient.BenchmarkQuery("SELECT data->>'kind' as kind, count(data->>'kind') as count FROM resources GROUP BY kind ORDER BY count DESC", PRINT_RESULTS)

	// fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind' using subquery")
	// benchmarkQuery(database, "SELECT kind, count(*) as count FROM (SELECT json_extract(resources.data, '$.kind') as kind FROM resources) GROUP BY kind ORDER BY count DESC", PRINT_RESULTS)
	// dbclient.BenchmarkQuery("SELECT kind, count(*) as count FROM (SELECT data->>'kind' as kind FROM resources) GROUP BY kind ORDER BY count DESC", PRINT_RESULTS)

	// fmt.Println("\nDESCRIPTION: Update a single record.")
	// benchmarkQuery(database, fmt.Sprintf("UPDATE resources SET data = json_set(data, '$.kind', 'value was updated') WHERE uid='%s'", lastUID), true)
	// // Print record to verify it was modified.
	// // benchmarkQuery(database, fmt.Sprintf("SELECT uid, data FROM resources WHERE uid='%s'", lastUID), true)

	// fmt.Printf("DESCRIPTION: Update %d records in the database.\n", UPDATE_TOTAL)
	// benchmarkUpdate(database, UPDATE_TOTAL)

	// fmt.Println("\nDESCRIPTION: Delete a single record.")
	// benchmarkQuery(database, fmt.Sprintf("DELETE FROM resources WHERE uid='%s'", lastUID), true)
	// // Print record to verify it was deleted.
	// // benchmarkQuery(database, fmt.Sprintf("SELECT uid, data FROM resources WHERE uid='%s'", lastUID), true)

	// fmt.Printf("DESCRIPTION: Delete %d records from the database.\n", DELETE_TOTAL)
	// benchmarkDelete(database, DELETE_TOTAL)

	fmt.Println("DONE exiting.")

}

/*****************************
Helper functions
*****************************/

/*
 * Read cluster data from file.
 */
func readTemplate() ([]map[string]interface{}, []map[string]interface{}) {
	bytes, _ := ioutil.ReadFile("./data/sno-0.json")
	var data map[string]interface{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		panic(err)
	}
	records := data["addResources"].([]interface{})
	edges := data["addEdges"].([]interface{})

	// Edges format is: { "edgeTypeA": ["destUID1", destUID2], "edgeTypeB": ["destUID3"]}
	findEdgesTo := func(sourceUID string) string {
		result := make(map[string][][]string)
		for _, edge := range edges {
			edgeMap := edge.(map[string]interface{})
			if edgeMap["SourceUID"] == sourceUID {
				edgeType := edgeMap["EdgeType"].(string)
				kind := edgeMap["DestKind"].(string)
				temp := make([]string, 0)
				temp = append(temp, edgeMap["DestUID"].(string))
				temp = append(temp, kind)
				destUIDs, exist := result[edgeType]
				if exist {
					result[edgeType] = append(destUIDs, temp)
				} else {
					result[edgeType] = [][]string{temp}
				}
			}
		}
		edgeJSON, _ := json.Marshal(result)
		return string(edgeJSON)
	}

	findEdgesFrom := func(destUID string) string {
		result := make(map[string][][]string)
		for _, edge := range edges {
			edgeMap := edge.(map[string]interface{})
			if edgeMap["DestUID"] == destUID {
				edgeType := edgeMap["EdgeType"].(string)
				kind := edgeMap["SourceKind"].(string)
				temp := make([]string, 0)
				temp = append(temp, edgeMap["SourceUID"].(string))
				temp = append(temp, kind)
				srcUIDs, exist := result[edgeType]
				if exist {
					result[edgeType] = append(srcUIDs, temp)
				} else {
					result[edgeType] = [][]string{temp}
				}
			}
		}
		edgeJSON, _ := json.Marshal(result)
		return string(edgeJSON)
	}

	addResources := make([]map[string]interface{}, len(records))
	for i, record := range records {
		uid := record.(map[string]interface{})["uid"]
		properties := record.(map[string]interface{})["properties"]
		data, _ := json.Marshal(properties)

		eTo := findEdgesTo(uid.(string))
		eFrom := findEdgesFrom(uid.(string))
		// LESSON - QUESTION: UIDs are long and use too much space. What is the risk of compressing?
		// uid = "local-cluster/" + strings.Split(uid.(string), "-")[5]
		//fmt.Println(eTo)
		addResources[i] = map[string]interface{}{"uid": uid, "data": string(data), "edgesTo": eTo, "edgesFrom": eFrom}
	}

	addEdges := make([]map[string]interface{}, len(edges))
	for i, edge := range edges {
		t := edge.(map[string]interface{})["EdgeType"]
		s := edge.(map[string]interface{})["SourceUID"]
		d := edge.(map[string]interface{})["DestUID"]
		addEdges[i] = map[string]interface{}{"sourceUID": s, "destUID": d, "type": t}
	}
	return addResources, addEdges
}

/*
 * Insert records
 */
func insert(tableName string, records []map[string]interface{}, db *pgxpool.Pool, clusterName string) {
	fmt.Print(".")

	for _, record := range records {
		lastUID = strings.Replace(record["uid"].(string), "local-cluster", clusterName, 1)
		// var err error
		if SINGLE_TABLE {
			// edges := record["edges"].(string)
			// edges = strings.ReplaceAll(edges, "local-cluster", clusterName)
			// _, err = db.Exec(context.Background(), lastUID, record["data"], edges)
			edgesTo := record["edgesTo"].(string)
			edgesTo = strings.ReplaceAll(edgesTo, "local-cluster", clusterName)
			edgesFrom := record["edgesFrom"].(string)
			edgesFrom = strings.ReplaceAll(edgesFrom, "local-cluster", clusterName)

			var data map[string]interface{}
			bytes := []byte(record["data"].(string))
			if err := json.Unmarshal(bytes, &data); err != nil {
				panic(err)
			}

			// fmt.Printf("Pushing to insert channnel... cluster %s. %s\n", clusterName, lastUID)
			record := &dbclient.Record{TableName: tableName, UID: lastUID, Cluster: clusterName, Name: "TODO:Name here", Properties: data, EdgesTo: edgesTo, EdgesFrom: edgesFrom}
			dbclient.InsertChan <- record

		} else {
			// _, err = statement.Exec(context.Background(), lastUID, record["data"])

			record := &dbclient.Record{UID: lastUID, Cluster: clusterName, Name: "TODO:Name here", Properties: record["data"].(map[string]interface{})}
			dbclient.InsertChan <- record
		}
		// if err != nil {
		// 	fmt.Println("Error inserting record:", err, statement)
		// 	panic(err)
		// }
	}
	// tx.Commit(context.Background())
}

/*
 * Insert edges in separate table.
 */
func insertEdges(edges []map[string]interface{}, statement *sql.Stmt, clusterName string) {
	fmt.Print(">")
	for _, edge := range edges {
		source := strings.Replace(edge["sourceUID"].(string), "local-cluster", clusterName, 1)
		dest := strings.Replace(edge["destUID"].(string), "local-cluster", clusterName, 1)
		_, err := statement.Exec(source, dest)

		if err != nil {
			fmt.Println("Error inserting edge:", err)
		}
	}
}

func createView(tableNames string) {
	// database.Exec(fmt.Sprintf("CREATE VIEW IF NOT EXISTS combined AS SELECT name FROM benchmark.tables"))
	rows, queryError := database.Query(context.Background(), tableNames)
	// fmt.Println(rows)
	if queryError != nil {
		fmt.Println("Error executing query: ", queryError)
	}
	createViewQuery := "CREATE VIEW resource_view AS "

	for i := 0; rows.Next(); i++ {

		var clusterName string
		rows.Scan(&clusterName) //this convert the values from rows into string go object.
		if i == 0 {
			createViewQuery = createViewQuery + "SELECT * FROM " + clusterName
		} else {
			createViewQuery = createViewQuery + " UNION ALL SELECT * FROM " + clusterName
		}
	}
	fmt.Println(createViewQuery)
	_, createViewError := database.Exec(context.Background(), createViewQuery)
	if createViewError != nil {
		fmt.Println("Error creating view : ", createViewError)
	}
	database.Exec(context.Background(), "COMMIT TRANSACTION")
}

func benchmarkQuery(database *sql.DB, q string, printResult bool) {
	startQuery := time.Now()
	rows, queryError := database.Query(q)
	defer rows.Close()
	if queryError != nil {
		fmt.Println("Error executing query: ", queryError)
	}

	fmt.Println("QUERY      :", q)
	if printResult {
		fmt.Println("RESULTS    :")
	} else {
		fmt.Println("RESULTS    : To print results set PRINT_RESULTS=true")
	}

	for rows.Next() {
		columns, _ := rows.Columns()
		columnData := make([]string, 3)
		switch len(columns) {
		case 3:
			rows.Scan(&columnData[0], &columnData[1], &columnData[2])
		case 2:
			rows.Scan(&columnData[0], &columnData[1])
		default:
			rows.Scan(&columnData[0])
		}

		if printResult {
			fmt.Println("  *\t", columnData[0], columnData[1], columnData[2])
		}
	}
	// LESSON: We can stream results from rows, but using aggregation and sorting will delay results because we have to process al records first.
	fmt.Printf("TIME       : %v \n\n", time.Since(startQuery))
}

/*
 * Helper method to select records for Update and Delete.
 */
func selectRandomRecords(database *sql.DB, total int) []string {
	records, err := database.Query("SELECT uid FROM resources ORDER BY RANDOM() LIMIT $1", total)
	if err != nil {
		fmt.Println("Error getting random uids. ", err)
	}
	uids := make([]string, total)
	for i := 0; records.Next(); i++ {
		scanErr := records.Scan(&uids[i])
		if scanErr != nil {
			fmt.Println(scanErr)
		}
	}
	return uids
}

/*
 * Benchmark UPDATE
 */
func benchmarkUpdate(database *sql.DB, updateTotal int) {
	// First, let's find some random records to update.
	uids := selectRandomRecords(database, updateTotal)

	// Now that we have the UIDs we want to update, start benchmarking from here.
	start := time.Now()
	updateStmt, _ := database.Prepare("UPDATE resources SET data = json_set(data, '$.kind', 'Updated value') WHERE id = $1")
	defer updateStmt.Close()
	// Lesson: Using BEGIN/COMMIT TRANSACTION doesn't seem to affect performance.
	for _, uid := range uids {
		updateStmt.Exec(uid)
	}

	fmt.Printf("QUERY      : UPDATE resources SET data = json_set(data, '$.kind', 'Updated value') WHERE id = $1 \n")
	fmt.Printf("TIME       : %v \n\n", time.Since(start))
}

/*
 * Benchmark DELETE
 */
func benchmarkDelete(database *sql.DB, deleteTotal int) {
	// First, let's find some random records to delete.
	uids := selectRandomRecords(database, deleteTotal)

	// Now that we have the UIDs we want to delete, start benchmarking from here.
	start := time.Now()
	_, err := database.Exec("DELETE from resources WHERE uid IN ($1)", strings.Join(uids, ", "))
	if err != nil {
		fmt.Println("error: ", err)
	}
	fmt.Printf("QUERY      : DELETE from resources WHERE uid IN ($1) \n") //, strings.Join(uids, ", "))
	fmt.Printf("TIME       : %v \n\n", time.Since(start))
}

func getEnvOrUseDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Println("\nMEMORY USAGE:")
	fmt.Printf("\tAlloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\n\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\n\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\n\tNumGC = %v\n\n", m.NumGC)
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
