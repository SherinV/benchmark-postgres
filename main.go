package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	pgx "github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
	"github.com/jlpadilla/benchmark-postgres/pkg/dbclient"
)

const (
	TOTAL_CLUSTERS = 100 // Number of SNO clusters to simulate.
	PRINT_RESULTS  = true
	SINGLE_TABLE   = false // Store relationships in single table or separate table.
	UPDATE_TOTAL   = 1000  // Number of records to update.
	DELETE_TOTAL   = 1000  // Number of records to delete.
)

var lastUID string
var database *pgxpool.Pool

func main() {
	dbclient.Single_table = SINGLE_TABLE
	fmt.Printf("Loading %d clusters from template data.\n\n", TOTAL_CLUSTERS)

	// Open the PostgreSQL database.
	database = dbclient.GetConnection()

	// Initialize the database tables.
	//var edgeStmt *sql.Stmt
	if SINGLE_TABLE {
		database.Exec(context.Background(), "DROP TABLE resources")
		database.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS resources (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB, ,edgedgesTo TEXTesFrom TEXT)")
	} else {
		for i := 0; i < TOTAL_CLUSTERS; i++ {
			clusterName := fmt.Sprintf("cluster%d", i)

			dquery := fmt.Sprintf("DROP TABLE  %s", clusterName)

			cquery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB, edgesTo TEXT,edgesFrom TEXT)", clusterName)
			fmt.Println("creating", clusterName)
			database.Exec(context.Background(), dquery)

			_, err := database.Exec(context.Background(), cquery)
			if err != nil {
				fmt.Println(err)
			}

		}

		// nodeStmt, _ = database.Prepare("INSERT INTO resources (uid, data) VALUES ($1, $2)")
		// edgeStmt, _ = database.Prepare("INSERT INTO relationships (sourceId, destId) VALUES ($1, $2)")
	}

	// Load data from file and unmarshall JSON only once.
	addNodes, _ := readTemplate()

	// Start counting here to exclude time it takes to read file and unmarshall json
	start := time.Now()

	// LESSON: When using BEGIN and COMMIT TRANSACTION saving to a file is comparable to in memory.
	for i := 0; i < TOTAL_CLUSTERS; i++ {
		// database.Exec("BEGIN TRANSACTION")
		insert(addNodes, database, fmt.Sprintf("cluster%d", i))
		//if !SINGLE_TABLE {
		//	insertEdges(addEdges, edgeStmt, fmt.Sprintf("cluster-%d", i))
		//}
		// database.Exec("COMMIT TRANSACTION")
	}

	// WORKAROUND to flush the insert channel.
	close(dbclient.InsertChan)
	time.Sleep(1 * time.Second)

	fmt.Println("\nInsert took", time.Since(start))

	savedQueries := []string{"SELECT uid from resources where data->>'kind' IN ('Daemonset', 'Deployment','Job', 'Statefulset', 'ReplicaSet') LIMIT 10 ", "SELECT uid from resources where data->>'kind' IN ('Daemonset', 'Deployment','Job', 'Statefulset', 'ReplicaSet')  "}

	relTestDescriptions := []string{"Related counts for EndpointSlice uid ", "Related counts for Connected Pod uid ", "Related counts for standalone Pod uid", "Related counts for 2 pods", "Related counts for Node uid", "Saved searches: 10 Workloads", "Saved searches: All Workloads"}
	relTestUIDs := [][]string{[]string{"cluster-0/700fd168-5431-4018-91cd-49bc43fe2f83"}, []string{"cluster-0/7bb2b2b2-bb21-4a73-8a90-6f81cf6a9884"}, []string{"cluster-0/0a062e8c-f388-4cbe-a335-6846d4052eb4"}, []string{"cluster-0/7bb2b2b2-bb21-4a73-8a90-6f81cf6a9884", "cluster-0/0a062e8c-f388-4cbe-a335-6846d4052eb4"}, []string{"cluster-0/d9fd3f13-9f08-430b-830a-43315944160f"}}
	for idx, i := range relTestUIDs {
		benchmarkRelationships(relTestDescriptions[idx], i)
	}

	for idx, i := range savedQueries {
		uids := selectRandomRecordsV2(0, i)
		benchmarkRelationships(relTestDescriptions[idx+5], uids)
	}

	// Benchmark queries
	fmt.Println("\nBENCHMARK QUERIES")

	fmt.Println("\nDESCRIPTION: Find a record using the UID")
	dbclient.BenchmarkQuery(fmt.Sprintf("SELECT uid, data FROM resources WHERE uid='%s'", lastUID), true)

	fmt.Println("\nDESCRIPTION: Count all resources")
	dbclient.BenchmarkQuery("SELECT count(*) from resources", true)

	// if !SINGLE_TABLE {
	// 	fmt.Println("\nDESCRIPTION: Count all relationships")
	// 	benchmarkQuery(database, "SELECT count(*) FROM relationships", true)
	// }

	fmt.Println("\nDESCRIPTION: Find records with a status name containing `Run`")
	dbclient.BenchmarkQuery("SELECT uid, data from resources WHERE data->>'status' = 'Running' LIMIT 10", PRINT_RESULTS)

	fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace'")
	dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' from resources", PRINT_RESULTS)

	// LESSON: Adding ORDER BY increases execution time by 2x.
	fmt.Println("\nDESCRIPTION: Find all the values for the field 'namespace' and sort in ascending order")
	dbclient.BenchmarkQuery("SELECT DISTINCT data->>'namespace' as namespace from resources ORDER BY namespace ASC", PRINT_RESULTS)

	fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind'")
	dbclient.BenchmarkQuery("SELECT data->>'kind' as kind, count(data->>'kind') as count FROM resources GROUP BY kind ORDER BY count DESC", PRINT_RESULTS)

	fmt.Println("\nDESCRIPTION: Find count of all values for the field 'kind' using subquery")
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
func insert(records []map[string]interface{}, db *pgxpool.Pool, clusterName string) {
	fmt.Print(".")

	for _, record := range records {
		lastUID = strings.Replace(record["uid"].(string), "local-cluster", clusterName, 1)
		// var err error
		//if SINGLE_TABLE {
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
		record := &dbclient.Record{UID: lastUID, Cluster: clusterName, Name: "TODO:Name here", Properties: data, EdgesTo: edgesTo, EdgesFrom: edgesFrom}
		dbclient.InsertChan <- record
		//} else {
		// _, err = statement.Exec(context.Background(), lastUID, record["data"])

		//record := &dbclient.Record{UID: lastUID, Cluster: clusterName, Name: "TODO:Name here", Properties: record["data"].(map[string]interface{})}
		//dbclient.InsertChan <- record
		//}
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

// func benchmarkQuery(database *sql.DB, q string, printResult bool) {
// 	startQuery := time.Now()
// 	rows, queryError := database.Query(q)
// 	defer rows.Close()
// 	if queryError != nil {
// 		fmt.Println("Error executing query: ", queryError)
// 	}

// 	fmt.Println("QUERY      :", q)
// 	if printResult {
// 		fmt.Println("RESULTS    :")
// 	} else {
// 		fmt.Println("RESULTS    : To print results set PRINT_RESULTS=true")
// 	}

// 	for rows.Next() {
// 		columns, _ := rows.Columns()
// 		columnData := make([]string, 3)
// 		switch len(columns) {
// 		case 3:
// 			rows.Scan(&columnData[0], &columnData[1], &columnData[2])
// 		case 2:
// 			rows.Scan(&columnData[0], &columnData[1])
// 		default:
// 			rows.Scan(&columnData[0])
// 		}

// 		if printResult {
// 			fmt.Println("  *\t", columnData[0], columnData[1], columnData[2])
// 		}
// 	}
// 	// LESSON: We can stream results from rows, but using aggregation and sorting will delay results because we have to process al records first.
// 	fmt.Printf("TIME       : %v \n\n", time.Since(startQuery))
// }

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

func benchmarkRelationships(desc string, uids []string) {
	fmt.Println("RELATIONSHIP BENCHMARK :", desc)
	savedQuery := time.Now()
	collectorMap := make(map[string]map[string]bool)
	for _, uid := range uids {

		_, _, row := searchRelationsWithUID(uid, "", 1)
		for k, v := range row {
			if ck, found := collectorMap[k]; found {
				for _, everyv := range v {
					ck[everyv] = true
				}

			} else {
				tmp := make(map[string]bool)
				for _, everyv := range v {
					tmp[everyv] = true
				}
				collectorMap[k] = tmp
			}
		}

	}
	for k, v := range collectorMap {
		fmt.Println(k, "-->", len(v))
	}
	fmt.Printf("%s TIME       : %v \n\n", desc, time.Since(savedQuery))
}

// Given uid ,returns related objects in a map , map key : "kind" => map value : list of "uid"
// You can also filter edges by certain relName( edgeType) , ex: ownedBy
// If no relName( edgeType) is specified all edge types are used
// depth specifies number of hops , if set to 0 all hops are traversed
// Uses BFS to find edges
func searchRelationsWithUID(uid string, relName string, depth int) (int, int, map[string][]string) {
	//start := time.Now()
	if depth == 0 {
		depth = math.MaxInt32
	}
	result := make(map[string][]string)
	visited := make(map[string]bool)
	var Q []string
	visited[uid] = true
	depthFound := 0
	var currentRels map[string][][]string
	Q = append(Q, uid)
	for len(Q) > 0 {

		currentID := Q[0]
		Q = Q[1:]
		if depth > depthFound {
			currentRels = getAllRelationships(currentID)
			depthFound = depthFound + 1
		}

		if len(currentRels) == 0 {
			continue
		}
		var vals [][]string
		var found bool
		if len(relName) != 0 {
			if vals, found = currentRels[relName]; !found {
				continue
			}

		} else {
			for _, v := range currentRels {
				vals = append(vals, v...)
			}
		}

		for _, val := range vals {
			if _, found := visited[val[0]]; !found {
				Q = append(Q, val[0])
				visited[val[0]] = true
				// adding to result map
				if ids, found := result[val[1]]; found {
					ids = append(ids, val[0])
					result[val[1]] = ids
				} else {
					result[val[1]] = []string{val[0]}
				}
			}
		}

	}
	//Collect the edges from resources that connect to the UID
	depthFound = 0
	visited = make(map[string]bool)
	visited[uid] = true
	Q = append(Q, uid)
	for len(Q) > 0 {
		currentID := Q[0]
		Q = Q[1:]
		if depth > depthFound {
			currentRels = getAllConnectedFrom(currentID)
			depthFound = depthFound + 1
		}
		if len(currentRels) == 0 {
			continue
		}
		var vals [][]string
		var found bool
		if len(relName) != 0 {
			if vals, found = currentRels[relName]; !found {
				continue
			}
		} else {
			for _, v := range currentRels {
				vals = append(vals, v...)
			}
		}
		for _, val := range vals {
			if _, found := visited[val[0]]; !found {
				Q = append(Q, val[0])
				visited[val[0]] = true
				// adding to result map
				if ids, found := result[val[1]]; found {
					ids = append(ids, val[0])
					result[val[1]] = ids
				} else {
					result[val[1]] = []string{val[0]}
				}
			}
		}

	}

	/*if len(result) > 0 {
		fmt.Println("Starting uid : ", uid)
		fmt.Println("maxDepth", depthFound, "==", "totalEdges", len(result), "==", result)
		fmt.Printf("TIME       : %v \n\n", time.Since(start))
	}*/

	return depthFound, len(result), result

}

// A function to get the relatedTo filed as a map Given the id , in resources table
func getAllRelationships(uid string) map[string][][]string {
	var relatedTo map[string][][]string
	tableName := uid[:strings.Index(uid, "/")]
	tableNameClean := strings.ReplaceAll(tableName, "-", "")
	uid = tableNameClean + uid[strings.Index(uid, "/"):]
	dquery := fmt.Sprintf("SELECT edgesTo FROM %s where uid='%s'", tableNameClean, uid)
	fmt.Println(dquery)
	rows, err := database.Query(context.Background(), dquery)

	if err != nil {
		fmt.Println(err)
	}
	var relatedTo2 string

	for rows.Next() {
		rows.Scan(&relatedTo2)
	}
	fmt.Println("relatedTo2", relatedTo2)
	//fmt.Println(relatedTo2)
	rows.Close()
	if err := json.Unmarshal([]byte(relatedTo2), &relatedTo); err != nil {
		fmt.Println(err)
	}
	//fmt.Println(uid, "==>", "(", kind, ")", relatedTo)
	return relatedTo
}

func generateAllRelationships() {
	rows, _ := database.Query(context.Background(), "SELECT uid FROM resources")
	var uids []string
	var uid string

	for rows.Next() {
		rows.Scan(&uid)
		uids = append(uids, uid)
	}
	rows.Close()
	for _, uid := range uids {
		//obtain all the nodes that can be travered from uid
		searchRelationsWithUID(uid, "", 0)
	}

}
func getAllConnectedFrom(uid string) map[string][][]string {
	var relatedTo map[string][][]string

	tableName := uid[:strings.Index(uid, "/")]
	tableNameClean := strings.ReplaceAll(tableName, "-", "")
	uid = tableNameClean + uid[strings.Index(uid, "/"):]
	dquery := fmt.Sprintf("SELECT edgesFrom FROM %s where uid='%s'", tableNameClean, uid)

	rows, err := database.Query(context.Background(), dquery)

	//rows, err := database.Query(context.Background(), "SELECT edgesFrom FROM resources where uid=$1 ", uid)

	if err != nil {
		fmt.Println(err)
	}
	var relatedTo2 string

	for rows.Next() {
		rows.Scan(&relatedTo2)
	}
	rows.Close()
	if err := json.Unmarshal([]byte(relatedTo2), &relatedTo); err != nil {
		fmt.Println(err)
	}
	//fmt.Println(uid, "<==", "(", kind, ")", relatedTo)
	return relatedTo
}

func selectRandomRecordsV2(total int, query string) []string {
	var uids []string
	var records pgx.Rows

	if query == "" {
		records, _ = database.Query(context.Background(), "SELECT uid FROM resources ORDER BY RANDOM() LIMIT $1", total)
		uids = make([]string, total)
	} else {
		records, _ = database.Query(context.Background(), query)
	}
	var id string
	for i := 0; records.Next(); i++ {
		scanErr := records.Scan(&id)
		uids = append(uids, id)
		if scanErr != nil {
			fmt.Println(scanErr)
		}
	}
	return uids
}
