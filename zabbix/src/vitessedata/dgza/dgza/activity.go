package dgza

import (
	"database/sql"
	"fmt"
)

func Ping(db *sql.DB) {
	sql := `select 1`
	rows, err := db.Query(sql)
	if err != nil {
		panic("Ping failed: " + err.Error())
	}
	defer rows.Close()
	fmt.Printf("1\n")
}

func GetSegStatus(db *sql.DB) {
	sql := `select sum (case when status = 'u' then 1 else 0 end),
	               sum (case when status = 'd' then 1 else 0 end) 
			from gp_segment_configuration
			`
	rows, err := db.Query(sql)
	if err != nil {
		panic("Get seg status failed: " + err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var v [2]int
		rows.Scan(&v[0], &v[1])
		fmt.Printf("- deepgreen.up_segments %d\n", v[0])
		fmt.Printf("- deepgreen.down_segments %d\n", v[1])
	}
}

func GetActivity(db *sql.DB) {
	sql := `select sum (case when current_query = '<IDLE>' then 1 else 0 end),
				   sum (case when current_query = '<IDLE>' then 0 else 1 end),
				   count (*),
				   sum (case when current_timestamp - query_start > interval '60 seconds' then 1 else 0 end)
			from pg_stat_activity
			`

	rows, err := db.Query(sql)
	if err != nil {
		panic("Get activity failed: " + err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var v [4]int
		rows.Scan(&v[0], &v[1], &v[2], &v[3])
		fmt.Printf("- deepgreen.idle_connections %d\n", v[0])
		fmt.Printf("- deepgreen.active_connections %d\n", v[1])
		fmt.Printf("- deepgreen.total_connections %d\n", v[2])
		fmt.Printf("- deepgreen.slow_queries %d\n", v[3])
	}
}
