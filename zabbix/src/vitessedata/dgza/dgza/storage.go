package dgza

import (
	"database/sql"
	"fmt"
)

//
// Many storage related views has been implemented in gp_toolkit.  However, the most
// straightforward one (seg_data_skew, in my opinion) is not easy to read out of gp_toolkit.
// Here we implement it as an example of transducer.  Transducer, esp. the Exec, is
// a very big gun.  It is best gun ever invented for shooting one's own foot.   Use with care.
//
// The implementation assumes all data are stored in default place ($PWD when transducer runs).
// If user has created their own tablespace, need to fix the scritps.
//

//
// Transducer to read the data size on eachseg.  Return total storage and storage
// skew (max/min, non-statistician's definition) to Zabbix.
//
func GetSegStorage(db *sql.DB) {
	sql := `
	select sum(sz), max(sz) / min(sz) from (
		select col1::float8 as sz from (
			select 
			dg_utils.transducer(E'Exec {
				"cmd": "bash -c ''du --summarize . | cut -f1 ''", 
				"ncol": 1 }', seg.*), 
			dg_utils.transducer_column_text(1) as col1
			from (select 'i'::text from dg_utils.eachseg) seg 
		) trt  
	) szt 
	`

	rows, err := db.Query(sql)
	if err != nil {
		panic("Get seg data size failed: " + err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var tot float64
		var skew float64
		rows.Scan(&tot, &skew)
		fmt.Printf("- deepgreen.seg_data_size %f\n", tot)
		fmt.Printf("- deepgreen.seg_data_skew %f\n", skew)
	}
}

//
// The following query execute du on master, return data size and log dir size.  We have
// seen excessive logging blow up the log dir and causing trouble.   Log dir size is also
// hard to read out from gp_toolkit.
//
func GetMasterStorage(db *sql.DB) {
	sql := `
	select 'master_data_size', col1::float8 from (
		select 
		dg_utils.transducer(E'Exec {
			"cmd": "bash -c ''du --summarize . | cut -f1 ''", 
			"ncol": 1 }', seg.*), 
		dg_utils.transducer_column_text(1) as col1
		from (select 'master'::text) seg ) datat
	union all
	select 'master_log_dir_size', col1::float8 from (
		select 
		dg_utils.transducer(E'Exec {
			"cmd": "bash -c ''du --summarize pg_log | cut -f1 ''", 
			"ncol": 1 }', seg.*), 
		dg_utils.transducer_column_text(1) as col1
		from (select 'master'::text) seg ) logt
	`

	rows, err := db.Query(sql)
	if err != nil {
		panic("Get master data size failed: " + err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var tag string
		var sz float64
		rows.Scan(&tag, &sz)
		fmt.Printf("- deepgreen.%s %f\n", tag, sz)
	}
}
