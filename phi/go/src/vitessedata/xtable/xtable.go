/*
XTable
*/

package xtable

import (
	"database/sql"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"io"
	"strconv"
	"strings"
)

type XColDef struct {
	Name    string
	SQLType string
}

type XTableSchema struct {
	Columns []XColDef
}

func (s *XTableSchema) NCol() int {
	return len(s.Columns)
}

func (s *XTableSchema) ColName(n int) string {
	return s.Columns[n].Name
}

type XTableCost struct {
	NRows     float64
	RowWidth  float64
	TotalCost float64
	PlanText  string
}

type XTable interface {
	Sql() (string, error)

	Explain() error
	Execute() (*sql.Rows, error)
	RenderAll(w io.Writer) error
	RenderN(w io.Writer, n int) error

	Schema() *XTableSchema
	Cost() *XTableCost
	Alias() string
	SetAlias(a string)
	Inputs() []XTable
}

type xtableCommon struct {
	dg     *Deepgreen
	schema XTableSchema
	cost   XTableCost
	alias  string
	inputs []XTable
	sql    string
}

func (t *xtableCommon) Alias() string {
	return t.alias
}

func (t *xtableCommon) SetAlias(a string) {
	t.alias = a
}

func (t *xtableCommon) Schema() *XTableSchema {
	return &t.schema
}

func (t *xtableCommon) Cost() *XTableCost {
	return &t.cost
}

func (t *xtableCommon) Inputs() []XTable {
	return t.inputs
}

// #x# -> where x is a number, tablealias
// #x.y# -> Where x is a number, y can be either a number or col name.
// ## -> # escape.
func (t *xtableCommon) resolveCol(s string) (string, error) {
	strs := strings.Split(s, "#")

	for i := 1; i < len(strs); i = i + 2 {
		if strs[i] == "" {
			if i == len(strs)-1 {
				return "", fmt.Errorf("# must be escaped as ##")
			}
			strs[i] = "#"
		} else {
			fields := strings.Split(strs[i], ".")
			if len(fields) != 1 && len(fields) != 2 {
				return "", fmt.Errorf("Colref must be #x# or #x.y#")
			}

			tabn, err := strconv.Atoi(fields[0])
			if err != nil || tabn >= len(t.inputs) {
				return "", fmt.Errorf("Colref table ref is not valid.")
			}

			srct := t.inputs[tabn]
			if len(fields) == 1 {
				strs[i] = srct.Alias()
			} else {
				colref := srct.Alias() + "."
				coln, err := strconv.Atoi(fields[1])
				if err != nil {
					colref = colref + fields[1]
				} else if coln >= srct.Schema().NCol() {
					return "", fmt.Errorf("Colref coln out of range")
				} else {
					colref = colref + srct.Schema().ColName(coln)
				}
				strs[i] = colref
			}
		}
	}

	return strings.Join(strs, ""), nil
}

func (t *xtableCommon) Sql() (string, error) {
	body, err := t.resolveCol(t.sql)
	if err != nil {
		return "", err
	}

	if t.inputs == nil || len(t.inputs) == 0 {
		return body, nil
	}

	ret := "WITH "
	for idx, it := range t.inputs {
		itsql, err := it.Sql()
		if err != nil {
			return "", err
		}
		ret += fmt.Sprintf("%s as (%s)", it.Alias(), itsql)
		if idx == len(t.inputs)-1 {
			ret += "\n"
		} else {
			ret += ",\n"
		}
	}
	return ret + body, nil
}

//
// explain runs explain on the XTable t, check if the sql is valid, and set
// the schema and estimated cost of xtable.
//
func (t *xtableCommon) Explain() error {
	sql, err := t.Sql()
	if err != nil {
		Log("=======EXPLAIN SQL ============================================")
		Log("SQL: %s", sql)
		Log("=======EXPLAIN SQL Error ======================================")
		LogErr(err, "Explain error.")
		Log("=======EXPALIN SQL End ========================================")
		return err
	}

	rows, err := t.dg.Conn.Query("explain verbose " + sql)
	if err != nil {
		LogErr(err, "Explain error.")
		return err
	}
	defer rows.Close()

	const (
		beforeCol = iota
		readingCol
		doneCol
		readPlan
		donePlan
		explainError
	)

	state := beforeCol
	var xtcost XTableCost
	var xtschema XTableSchema
	xtschema.Columns = make([]XColDef, 1)
	nextCol := 0
	var errstring string

	for rows.Next() {
		var rline string
		rows.Scan(&rline)

		line := strings.TrimSpace(rline)
		switch state {
		case beforeCol:
			if strings.HasPrefix(line, "ERROR:") {
				errstring = errstring + rline
				state = explainError
			} else if strings.HasPrefix(line, ":total_cost") {
				v := line[len(":total_cost")+1:]
				xtcost.TotalCost, _ = strconv.ParseFloat(v, 64)
			} else if strings.HasPrefix(line, ":plan_rows") {
				v := line[len(":plan_rows")+1:]
				xtcost.NRows, _ = strconv.ParseFloat(v, 64)
			} else if strings.HasPrefix(line, ":plan_width") {
				v := line[len(":plan_width")+1:]
				xtcost.RowWidth, _ = strconv.ParseFloat(v, 64)
			} else if strings.HasPrefix(line, ":targetlist") {
				state = readingCol
			}

		case readingCol:
			if strings.HasPrefix(line, ":vartype") {
				v := line[len(":vartype")+1:]
				oid, _ := strconv.Atoi(v)
				xtschema.Columns[nextCol].SQLType = t.dg.typMap[oid]
			} else if strings.HasPrefix(line, ":resname") {
				xtschema.Columns[nextCol].Name = line[len(":resname")+1:]
			} else if strings.HasPrefix(line, ":resjunk") {
				v := line[len(":resjunk")+1:]
				if v == "false" {
					var c XColDef
					xtschema.Columns = append(xtschema.Columns, c)
					nextCol++
				}
			} else if strings.HasPrefix(line, ":flow") {
				state = doneCol
			}
		case doneCol:
			if len(rline) >= 2 && rline[0] == ' ' && rline[1] != ' ' {
				state = readPlan
			}
		case readPlan:
			if rline[0] == ' ' && rline[1] != ' ' {
				state = donePlan
			} else {
				xtcost.PlanText = xtcost.PlanText + rline + "\n"
			}
		case explainError:
			errstring = errstring + rline
		}

	}

	if state == explainError {
		return fmt.Errorf(errstring)
	}

	xtschema.Columns = xtschema.Columns[:len(xtschema.Columns)-1]
	*(t.Schema()) = xtschema
	*(t.Cost()) = xtcost
	return nil
}

//
// ExecuteTable runs the XTable and return the execution results.
//
func (t *xtableCommon) Execute() (*sql.Rows, error) {
	q, err := t.Sql()
	if err != nil {
		return nil, err
	}
	return t.dg.Conn.Query(q)
}

func (xt *xtableCommon) RenderN(w io.Writer, n int) error {
	rows, err := xt.Execute()
	if err != nil {
		return err
	}
	defer rows.Close()

	tw := tablewriter.NewWriter(w)
	hdr := make([]string, len(xt.Schema().Columns))
	for idx, col := range xt.Schema().Columns {
		hdr[idx] = col.Name
	}
	tw.SetHeader(hdr)

	cnt := 0
	for rows.Next() {
		row := make([]interface{}, len(xt.Schema().Columns))
		data := make([]string, len(xt.Schema().Columns))
		for i := 0; i < len(row); i++ {
			row[i] = &data[i]
		}
		rows.Scan(row...)
		tw.Append(data)

		cnt++
		if cnt == n {
			break
		}
	}

	tw.Render()
	return nil
}

func (t *xtableCommon) RenderAll(w io.Writer) error {
	return t.RenderN(w, -1)
}

func MakeXTableSql(dg *Deepgreen, sql string, srct []XTable) (XTable, error) {
	var xt xtableCommon
	xt.dg = dg
	xt.alias = dg.nextTmpName()
	xt.inputs = srct
	xt.sql = sql

	err := xt.Explain()
	if err != nil {
		return nil, err
	}
	return &xt, nil
}

func MakeXTable(dg *Deepgreen, tbl string) (XTable, error) {
	return MakeXTableSql(dg, "select * from "+tbl, nil)
}
