package codegen

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"vitessedata/phi/datatype"
)

type ioSchema struct {
	ColNames []string
	ColTypes []datatype.OptionalType
	ColMap   map[string]int
}

func (s *ioSchema) genGoRec(rec string) string {
	// type In/OutRecord struct {
	ret := fmt.Sprintf("type %s struct {\n", rec)
	for i := 0; i < len(s.ColNames); i++ {
		ret += fmt.Sprintf("\t%s datatype.%s\n", s.ColNames[i], s.ColTypes[i].Names()[datatype.GoOptName])
	}
	ret += "}\n\n"

	// func (r *In/OutRecord) Get_col() (type, bool)
	for i := 0; i < len(s.ColNames); i++ {
		colname := s.ColNames[i]
		gotype := s.ColTypes[i].Names()[datatype.Name]

		ret += fmt.Sprintf("func (r *%s) Get_%s() (%s, bool) {\n", rec, colname, gotype)
		ret += fmt.Sprintf("\treturn r.%s.Get()\n", colname)
		ret += "}\n\n"

		ret += fmt.Sprintf("func (r *%s) Set_%s(v %s) {\n", rec, colname, gotype)
		ret += fmt.Sprintf("\tr.%s.Set(v)\n", colname)
		ret += "}\n\n"

		ret += fmt.Sprintf("func (r *%s) Set_%s_Null() {\n", rec, colname)
		ret += fmt.Sprintf("\tr.%s.SetNull()\n", colname)
		ret += "}\n\n"
	}

	return ret
}

func (s *ioSchema) parse(scanner *bufio.Scanner) error {
	s.ColMap = make(map[string]int)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "END INPUT TYPES") ||
			strings.Contains(line, "END OUTPUT TYPES") {
			return nil
		}

		fields := strings.Fields(line)
		nfield := len(fields)
		if nfield < 2 {
			return fmt.Errorf("Invalid column decl: %s", line)
		}

		t := datatype.MapType(fields[nfield-1])
		if t == nil {
			return fmt.Errorf("Unknown type: %s", fields[nfield-1])
		}

		n := fields[nfield-2]
		if s.ColMap[n] != 0 {
			return fmt.Errorf("Duplicate Col %s", n)
		}
		s.ColNames = append(s.ColNames, n)
		s.ColTypes = append(s.ColTypes, t)
		s.ColMap[n] = 1
	}
	return nil
}

func processSchema(fn string) (*ioSchema, *ioSchema, error) {
	file, err := os.Open(fn)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	var isch ioSchema
	var osch ioSchema

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "BEGIN INPUT TYPES") {
			err = isch.parse(scanner)
			if err != nil {
				return nil, nil, err
			}
		} else if strings.Contains(line, "BEGIN OUTPUT TYPES") {
			err = osch.parse(scanner)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if isch.ColNames == nil {
		return nil, nil, fmt.Errorf("Src file %s does not have input schema.", fn)
	}

	if osch.ColNames == nil {
		return nil, nil, fmt.Errorf("Src file %s does not have output schema.", fn)
	}
	return &isch, &osch, nil
}
