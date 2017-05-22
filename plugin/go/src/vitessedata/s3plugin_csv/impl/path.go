package impl

import (
	"fmt"
	"unicode/utf8"
	"vitessedata/plugin"
)

// pathSimplePrefix returns a string that can be used as prefix search for S3.
// See golang path/filepath Match function for syntax we support.
func pathSimplePrefix(path string) string {
	rs := make([]rune, 0)
	leading := true
	for _, r := range path {
		// skip leading /
		if leading && r == '/' {
			continue
		}
		leading = false

		// we will bomb out whenever we see any strange char that matters
		// to the matching alogrith.
		//
		//Note the following,
		// # 0-9, -, even they matter to pattern, but they are valid.
		// # / is valid.
		// # whitespaces are valid.
		// # some chars, ^, ], (, ), {, }, even should be ok if just looking
		//	  at Match spec, but let's call them out anyway.
		// # I don't know about other strange things like ' and ", using them
		//	  in pathname are asking for trouble anyway so whoever did that
		//	  deserve whatever the result is.
		switch r {
		case '*', '?', '[', '^', ']', '(', ')', '{', '}', '\\':
			return string(rs)
		default:
			rs = append(rs, r)
		}
	}
	return string(rs)
}

func pathIsDir(s string) bool {
	r, sz := utf8.DecodeLastRuneInString(s)
	if sz == 0 {
		// empty string, should never happen.
		plugin.FatalIfErr(fmt.Errorf("S3 Error"), "S3 Object has no name.")
	}
	return r == '/'
}
