package datatable

import (
	"fmt"
)

func safeToStrings(s ...interface{}) []string {
	r := make([]string, len(s), len(s))
	for i, v := range s {
		r[i] = safeToString(v)
	}
	return r
}
func safeToString(s interface{}) string {

	if s == nil {
		return ""
	}
	switch r := s.(type) {
	case string:
		return r
	case []byte:
		return string(r)
	default:
		return fmt.Sprintf("%v", s)
	}

}
