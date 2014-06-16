package datatable

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func copyMap(v map[string]interface{}) map[string]interface{} {
	var network bytes.Buffer        // Stand-in for a network connection
	enc := gob.NewEncoder(&network) // Will write to network.
	dec := gob.NewDecoder(&network)
	err := enc.Encode(v)
	if err != nil {
		panic(err)
	}

	q := map[string]interface{}{}
	err = dec.Decode(&q)
	if err != nil {
		panic(err)
	}
	return q
}
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
