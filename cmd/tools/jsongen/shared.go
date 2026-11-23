// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"go/types"
	"strings"
)

// member is a shared field as its group encodes it: appendFmt appends the value, given as the
// operand of %s, to b, and size bounds what that appends.
type member struct {
	ref, name string
	pointer   bool
	omitted   bool   // a nil pointer leaves the field out rather than writing null
	present   string // for a value field, the condition omitempty writes it under; "" for always
	appendFmt string
	size      int
}

// quantityLen is a quoted 0x-prefixed 64-bit quantity at its longest.
const quantityLen = len(`"0x"`) + 16

// sharedMember accepts the forms whose encoding has a fixed upper bound, so a group fits in a
// buffer the generated code declares, and whose values compare with ==.
func sharedMember(ref, name, form string, t types.Type, omit bool) (*member, error) {
	_, pointer := t.Underlying().(*types.Pointer)
	m := &member{ref: ref, name: name, pointer: pointer, omitted: pointer && omit}
	bare := deref(t)
	switch form {
	case "bool":
		if pointer {
			return nil, fmt.Errorf("a shared bool cannot be a pointer")
		}
		m.appendFmt, m.size = "strconv.AppendBool(b, %s)", len("false")
		if omit {
			m.present = ref
		}
	case "quantity":
		b, ok := bare.Underlying().(*types.Basic)
		if !ok || b.Info()&types.IsUnsigned == 0 {
			return nil, fmt.Errorf("a shared quantity must be an unsigned integer, not %s", bare)
		}
		m.appendFmt, m.size = `append(hexutil.AppendUint64(append(b, '"'), uint64(%s)), '"')`, quantityLen
		if omit && !pointer {
			m.present = ref + " != 0"
		}
	case "data":
		a, ok := t.Underlying().(*types.Array)
		if !ok {
			return nil, fmt.Errorf("shared data must be a fixed-size array, not %s", t)
		}
		m.appendFmt, m.size = "hexutil.AppendQuoted(b, %s[:])", len(`"0x"`)+2*int(a.Len())
		if omit {
			m.present = fmt.Sprintf("len(%s) > 0", ref)
		}
	default:
		return nil, fmt.Errorf("ethjson:%q cannot be shared", form)
	}
	if pointer && !omit {
		m.size = max(m.size, len("null"))
	}
	return m, nil
}

// alwaysWritten reports a member that puts its name in every encoding of its group.
func (m *member) alwaysWritten() bool { return !m.omitted && m.present == "" }

// group is a run of consecutive shared fields, which the slice method writes as one block.
type group []*member

// mayBeEmpty reports a group whose every member may be left out, so its block may be empty.
func (g group) mayBeEmpty() bool {
	for _, m := range g {
		if m.alwaysWritten() {
			return false
		}
	}
	return true
}

// bufLen bounds the block: each member's name and value, and a comma between members.
func (g group) bufLen() int {
	n := len(g) - 1
	for _, m := range g {
		n += len(`"":`) + len(m.name) + m.size
	}
	return n
}

func checkSlice(scope *types.Scope, slice string, elem types.Type) error {
	obj := scope.Lookup(slice)
	if obj == nil {
		return fmt.Errorf("no %s in the package", slice)
	}
	if s, ok := obj.Type().Underlying().(*types.Slice); ok {
		if p, ok := s.Elem().(*types.Pointer); ok && types.Identical(p.Elem(), elem) {
			return nil
		}
	}
	return fmt.Errorf("%s is not a slice of *%s", slice, elem)
}

// writeSliceMethod writes the slice's MarshalFastJSONTo, and the cache of encoded groups it
// keeps for the length of one call.
func writeSliceMethod(w *bytes.Buffer, slice, elem string, fields []field) {
	cache := strings.ToLower(slice[:1]) + slice[1:] + "Shared"
	var loop strings.Builder
	var groups []group
	for i, f := range fields {
		if f.shared == nil {
			loop.WriteString(f.stmt)
			continue
		}
		if i > 0 && fields[i-1].shared != nil {
			groups[len(groups)-1] = append(groups[len(groups)-1], f.shared)
			continue
		}
		groups = append(groups, group{f.shared})
		if !groupFrom(fields[i:]).mayBeEmpty() {
			fmt.Fprintf(&loop, "\ts.RawFields(shared.group%d(x))\n", len(groups)-1)
		} else {
			fmt.Fprintf(&loop, "\tif b := shared.group%d(x); len(b) > 0 {\n\t\ts.RawFields(b)\n\t}\n", len(groups)-1)
		}
	}

	fmt.Fprintf(w, `
// %[1]s writes the elements as a bare array, each as %[3]s.%[1]s does.
func (xs %[2]s) %[1]s(s *jsonstream.Stream) error {
	if xs == nil {
		s.WriteNil()
		return nil
	}
`, method, slice, elem)
	if len(groups) > 0 {
		fmt.Fprintf(w, "\tvar shared %s\n", cache)
	}
	fmt.Fprintf(w, `	s.WriteArrayStart()
	for _, x := range xs {
		if x == nil {
			s.WriteNil()
			continue
		}
		s.WriteObjectStart()
%s		s.WriteObjectEnd()
	}
	s.WriteArrayEnd()
	return nil
}
`, loop.String())
	if len(groups) == 0 {
		return
	}

	fmt.Fprintf(w, "\n// %s holds each group of shared %s fields as last encoded, and the element it was encoded from.\ntype %s struct {\n", cache, elem, cache)
	for i, g := range groups {
		fmt.Fprintf(w, "\tok%[1]d bool\n\tkey%[1]d %[2]s\n\tlen%[1]d int\n\tbuf%[1]d [%[3]d]byte\n", i, elem, g.bufLen())
	}
	w.WriteString("}\n")
	for i, g := range groups {
		writeGroupMethod(w, cache, elem, i, g)
	}
}

// groupFrom is the run of shared fields that starts fields.
func groupFrom(fields []field) group {
	var g group
	for _, f := range fields {
		if f.shared == nil {
			break
		}
		g = append(g, f.shared)
	}
	return g
}

func writeGroupMethod(w *bytes.Buffer, cache, elem string, i int, g group) {
	key := fmt.Sprintf("c.key%d", i)
	var same, assign, encode strings.Builder
	names := make([]string, len(g))
	certain := false // whether b holds a member already, so the next needs a comma
	for j, m := range g {
		names[j] = m.name
		k := key + strings.TrimPrefix(m.ref, "x")
		same.WriteString(" &&\n\t\t")
		if m.pointer {
			// Through the pointers: two elements may hold the same value at different addresses.
			fmt.Fprintf(&same, "(%[1]s == nil) == (%[2]s == nil) && (%[1]s == nil || *%[1]s == *%[2]s)", m.ref, k)
		} else {
			fmt.Fprintf(&same, "%s == %s", m.ref, k)
		}
		fmt.Fprintf(&assign, "\t%s = %s\n", k, m.ref)

		label := fmt.Sprintf("%q:", m.name)
		var prefix string
		switch {
		case j == 0:
		case certain:
			label = "," + label
		default:
			prefix = "if len(b) > 0 {\nb = append(b, ',')\n}\n"
		}
		value := m.ref
		if m.pointer {
			value = "*" + m.ref
		}
		write := fmt.Sprintf("%sb = append(b, %q...)\nb = %s\n", prefix, label, fmt.Sprintf(m.appendFmt, value))
		switch {
		case m.omitted:
			fmt.Fprintf(&encode, "if %s != nil {\n%s}\n", m.ref, write)
		case m.pointer:
			fmt.Fprintf(&encode, "if %s == nil {\n%sb = append(b, %q...)\n} else {\n%s}\n", m.ref, prefix, label+"null", write)
		case m.present != "":
			fmt.Fprintf(&encode, "if %s {\n%s}\n", m.present, write)
		default:
			encode.WriteString(write)
		}
		certain = certain || m.alwaysWritten()
	}

	fmt.Fprintf(w, `
// group%[1]d returns %[5]s of x, encoded again only when they differ from the previous element's.
func (c *%[2]s) group%[1]d(x *%[3]s) []byte {
	if c.ok%[1]d%[4]s {
		return c.buf%[1]d[:c.len%[1]d]
	}
	c.ok%[1]d = true
%[6]s	b := c.buf%[1]d[:0]
%[7]s	c.len%[1]d = len(b)
	return b
}
`, i, cache, elem, same.String(), strings.Join(names, ", "), assign.String(), encode.String())
}
