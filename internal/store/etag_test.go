package store

import "testing"

func TestNormalizeETag(t *testing.T) {
	const sha = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	cases := []struct {
		name, in, want string
	}{
		{"plain", "abc123", "abc123"},
		{"quoted", `"abc123"`, "abc123"},
		{"weak quoted", `W/"abc123"`, "abc123"},
		{"weak lower-case prefix", `w/"abc123"`, "abc123"},
		{"weak unquoted", "W/abc123", "abc123"},
		{"upper-case hex", `"ABC123"`, "abc123"},
		{"weak upper-case hex", `W/"ABC123"`, "abc123"},
		{"multipart", `"d41d8cd98f00b204e9800998ecf8427e-3"`, "d41d8cd98f00b204e9800998ecf8427e-3"},
		{"surrounding whitespace", `  "abc123" ` + "\n", "abc123"},
		{"empty", "", ""},
		{"lone quote", `"`, `"`},
		{"only one pair of quotes stripped", `""abc123""`, `"abc123"`},
		{"local format unchanged", sha + " 1234", sha + " 1234"},
		{"local format only lower-cased", "9F86D081884C7D659A2FEAA0C55AD015A3BF4F1B2B0B822CD15D6C15B0F00A08 1234", sha + " 1234"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := NormalizeETag(c.in); got != c.want {
				t.Fatalf("NormalizeETag(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestNormalizeETagAgreesAcrossReformats pins the property the ops layer
// relies on: every reformatting a provider may apply between a PUT and a
// HEAD of the same object normalizes to the same string, and two
// different etags still do not.
func TestNormalizeETagAgreesAcrossReformats(t *testing.T) {
	forms := []string{`"abc123"`, "abc123", `W/"abc123"`, `w/"ABC123"`, `"ABC123"`, " abc123 "}
	want := NormalizeETag(forms[0])
	for _, f := range forms[1:] {
		if got := NormalizeETag(f); got != want {
			t.Fatalf("NormalizeETag(%q) = %q, want %q (as %q)", f, got, want, forms[0])
		}
	}
	if NormalizeETag(`"abc123"`) == NormalizeETag(`"abc124"`) {
		t.Fatal("different etags normalized to the same string")
	}
}
