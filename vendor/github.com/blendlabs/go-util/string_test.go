package util

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestRandomString(t *testing.T) {
	assert := assert.New(t)
	str := String.RandomString(10)
	assert.Equal(len(str), 10)
}

func TestCaseInsensitiveEquals(t *testing.T) {
	assert := assert.New(t)
	assert.True(String.CaseInsensitiveEquals("foo", "FOO"))
	assert.True(String.CaseInsensitiveEquals("foo123", "FOO123"))
	assert.True(String.CaseInsensitiveEquals("!foo123", "!foo123"))
	assert.False(String.CaseInsensitiveEquals("foo", "bar"))
}

func TestRegexMatch(t *testing.T) {
	assert := assert.New(t)

	result := String.RegexMatch("a", "b")
	assert.Equal("", result)
}

func TestParse(t *testing.T) {
	assert := assert.New(t)

	good := Parse.Float64("3.14")
	bad := Parse.Float64("I Am Dog")
	assert.Equal(3.14, good)
	assert.Equal(0.0, bad)

	good32 := Parse.Float32("3.14")
	bad32 := Parse.Float32("I Am Dog")
	assert.Equal(3.14, good32)
	assert.Equal(0.0, bad32)

	goodInt := Parse.Int("3")
	badInt := Parse.Int("I Am Dog")
	assert.Equal(3, goodInt)
	assert.Equal(0.0, badInt)

	strGood := String.Float64(3.14)
	assert.Equal("3.14", strGood)

	strGood = String.Int(3)
	assert.Equal("3", strGood)
}

func TestTrimWhitespace(t *testing.T) {
	assert := assert.New(t)

	tests := []KeyValuePairOfString{
		{"test", "test"},
		{" test", "test"},
		{"test ", "test"},
		{" test ", "test"},
		{"\ttest", "test"},
		{"test\t", "test"},
		{"\ttest\t", "test"},
		{" \ttest\t ", "test"},
		{" \ttest\n\t ", "test"},
	}

	for _, test := range tests {
		result := String.TrimWhitespace(test.Key)
		assert.Equal(test.Value, result)
	}
}

func TestIsCamelCase(t *testing.T) {
	assert := assert.New(t)

	assert.True(String.IsCamelCase("McDonald"))
	assert.True(String.IsCamelCase("mcDonald"))
	assert.False(String.IsCamelCase("mcdonald"))
	assert.False(String.IsCamelCase("MCDONALD"))
}

func TestIsUpper(t *testing.T) {
	assert := assert.New(t)
	assert.True(String.IsUpper(rune('A')))
	assert.True(String.IsUpper(rune('I')))
	assert.True(String.IsUpper(rune('Z')))

	assert.False(String.IsUpper(rune('a')))
	assert.False(String.IsUpper(rune('i')))
	assert.False(String.IsUpper(rune('z')))

	assert.False(String.IsUpper(rune(' ')))
	assert.False(String.IsUpper(rune('0')))
}

func TestIsLower(t *testing.T) {
	assert := assert.New(t)
	assert.False(String.IsLower(rune('A')))
	assert.False(String.IsLower(rune('I')))
	assert.False(String.IsLower(rune('Z')))

	assert.True(String.IsLower(rune('a')))
	assert.True(String.IsLower(rune('i')))
	assert.True(String.IsLower(rune('z')))

	assert.False(String.IsLower(rune(' ')))
	assert.False(String.IsLower(rune('0')))
}

func TestIsSymbol(t *testing.T) {
	assert := assert.New(t)
	assert.False(String.IsSymbol(rune('A')))
	assert.False(String.IsSymbol(rune('I')))
	assert.False(String.IsSymbol(rune('Z')))

	assert.False(String.IsSymbol(rune('a')))
	assert.False(String.IsSymbol(rune('i')))
	assert.False(String.IsSymbol(rune('z')))

	assert.True(String.IsSymbol(rune(' ')))
	assert.True(String.IsSymbol(rune('#')))
	assert.True(String.IsSymbol(rune('/')))
	assert.False(String.IsSymbol(rune('0')))
}

func TestIsNumber(t *testing.T) {
	assert := assert.New(t)
	assert.False(String.IsNumber("A"))
	assert.False(String.IsNumber("ABC"))
	assert.False(String.IsNumber("A123B"))

	assert.False(String.IsNumber("abccc"))
	assert.False(String.IsNumber("123abccc"))
	assert.False(String.IsNumber("abc&&cc"))

	assert.False(String.IsNumber(" "))
	assert.False(String.IsNumber("#"))
	assert.False(String.IsNumber("1/31fafa"))

	assert.True(String.IsNumber("0"))
	assert.True(String.IsNumber("100.9843"))
	assert.True(String.IsNumber("-199"))
	assert.True(String.IsNumber("-2.1234"))
	assert.True(String.IsNumber("100.3333E3"))
}

func TestCombinePathComponents(t *testing.T) {
	assert := assert.New(t)

	value := String.CombinePathComponents("foo")
	assert.Equal("foo", value)

	value = String.CombinePathComponents("/foo")
	assert.Equal("foo", value)

	value = String.CombinePathComponents("foo/")
	assert.Equal("foo", value)

	value = String.CombinePathComponents("/foo/")
	assert.Equal("foo", value)

	value = String.CombinePathComponents("foo", "bar")
	assert.Equal("foo/bar", value)

	value = String.CombinePathComponents("foo/", "bar")
	assert.Equal("foo/bar", value)

	value = String.CombinePathComponents("foo/", "/bar")
	assert.Equal("foo/bar", value)

	value = String.CombinePathComponents("/foo/", "/bar")
	assert.Equal("foo/bar", value)

	value = String.CombinePathComponents("/foo/", "/bar/")
	assert.Equal("foo/bar", value)

	value = String.CombinePathComponents("foo", "bar", "baz")
	assert.Equal("foo/bar/baz", value)

	value = String.CombinePathComponents("foo/", "bar/", "baz")
	assert.Equal("foo/bar/baz", value)

	value = String.CombinePathComponents("foo/", "bar/", "baz/")
	assert.Equal("foo/bar/baz", value)

	value = String.CombinePathComponents("foo/", "/bar/", "/baz")
	assert.Equal("foo/bar/baz", value)

	value = String.CombinePathComponents("/foo/", "/bar/", "/baz")
	assert.Equal("foo/bar/baz", value)

	value = String.CombinePathComponents("/foo/", "/bar/", "/baz/")
	assert.Equal("foo/bar/baz", value)
}

func TestRegexExtractSubMatches(t *testing.T) {
	assert := assert.New(t)

	corpus := "/accounts/1234/connections/4321/foo/bar"
	regex := `/accounts/\d+/connections/(\d+)/.*`

	matches := String.RegexExtractSubMatches(corpus, regex)
	assert.Len(matches, 2)
	assert.Equal("4321", matches[1])
}

func TestHasPrefixCaseInsensitive(t *testing.T) {
	assert := assert.New(t)

	assert.True(String.HasPrefixCaseInsensitive("hello world!", "hello"))
	assert.True(String.HasPrefixCaseInsensitive("hello world", "hello world"))
	assert.True(String.HasPrefixCaseInsensitive("HELLO world", "hello"))
	assert.True(String.HasPrefixCaseInsensitive("hello world", "HELLO"))
	assert.True(String.HasPrefixCaseInsensitive("hello world", "h"))

	assert.False(String.HasPrefixCaseInsensitive("hello world", "butters"))
	assert.False(String.HasPrefixCaseInsensitive("hello world", "hello world boy is this long"))
	assert.False(String.HasPrefixCaseInsensitive("hello world", "world")) //this would pass suffix
}

func TestHasSuffixCaseInsensitive(t *testing.T) {
	assert := assert.New(t)

	assert.True(String.HasSuffixCaseInsensitive("hello world!", "world!"))
	assert.True(String.HasSuffixCaseInsensitive("hello world", "d"))
	assert.True(String.HasSuffixCaseInsensitive("hello world", "hello world"))

	assert.True(String.HasSuffixCaseInsensitive("hello WORLD", "world"))
	assert.True(String.HasSuffixCaseInsensitive("hello world", "WORLD"))

	assert.False(String.HasSuffixCaseInsensitive("hello world", "hello hello world"))
	assert.False(String.HasSuffixCaseInsensitive("hello world", "foobar"))
	assert.False(String.HasSuffixCaseInsensitive("hello world", "hello")) //this would pass prefix
}

func TestStringToTitleCase(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("123456", String.ToTitleCase("123456"))
	assert.Equal("Test", String.ToTitleCase("test"))
	assert.Equal("Test", String.ToTitleCase("TEST"))
	assert.Equal("Test", String.ToTitleCase("Test"))
	assert.Equal("Test Strings", String.ToTitleCase("test strings"))
	assert.Equal("Test_Strings", String.ToTitleCase("test_strings"))
	assert.Equal("Test_Strings", String.ToTitleCase("TEST_STRINGS"))
}

func TestStringFixedWidth(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("   abc", String.FixedWidth("abc", 6))
	assert.Equal("a", String.FixedWidth("abc", 1))
}

func TestStringFixedWidthLeftAligned(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("abc   ", String.FixedWidthLeftAligned("abc", 6))
	assert.Equal("a", String.FixedWidthLeftAligned("abc", 1))
}

func TestTrimPrefixCaseInsensitive(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("def", String.TrimPrefixCaseInsensitive("abcdef", "abc"))
	assert.Equal("def", String.TrimPrefixCaseInsensitive("abcdef", "ABC"))
	assert.Equal("DEF", String.TrimPrefixCaseInsensitive("abcDEF", "abc"))
	assert.Equal("abcdef", String.TrimPrefixCaseInsensitive("abcdef", "foo"))
	assert.Equal("abc", String.TrimPrefixCaseInsensitive("abc", "abcdef"))
}

func TestTrimSuffixCaseInsensitive(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("abc", String.TrimSuffixCaseInsensitive("abcdef", "def"))
	assert.Equal("ab2", String.TrimSuffixCaseInsensitive("ab2def", "DEF"))
	assert.Equal("ab3", String.TrimSuffixCaseInsensitive("ab3DEF", "def"))
	assert.Equal("abcdef", String.TrimSuffixCaseInsensitive("abcdef", "foo"))
	assert.Equal("abc", String.TrimSuffixCaseInsensitive("abc", "abcdef"))
}

func TestSplitOnSpace(t *testing.T) {
	assert := assert.New(t)

	values := String.SplitOnSpace("")
	assert.Len(values, 0)

	values = String.SplitOnSpace("foo")
	assert.Len(values, 1)
	assert.Equal("foo", values[0])

	values = String.SplitOnSpace("foo bar")
	assert.Len(values, 2)
	assert.Equal("foo", values[0])
	assert.Equal("bar", values[1])

	values = String.SplitOnSpace("foo  bar")
	assert.Len(values, 2)
	assert.Equal("foo", values[0])
	assert.Equal("bar", values[1])

	values = String.SplitOnSpace("foo\tbar")
	assert.Len(values, 2)
	assert.Equal("foo", values[0])
	assert.Equal("bar", values[1])

	values = String.SplitOnSpace("foo \tbar")
	assert.Len(values, 2)
	assert.Equal("foo", values[0])
	assert.Equal("bar", values[1])

	values = String.SplitOnSpace("foo bar  ")
	assert.Len(values, 2)
	assert.Equal("foo", values[0])
	assert.Equal("bar", values[1])

	values = String.SplitOnSpace("foo bar baz")
	assert.Len(values, 3)
	assert.Equal("foo", values[0])
	assert.Equal("bar", values[1])
	assert.Equal("baz", values[2])
}
