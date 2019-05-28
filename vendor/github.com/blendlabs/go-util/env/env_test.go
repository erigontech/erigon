package env

import (
	"testing"
	"time"

	assert "github.com/blendlabs/go-assert"
	util "github.com/blendlabs/go-util"
)

func TestNewVarsFromEnvironment(t *testing.T) {
	assert := assert.New(t)
	assert.NotNil(NewVarsFromEnvironment())
}

func TestVarsSet(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"Foo": "baz",
	}

	vars.Set("Foo", "bar")
	assert.Equal("bar", vars.String("Foo"))

	vars.Set("NotFoo", "buzz")
	assert.Equal("buzz", vars.String("NotFoo"))
}

func TestEnvBool(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"true":  "true",
		"1":     "1",
		"yes":   "yes",
		"false": "false",
	}

	assert.True(vars.Bool("true"))
	assert.True(vars.Bool("1"))
	assert.True(vars.Bool("yes"))
	assert.False(vars.Bool("false"))
	assert.False(vars.Bool("no"))

	// Test Set False
	assert.False(vars.Bool("false"))

	// Test Unset Default
	assert.False(vars.Bool("0"))

	// Test Unset User Default
	assert.True(vars.Bool("0", true))
}

func TestEnvInt(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"One": "1",
		"Two": "2",
		"Foo": "Bar",
	}

	assert.Equal(1, vars.MustInt("One"))
	assert.Equal(2, vars.MustInt("Two"))
	_, err := vars.Int("Foo")
	assert.NotNil(err)
	assert.Zero(vars.MustInt("Baz"))
	assert.Equal(4, vars.MustInt("Baz", 4))
}

func TestEnvInt64(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"One": "1",
		"Two": "2",
		"Foo": "Bar",
	}

	assert.Equal(1, vars.MustInt64("One"))
	assert.Equal(2, vars.MustInt64("Two"))
	_, err := vars.Int64("Foo")
	assert.NotNil(err)
	assert.Zero(vars.MustInt64("Baz"))
	assert.Equal(4, vars.MustInt64("Baz", 4))
}

func TestEnvBytes(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"Foo": "abcdef",
	}

	assert.Equal("abcdef", string(vars.Bytes("Foo")))
	assert.Nil(vars.Bytes("NotFoo"))
	assert.Equal("Bar", string(vars.Bytes("NotFoo", []byte("Bar"))))
}

func TestEnvBase64(t *testing.T) {
	assert := assert.New(t)

	testValue := util.Base64.Encode([]byte("this is a test"))
	vars := Vars{
		"Foo": string(testValue),
		"Bar": "not_base64",
	}

	res, err := vars.Base64("Foo")
	assert.Nil(err)
	assert.Equal("this is a test", string(res))

	res, err = vars.Base64("Bar")
	assert.NotNil(err)
	assert.Empty(res)
}

func TestEnvHasKey(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"test1": "foo",
		"test2": "bar",
		"test3": "baz",
		"test4": "buzz",
	}

	assert.True(vars.Has("test1"))
	assert.False(vars.Has("notTest1"))
}

func TestEnvHasAnyKeys(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"test1": "foo",
		"test2": "bar",
		"test3": "baz",
		"test4": "buzz",
	}

	assert.True(vars.HasAny("test1"))
	assert.True(vars.HasAny("test1", "test2", "test3", "test4"))
	assert.True(vars.HasAny("test1", "test2", "test3", "notTest4"))
	assert.False(vars.HasAny("notTest1", "notTest2"))
	assert.False(vars.HasAny())
}

func TestEnvHasAllKeys(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"test1": "foo",
		"test2": "bar",
		"test3": "baz",
		"test4": "buzz",
	}

	assert.True(vars.HasAll("test1"))
	assert.True(vars.HasAll("test1", "test2", "test3", "test4"))
	assert.False(vars.HasAll("test1", "test2", "test3", "notTest4"))
	assert.False(vars.HasAll())
}

func TestVarsKeys(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"test1": "foo",
		"test2": "bar",
		"test3": "baz",
		"test4": "buzz",
	}

	keys := vars.Vars()
	assert.Len(keys, 4)
	assert.Any(keys, func(v interface{}) bool { return v.(string) == "test1" })
	assert.Any(keys, func(v interface{}) bool { return v.(string) == "test2" })
	assert.Any(keys, func(v interface{}) bool { return v.(string) == "test3" })
	assert.Any(keys, func(v interface{}) bool { return v.(string) == "test4" })
}

func TestEnvUnion(t *testing.T) {
	assert := assert.New(t)

	vars1 := Vars{
		"test3": "baz",
		"test4": "buzz",
	}

	vars2 := Vars{
		"test1": "foo",
		"test2": "bar",
	}

	union := vars1.Union(vars2)

	assert.Len(union, 4)
	assert.True(union.HasAll("test1", "test3"))
}

type readInto struct {
	Test1 string        `env:"test1"`
	Test2 int           `env:"test2"`
	Test3 float64       `env:"test3"`
	Dur   time.Duration `env:"dur"`
	Sub   readIntoSub
}

type readIntoSub struct {
	Test4 string   `env:"test4"`
	Test5 []string `env:"test5,csv"`
	Test6 []byte   `env:"test6,base64"`
	Test7 []byte   `env:"test7,bytes"`
	Test8 *bool    `env:"test8"`
}

func TestEnvReadInto(t *testing.T) {
	assert := assert.New(t)

	vars1 := Vars{
		"test1": "foo",
		"test2": "1",
		"test3": "2.0",
		"test4": "bar",
		"dur":   "4s",
		"test5": "bar0,bar1,bar2",
		"test6": string(util.Base64.Encode([]byte("base64encoded"))),
		"test7": "alsoBytes",
		"test8": "true",
	}

	var obj readInto
	err := vars1.ReadInto(&obj)
	assert.Nil(err)
	assert.Equal("foo", obj.Test1)
	assert.Equal(1, obj.Test2)
	assert.Equal(2.0, obj.Test3)
	assert.Equal(4*time.Second, obj.Dur)
	assert.Equal("bar", obj.Sub.Test4)
	assert.NotEmpty(obj.Sub.Test5)
	assert.Equal("bar0", obj.Sub.Test5[0])
	assert.Equal("bar1", obj.Sub.Test5[1])
	assert.Equal("bar2", obj.Sub.Test5[2])
	assert.NotEmpty(obj.Sub.Test6)
	assert.NotEmpty(obj.Sub.Test7)
}

func TestEnvDelete(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"test": "foo",
		"bar":  "baz",
	}
	assert.True(vars.Has("test"))
	vars.Delete("test")
	assert.False(vars.Has("test"))
	assert.True(vars.Has("bar"))
}

func TestEnvCSV(t *testing.T) {
	assert := assert.New(t)

	vars := Vars{
		"foo": "a,b,c",
		"bar": "",
	}

	assert.Equal([]string{"a", "b", "c"}, vars.CSV("foo"))
	assert.Equal([]string{"a", "b"}, vars.CSV("bar", "a", "b"))
	assert.Equal([]string{"a", "b"}, vars.CSV("baz", "a", "b"))
	assert.Nil(vars.CSV("baz"))
}
