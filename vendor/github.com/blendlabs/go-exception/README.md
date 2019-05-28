go-exception
============

[![Build Status](https://travis-ci.org/blendlabs/go-exception.svg?branch=master)](https://travis-ci.org/blendlabs/go-exception)

This is a simple library for wrapping errors.Error with a stack trace.

##Sample Output

If we run `ex.Error()` on an Exception we will get a more detailed output than a normal `errorString`

```text
Exception: this is a sample error
       At: foo_controller.go:20 testExceptions()
           http.go:198 func1()
           http.go:213 func1()
           http.go:117 func1()
           router.go:299 ServeHTTP()
           server.go:1862 ServeHTTP()
           server.go:1361 serve()
           asm_amd64.s:1696 goexit()
```

##Usage

If we want to create a new exception we can use `New`

```go
	return exception.New("this is a test exception")
```

`New` will create a stack trace at the given line. It ignores stack frames within the `exception` package itself. 
There is also a convenience method `Newf` that will mimic Printf like functions.

If we want to wrap an existing golang `error` all we have to do is call `Wrap`

```go
	file, fileErr := os.ReadFile("my_file.txt")
	if fileErr != nil {
		return exception.Wrap(fileErr)
	}
```

A couple properties of wrap:
* It will return nil if the input error is nil
* It will not modify an error that is actually an exception, it will simply return (propagate) it.
* It will create a stack trace for the error if it is not nil, and assign the message from the existing error.

If we want to merge a couple exceptions together, i.e. we can use `WrapMany`

```go
if q.Rows != nil {
	if closeErr := q.Rows.Close(); closeErr != nil {
		return exception.WrapMany(q.Error, closeErr)
	}
}
```

`WrapMany` will "nest" the exceptions, and this will be reflected in the output.
