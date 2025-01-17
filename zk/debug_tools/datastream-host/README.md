# Datastream Server

This Go program starts a datastream server using the provided configuration.

## How to run

Replace `<path_to_datastream_file>` with the path to your datastream file (a file to be used by the software, consider it an output).

see an example of it creating the file, you can run the command without the flag for the file to be created.
`{"level":"info","ts":1736977411.8386219,"caller":"datastreamer/streamfile.go:125","msg":"Creating new file for datastream: .bin","pid":56764,"version":"v0.1.0"}`

```sh
go run zk/debug_tools/datastream-host/main.go -file <path_to_datastream_file>
```

