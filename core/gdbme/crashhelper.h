#ifndef CRASHHELPER_H
#define CRASHHELPER_H

#ifdef __cplusplus
extern "C" {
#endif

// Checks command-line arguments and, if not running under gdb,
// re-executes the process under gdb.
void check_and_restart(int argc, char *argv[]);

#ifdef __cplusplus
}
#endif

#endif  // CRASHHELPER_H
