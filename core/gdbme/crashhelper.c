#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "crashhelper.h"

#define GDB_PATH "/usr/bin/gdb"  // Adjust this if gdb is in a different location

// Returns non-zero if the some of arguments equals our marker flag.
int has_gdb_flag(int argc, char *argv[]) {
    if (argc < 1) {
        return 0;
    }
    int gdb_flag = 0;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--gdbme") == 0) {
            gdb_flag = 1;
        }
    }

    return gdb_flag;
}

// Restart the process under gdb with options that run the program
// and print a full backtrace on a crash.
void restart_under_gdb(int argc, char *argv[]) {
    const char *gdb_options[] = {
        GDB_PATH,
        "-q",             // Quiet startup
        "-batch",         // Run in batch mode (non-interactive)
        "-nx",            // Do not read any gdb init files
        "-nh",            // Do not execute commands from .gdbinit
        "-return-child-result",  // Return the program's exit status
        "-ex", "run",     // Run the program
        "-ex", "bt full", // Print a full backtrace with local variables on crash
        "--args"          // All subsequent arguments are passed to the program
    };
    int num_gdb_opts = sizeof(gdb_options) / sizeof(gdb_options[0]);

    // Calculate the new argument count: gdb options + original args + marker flag.
    int new_argc = num_gdb_opts + argc + 1;
    char **new_argv = malloc((new_argc + 1) * sizeof(char *));
    if (!new_argv) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    int pos = 0;

    for (int i = 0; i < num_gdb_opts; i++) {
        new_argv[pos++] = (char *)gdb_options[i];
    }

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--gdbme") != 0) {
            new_argv[pos++] = argv[i];
        }
    }
    new_argv[pos] = NULL;  // Null-terminate the array

    fprintf(stderr, "Restarting under gdb for enhanced crash diagnostics...\n");
    execvp(new_argv[0], new_argv);
    perror("execvp");
    exit(EXIT_FAILURE);
}

void check_and_restart(int argc, char *argv[]) {
    if (has_gdb_flag(argc, argv)) {
        restart_under_gdb(argc, argv);
    }
}
