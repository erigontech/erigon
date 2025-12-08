# Bumper tool

## Contents
- [Bump](#bump)
   - [If not a user then who?](#if-not-a-user-then-who)
   - [Purpose of this tool](#purpose-of-this-tool)
   - [Structure of bump](#structure-of-bump)
   - [How to](#how-to)
      - [TUI](#tui)
      - [CLI](#cli)
- [Rename](#rename)
   - [Totally last warning](#totally-last-warning)
   - [Purpose of this tool](#purpose-of-this-tool-1)
   - [How to](#how-to-1)
      - [Flags](#flags)
      - [TUI](#tui-1)
      - [CLI](#cli-1)
- [Algorythm](#algorythm)
- [FAQ](#faq)

Here's a short guide for bumper: (If you are **user** it's better **never** use this tool w/o confidence, you could mess up your erigon)
Anyway, if you've done something terrible and looking for troubleshooting:
`git reset --hard v3.1.x` (or another version.)

Bumper tool has two options of performing: Bump and Rename. Bellow I'll explain both of them.

## Bump
Tool for bumping versions of files in erigon codebase.
Here's a short guide for version bumper: (If you are **user** it's better **never** use this w/o confidence, you could mess up your erigon)
Anyway, if you've done something terrible and looking for troubleshooting:
`git reset --hard v3.1.x` (or another version.)
### If not a user then who?
Developers, devops and other folks that are interested in bumping snapshot version of erigon for some reason.
### Purpose of this tool
Provide simple tooling for devs to bump version of existing snapshots.
### Structure of bump
- CLI util (bumper itself) `bump.go`
- Version Schema Generator (inside e3) `version_gen.go`
- Version Schema (generated) `version_schema_gen.go`
- Version Schema yaml (could be modified w/o bumper tool) `versions.yaml`
### How to
There're two mods of bumper: `CLI` and `TUI`:
#### TUI
run tool with `go run ./cmd/bumper bump`
```
Schema Versions                                                                                                     
╭────────────────────╮  ╭──────────────────────────────────────────────────╮                                        
│Schemas             │  │accounts                                          │                                        
│ Schemas            │  │ Part      Key     Current   Min     Status       │                                        
│ accounts           │  │ domain    bt      1.1       1.0     ok           │                                        
│ code               │  │ domain    kv      1.1       1.0     ok           │                                        
│ commitment         │  │ domain    kvei    1.1       1.0     ok           │                                        
│ logaddrs           │  │ hist      v       1.1       1.0     ok           │                                        
│ logtopics          │  │ hist      vi      1.1       1.0     ok           │                                        
│ rcache             │  │ ii        ef      2.0       1.0     ok           │                                        
│ receipt            │  │ ii        efi     2.0       1.0     ok           │                                        
│ storage            │  │                                                  │                                        
│ tracesfrom         │  │                                                  │                                        
│ tracesto           │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  │                                                  │                                        
│                    │  ╰──────────────────────────────────────────────────╯                                        
│                    │                                                                                              
╰────────────────────╯                                                                                              
[↑/↓] move  [Tab] switch  [e] edit current  [m] edit min  [.] +0.1  [M] +1.0  [S] save  [Ctrl+S] save&exit  [Q] quit
versions.yaml • no changes • Ctrl+S=Save&Exit                           
```

1. Choose domain and extension of files which you want to bump (arrows and tab to navigate)
2. use `.` to perform minor bump (`1.2`->`1.3`) and `M` to major (`2.3`->`3.0`) Also, pay attention that there are Current and Minimal Supported version. If you want to edit version in your way, you can press `e` and made it whichever you want (`1.23` -> `15.2`)
3. `Ctrl+S` to save&exit

**NB!** In our project we have version guidelines, TL;DR:
1. bump the **minor** version if only content changes.
2. bump the **major** version if an old version of erigon can't read a new file.
   After save tool would regenerate files `version_schema_gen.go` and `versions.yaml`. So after it the flow is over, enjoy!

**P.S**
If you don't want to use the tool, you could edit `versions.yaml` after it exec `go run ./cmd/bumper bump` and press `q`
#### CLI
in development

## Rename
Tool for rename existing snapshots to align them with existing version schema. 
### Totally last warning
This could mess up your snapshot folder DO NOT USE IT w/o confidence (if you're not a dev/devops it's better to avoid it.)
### Purpose of this tool
Provide simple tooling for devs and devopses to align a version of all the files in the provided directory to the Schema 
in Erigon code.
### How to
This tool behaves much simpler than previous. All you need to do is to run `go run ./cmd/bumper rename --datadir /path/to/your/datadir`
and tick preferable domains and extensions to rename in simple TUI.
#### Flags
As the only arguments to our rename func are exts and domains there are some simple flags to help you tick right options.
1. `--datadir string`            Directory containing versioned files
2. `--exclude-domains strings`   Domains to exclude
3. `--exclude-exts strings`      Extensions to exclude
4. `-h, --help`                      help for rename
5. `--include-domains strings`   Domains to include (default: all)
6. `--include-exts strings`      Extensions to include (default: all)

#### TUI
```aiignore
  ←/→ to switch columns or OK/Cancel, ↑/↓ to move, enter/space to toggle, tab to confirm  
                                                                                          
>  [x] accounts                    [x] .kv
    [x] storage                    [x] .bt
    [x] code                       [x] .kvi
    [x] commitment                 [x] .kvei
    [x] receipt                    [x] .vi
    [x] rcache                     [x] .v
    [x] logaddr                    [x] .efi
    [x] logtopic                   [x] .ef
    [x] tracesfrom             
    [x] tracesto               

(Tab to switch to OK/Cancel)
```
Tbh, it's much simpler than bump TUI, so just tick preferable exts and domains -> tab -> Ok or Cancel (arrows and tab to navigate)
#### CLI
in development
## Algorythm
If you want to upgrade something:
1. write new logic for new versions of files
2. use bumper
3. generate snapshots from scratch
4. use renamer to ensure that you make snapshots follow your schema.

## FAQ
**Q:** I generated new files and forgot to increase a version — what to do?
**A:** Use renamer and choose there only files that you definitely want to rename.

**Q:** I modified templates (by refactoring) - how to re-gen code?
**A:** ?
