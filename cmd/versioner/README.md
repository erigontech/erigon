# Versioner tool

Here's a short guide for versioner: (If you are **user** it's better **never** use this tool w/o confidence, you could mess up your erigon)
Anyway, if you've done something terrible and looking for troubleshooting:
`git reset --hard v3.1.x` (or another version.)

## Bumper
Tool for bumping versions of files in erigon codebase.
[Bumper](docs/Bumper.md)

## Renamer
Tool for rename existing snapshots to align them with existing version schema. 
### Totally last warning
This could mess up your snapshot folder DO NOT USE IT w/o confidence (if you're not a dev/devops it's better to avoid it.)
[Renamer](docs/Renamer.md)

## Algorythm
If you want to upgrade something:
1. write new logic for new versions of files
2. use bumper
3. generate snapshots from scratch
4. use renamer to ensure that you make snapshots follow your schema.