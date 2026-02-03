---
name: erigon-seg-rebase
description: Use the 'erigon seg step-rebase' command to change the step size of an existing datadir. Use when the user wants to rebase segments or modify step sizes.
allowed-tools: Bash, Read, Glob
---

# Erigon Segment Step Rebase

## Overview
The `erigon seg step-rebase` command changes the step size of an existing Erigon datadir. This is used to modify the granularity of snapshot segments.

## Command Syntax

```bash
./build/bin/erigon seg step-rebase --datadir=<path> --new-step-size=<size> [other-flags]
```

## Required Flags

- `--datadir`: Path to the Erigon datadir (required)
- `--new-step-size`: New step size to rebase to (default: 1562500)

## Common Step Sizes

- `1562500`: Default/full stepsize
- `781250`: stepsize/2 (half step)
- `390625`: stepsize/4 (quarter step)

## Usage Patterns

### Rebase to Half Step
```bash
./build/bin/erigon seg step-rebase --datadir=/path/to/datadir --new-step-size=781250
```

### Rebase to Quarter Step
```bash
./build/bin/erigon seg step-rebase --datadir=/path/to/datadir --new-step-size=390625
```

## Important Considerations

### Before Running
1. **Backup datadir**: Consider backing up the datadir before rebasing
2. **Stop Erigon**: Ensure Erigon is not running on the target datadir
3. **Verify current step**: Check what the current step size is
4. **Ensure Erigon binary is built**: run `make erigon` to build it

### After Running
1. Verify the rebase completed successfully
2. Check segment files in the datadir
3. Restart Erigon if needed

## Step Sizes

Common step sizes used in Erigon:
- `1562500`: Full/default stepsize
- `781250`: Half step (stepsize/2)
- `390625`: Quarter step (stepsize/4)

The step size affects:
- Snapshot segment granularity
- File size and organization
- Query and sync performance

## Workflow

When the user wants to rebase step size:

1. **Confirm parameters**
   - Ask for target datadir path
   - Ask for desired step size
   - Confirm if they want to rebase a specific range

2. **Safety checks**
   - Verify datadir exists
   - Check if Erigon is running (should not be)

3. **Execute rebase**
   ```bash
   ./build/bin/erigon seg step-rebase --datadir=/path/to/datadir --new-step-size=<size>
   ```

4. **Verify results**
   - Check command output for errors
   - List segment files to verify changes

## Error Handling

Common issues:
- **"datadir not found"**: Verify the path is correct
- **"database locked"**: Stop Erigon process first
- **Invalid step size**: Use valid step sizes (1562500, 781250, or 390625)

## Examples

### Example 1: Rebase datadir to half step
```bash
./build/bin/erigon seg step-rebase --datadir=./chaindata --new-step-size=781250
```

### Example 2: Rebase to quarter step
```bash
./build/bin/erigon seg step-rebase --datadir=/mnt/erigon/datadir --new-step-size=390625
```

## Tips

- The operation can be time-consuming for large datadirs
- Smaller step sizes (like 390625) create more granular segments
- If building from source, use `make erigon` to build the binary at `build/bin/erigon`
