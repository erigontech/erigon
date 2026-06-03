// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

//go:build race && darwin

// Package race pre-maps TSAN shadow for the Go heap address window on
// darwin. The kernel can place large file mappings (mdbx, snapshots)
// between Go heap arenas: the runtime's racecalladdr filter accepts such
// addresses (it checks one coarse [racearenastart, racearenaend) interval)
// but shadow is mapped per arena only, so the first instrumented read
// faults inside __tsan_read ("fatal error: runtime: split stack overflow").
// TSAN's own __tsan_map_shadow cannot repair this — Go-mode MapShadow
// tracks a monotonic mapped-shadow interval and silently skips interior
// requests — so at init this package fills every unmapped gap in the
// window's shadow (shadow = app*2 + 0x2000_0000_0000) with zeroed
// MAP_FIXED mappings, leaving existing arena shadow untouched.
package race

/*
#include <mach/mach.h>
#include <mach/mach_vm.h>
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>

static const uint64_t kShadowBeg = 0x200000000000ull;

static uint64_t mem_to_shadow(uint64_t addr) { return addr*2 + kShadowBeg; }

static int shadow_is_mapped(uint64_t shadow_addr) {
	mach_vm_address_t q = shadow_addr;
	mach_vm_size_t size = 0;
	vm_region_basic_info_data_64_t info;
	mach_msg_type_number_t count = VM_REGION_BASIC_INFO_COUNT_64;
	mach_port_t object_name = MACH_PORT_NULL;
	kern_return_t kr = mach_vm_region(mach_task_self(), &q, &size, VM_REGION_BASIC_INFO_64,
		(vm_region_info_t)&info, &count, &object_name);
	if (object_name != MACH_PORT_NULL) {
		mach_port_deallocate(mach_task_self(), object_name);
	}
	return kr == KERN_SUCCESS && q <= shadow_addr && (info.protection & VM_PROT_READ) != 0;
}

// map_shadow_holes mmaps zeroed shadow into the unmapped gaps of the shadow
// range for app range [abeg, aend), leaving already-mapped shadow untouched.
static int map_shadow_holes(uint64_t abeg, uint64_t aend) {
	long page = sysconf(_SC_PAGESIZE);
	if (page <= 0) {
		return -1;
	}
	uint64_t mask = ~(uint64_t)(page - 1);
	uint64_t sbeg = mem_to_shadow(abeg) & mask;
	uint64_t send = (mem_to_shadow(aend) + page - 1) & mask;
	uint64_t addr = sbeg;
	while (addr < send) {
		mach_vm_address_t q = addr;
		mach_vm_size_t size = 0;
		vm_region_basic_info_data_64_t info;
		mach_msg_type_number_t count = VM_REGION_BASIC_INFO_COUNT_64;
		mach_port_t object_name = MACH_PORT_NULL;
		kern_return_t kr = mach_vm_region(mach_task_self(), &q, &size, VM_REGION_BASIC_INFO_64,
			(vm_region_info_t)&info, &count, &object_name);
		if (object_name != MACH_PORT_NULL) {
			mach_port_deallocate(mach_task_self(), object_name);
		}
		uint64_t hole_end = (kr == KERN_SUCCESS && q < send) ? q : send;
		if (hole_end > addr) {
			void *p = mmap((void *)addr, hole_end-addr, PROT_READ|PROT_WRITE,
				MAP_FIXED|MAP_PRIVATE|MAP_ANON|MAP_NORESERVE, -1, 0);
			if (p == MAP_FAILED) {
				return -1;
			}
		}
		if (kr != KERN_SUCCESS || q >= send) {
			break;
		}
		addr = q + size;
	}
	return 0;
}
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

// TSAN's Go/darwin heap window; raceinit places arenas here.
const heapWindowBeg, heapWindowEnd = 0x00c0_0000_0000, 0x00e0_0000_0000

// enabled records that the layout self-check passed and the window's shadow
// holes were filled at init.
var enabled bool

func init() {
	probe := uintptr(unsafe.Pointer(new([16]byte)))
	if probe < heapWindowBeg || probe >= heapWindowEnd ||
		C.shadow_is_mapped(C.uint64_t(mem2shadow(probe))) == 0 {
		fmt.Fprintln(os.Stderr, "race: TSAN shadow layout self-check failed; not pre-mapping shadow for the heap window")
		return
	}
	enabled = C.map_shadow_holes(C.uint64_t(heapWindowBeg), C.uint64_t(heapWindowEnd)) == 0
	if !enabled {
		fmt.Fprintln(os.Stderr, "race: pre-mapping TSAN shadow for the heap window failed")
	}
}

func mem2shadow(addr uintptr) uintptr { return addr*2 + 0x2000_0000_0000 }

// shadowIsMapped reports whether the shadow for addr is mapped (test hook).
func shadowIsMapped(addr uintptr) bool {
	return C.shadow_is_mapped(C.uint64_t(mem2shadow(addr))) != 0
}
