// Copyright 2022 The Erigon Authors
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

package bptree

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
)

type Node23Graph struct {
	node *Node23
}

func NewGraph(node *Node23) *Node23Graph {
	return &Node23Graph{node}
}

func (g *Node23Graph) saveDot(filename string, debug bool) {
	palette := []string{"#FDF3D0", "#DCE8FA", "#D9E7D6", "#F1CFCD", "#F5F5F5", "#E1D5E7", "#FFE6CC", "white"}
	const unexposedIndex = 0
	const exposedIndex = 1
	const updatedIndex = 2

	f, err := os.OpenFile(filename+".dot", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	if g.node == nil {
		if _, err := f.WriteString("strict digraph {\nnode [shape=record];}\n"); err != nil {
			log.Fatal(err)
		}
		return
	}
	if _, err := f.WriteString("strict digraph {\nnode [shape=record];\n"); err != nil {
		log.Fatal(err)
	}
	for _, n := range g.node.walkNodesPostOrder() {
		left, down, right := "", "", ""
		switch n.childrenCount() {
		case 1:
			left = "<L>L"
		case 2:
			left = "<L>L"
			right = "<R>R"
		case 3:
			left = "<L>L"
			down = "<D>D"
			right = "<R>R"
		}
		var nodeID string
		if n.isLeaf {
			var next string
			if n.keyCount() > 0 {
				if n.nextKey() == nil {
					next = "nil"
				} else {
					next = strconv.FormatUint(uint64(*n.nextKey()), 10)
				}
				if debug {
					nodeID = fmt.Sprintf("k=%v %s-%v", deref(n.keys[:len(n.keys)-1]), next, n.keys)
				} else {
					nodeID = fmt.Sprintf("k=%v %s", deref(n.keys[:len(n.keys)-1]), next)
				}
			} else {
				nodeID = "k=[]"
			}
		} else {
			if debug {
				nodeID = fmt.Sprintf("k=%v-%v", deref(n.keys), n.keys)
			} else {
				nodeID = fmt.Sprintf("k=%v", deref(n.keys))
			}
		}
		var color string
		if n.exposed {
			if n.updated {
				color = palette[updatedIndex]
			} else {
				color = palette[exposedIndex]
			}
		} else {
			ensure(!n.updated, fmt.Sprintf("saveDot: node %v is not exposed but updated", n))
			color = palette[unexposedIndex]
		}
		s := fmt.Sprintf("%d [label=\"%s|{<C>%s|%s}|%s\" style=filled fillcolor=\"%s\"];\n", n.rawPointer(), left, nodeID, down, right, color)
		if _, err := f.WriteString(s); err != nil {
			log.Fatal(err)
		}
	}
	for _, n := range g.node.walkNodesPostOrder() {
		var treeLeft, treeDown, treeRight *Node23
		switch n.childrenCount() {
		case 1:
			treeLeft = n.children[0]
		case 2:
			treeLeft = n.children[0]
			treeRight = n.children[1]
		case 3:
			treeLeft = n.children[0]
			treeDown = n.children[1]
			treeRight = n.children[2]
		}
		if treeLeft != nil {
			//if _, err := f.WriteString(fmt.Sprintln(n.rawPointer(), ":L -> ", treeLeft.rawPointer(), ":C;")); err != nil {
			if _, err := f.WriteString(fmt.Sprintf("%d:L -> %d:C;\n", n.rawPointer(), treeLeft.rawPointer())); err != nil {
				log.Fatal(err)
			}
		}
		if treeDown != nil {
			//if _, err := f.WriteString(fmt.Sprintln(n.rawPointer(), ":D -> ", treeDown.rawPointer(), ":C;")); err != nil {
			if _, err := f.WriteString(fmt.Sprintf("%d:D -> %d:C;\n", n.rawPointer(), treeDown.rawPointer())); err != nil {
				log.Fatal(err)
			}
		}
		if treeRight != nil {
			//if _, err := f.WriteString(fmt.Sprintln(n.rawPointer(), ":R -> ", treeRight.rawPointer(), ":C;")); err != nil {
			if _, err := f.WriteString(fmt.Sprintf("%d:R -> %d:C;\n", n.rawPointer(), treeRight.rawPointer())); err != nil {
				log.Fatal(err)
			}
		}
	}
	if _, err := f.WriteString("}\n"); err != nil {
		log.Fatal(err)
	}
}

func (g *Node23Graph) saveDotAndPicture(filename string, debug bool) error {
	graphDir := "testdata/graph/"
	_ = os.MkdirAll(graphDir, os.ModePerm)
	filepath := graphDir + filename
	_ = os.Remove(filepath + ".dot")
	_ = os.Remove(filepath + ".png")
	g.saveDot(filepath, debug)
	dotExecutable, _ := exec.LookPath("dot")
	cmdDot := &exec.Cmd{
		Path:   dotExecutable,
		Args:   []string{dotExecutable, "-Tpng", filepath + ".dot", "-o", filepath + ".png"},
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	if err := cmdDot.Run(); err != nil {
		return err
	}
	return nil
}
