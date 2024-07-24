package eth

import (
	"fmt"
	"os"
	"testing"
)

func TestRemoveContents(t *testing.T) {
	rootName := t.TempDir()
	err := os.RemoveAll(rootName)
	if err != nil {
		t.Fatal(err)
	}
	//t.Logf("creating %s/root...", rootName)
	root := fmt.Sprintf("%s/root", rootName)
	err = os.Mkdir(rootName, 0750)
	if err != nil {
		t.Fatal(err)
	}
	rootName = root
	err = os.Mkdir(root, 0750)
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println("OK")
	for i := 0; i < 3; i++ {
		outerName := fmt.Sprintf("%s/outer_%d", rootName, i+1)
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0750)
		if err != nil {
			t.Fatal(err)
		}
		//t.Logf("OK")
		for j := 0; j < 2; j++ {
			innerName := fmt.Sprintf("%s/inner_%d", outerName, j+1)
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0750)
			if err != nil {
				t.Fatal(err)
			}
			//t.Log("OK")
			for k := 0; k < 2; k++ {
				innestName := fmt.Sprintf("%s/innest_%d", innerName, k+1)
				//t.Logf("creating %s... ", innestName)
				err := os.Mkdir(innestName, 0750)
				if err != nil {
					t.Fatal(err)
				}
				//t.Log("OK")
			}
		}
	}
	list, err := os.ReadDir(rootName)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatal("expected 3 dirs got ", len(list))
	}
	err = RemoveContents(rootName)
	if err != nil {
		t.Fatal(err)
	}
	list, err = os.ReadDir(rootName)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatal("expected 0 dirs got ", len(list))
	}
}
