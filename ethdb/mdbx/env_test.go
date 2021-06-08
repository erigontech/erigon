package mdbx

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func TestEnv_Path_notOpen(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer env.Close()

	// before Open the Path method returns "" and a non-nil error.
	path, err := env.Path()
	if err == nil {
		t.Errorf("no error returned before Open")
	}
	if path != "" {
		t.Errorf("non-zero path returned before Open")
	}
}

func TestEnv_Path(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// open an environment
	dir := t.TempDir()
	err = env.Open(dir, 0, 0644)
	defer env.Close()
	if err != nil {
		t.Errorf("open: %v", err)
	}
	path, err := env.Path()
	if err != nil {
		t.Errorf("path: %v", err)
	}
	if path != dir {
		t.Errorf("path: %q (!= %q)", path, dir)
	}
}

func TestEnv_Open_notExist(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("create: %s", err)
	}
	defer env.Close()

	// ensure that opening a non-existent path fails.
	err = env.Open("/path/does/not/exist/aoeu", 0, 0664)
	if !IsNotExist(err) {
		t.Errorf("open: %v", err)
	}
}

func TestEnv_Open(t *testing.T) {
	env, err1 := NewEnv()
	if err1 != nil {
		t.Error(err1)
		return
	}
	defer env.Close()

	// open an environment at a temporary path.
	path := t.TempDir()
	err := env.Open(path, 0, 0664)
	if err != nil {
		t.Errorf("open: %s", err)
	}
}

func TestEnv_FD(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("FD funcs not supported on windows")
	}
	env, err1 := NewEnv()
	if err1 != nil {
		t.Error(err1)
		return
	}
	defer env.Close()

	fd, err := env.FD()
	if err != nil && !strings.Contains(err.Error(), "operation not permitted") {
		t.Errorf("fd: %x (%v)", fd, err)
	}

	// open an environment at a temporary path.
	path := t.TempDir()
	err = env.Open(path, 0, 0664)
	if err != nil {
		t.Errorf("open: %s", err)
	}

	fd, err = env.FD()
	if err != nil {
		t.Errorf("fd error: %v", err)
	}
	if fd == 0 {
		t.Errorf("fd: %x", fd)
	}
}

func TestEnv_Flags(t *testing.T) {
	env := setup(t)

	flags, err := env.Flags()
	if err != nil {
		t.Error(err)
		return
	}

	if flags&NoTLS == 0 {
		t.Errorf("NoTLS is not set")
	}
	//if flags&SafeNoSync != 0 {
	//	t.Errorf("UtterlyNoSync is set")
	//}

	err = env.SetFlags(SafeNoSync)
	if err != nil {
		t.Error(err)
	}

	flags, err = env.Flags()
	if err != nil {
		t.Error(err)
	}
	if flags&SafeNoSync == 0 {
		t.Error("UtterlyNoSync is not set")
	}

	err = env.UnsetFlags(SafeNoSync)
	if err != nil {
		t.Error(err)
	}

	flags, err = env.Flags()
	if err != nil {
		t.Error(err)
	}
	if flags&SafeNoSync != 0 {
		t.Error("UtterlyNoSync is set")
	}
}

func TestEnv_SetMaxReader(t *testing.T) {
	dir := t.TempDir()

	env, err := NewEnv()
	if err != nil {
		t.Error(err)
	}

	maxreaders := uint64(246)
	err = env.SetOption(OptMaxReaders, maxreaders)
	if err != nil {
		t.Fatal(err)
	}
	_maxreaders, err := env.GetOption(OptMaxReaders)
	if err != nil {
		t.Fatal(err)
	}
	if _maxreaders < maxreaders {
		t.Errorf("unexpected MaxReaders: %v (< %v)", _maxreaders, maxreaders)
	}

	err = env.Open(dir, 0, 0644)
	defer env.Close()
	if err != nil {
		env.Close()
		t.Error(err)
	}
	//
	//err = env.SetOption(OptMaxReaders, uint64(126))
	//if !IsErrnoSys(err, syscall.EPERM) {
	//	t.Errorf("unexpected error: %v (!= %v)", err, syscall.EPERM)
	//}
	//_maxreaders, err = env.GetOption(OptMaxReaders)
	//if err != nil {
	//	t.Error(err)
	//}
	//if _maxreaders < maxreaders {
	//	t.Errorf("unexpected MaxReaders: %v (!= %v)", _maxreaders, maxreaders)
	//}
}

func TestEnv_SetDebug(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Error(err)
	}

	err = env.SetDebug(LogLvlDoNotChange, DbgLegacyTxOverlap, LoggerDoNotChange)
	if err != nil {
		t.Error(err)
	}
}

//func TestEnv_SetMapSize(t *testing.T) {
//	env := setup(t)
//
//
//	const minsize = 100 << 20 // 100MB
//	err := env.SetMapSize(minsize)
//	if err != nil {
//		t.Error(err)
//	}
//
//	err = env.Update(func(txn *Txn) (err error) {
//		return nil
//	})
//	if err != nil {
//		t.Error(err)
//	}
//
//	info, err := env.Info()
//	if err != nil {
//		t.Error(err)
//	} else if info.MapSize < minsize {
//		t.Errorf("unexpected mapsize: %v (< %v)", info.MapSize, minsize)
//	}
//}

//func TestEnv_ReaderList(t *testing.T) {
//	env := setup(t)
//
//
//	var numreaders = 2
//
//	var fin sync.WaitGroup
//	defer fin.Wait()
//	ready := make(chan struct{})
//	done := make(chan struct{})
//	defer close(done)
//
//	t.Logf("starting")
//
//	for i := 0; i < numreaders; i++ {
//		fin.Add(1)
//		go func(i int) {
//			defer fin.Done()
//			err := env.View(func(txn *Txn) (err error) {
//				t.Logf("reader %v: ready", i)
//				ready <- struct{}{}
//
//				<-done
//				t.Logf("reader %v: done", i)
//				return nil
//			})
//			if err != nil {
//				t.Errorf("reader %d: %q", i, err)
//			}
//		}(i)
//
//		// wait for each reader to become ready
//		<-ready
//	}
//
//	var readers []string
//	_ = env.ReaderList(func(msg string) error {
//		t.Logf("reader: %q", msg)
//		readers = append(readers, msg)
//		return nil
//	})
//	if len(readers) != numreaders+1 {
//		t.Errorf("unexpected reader list size: %d (!= %d)", len(readers), numreaders)
//	}
//}

//func TestEnv_ReaderList_error(t *testing.T) {
//	env := setup(t)
//
//
//	var numreaders = 2
//
//	var fin sync.WaitGroup
//	defer fin.Wait()
//	ready := make(chan struct{})
//	done := make(chan struct{})
//	defer close(done)
//
//	t.Logf("starting")
//
//	for i := 0; i < numreaders; i++ {
//		fin.Add(1)
//		go func(i int) {
//			defer fin.Done()
//			err := env.View(func(txn *Txn) (err error) {
//				t.Logf("reader %v: ready", i)
//				ready <- struct{}{}
//
//				<-done
//				t.Logf("reader %v: done", i)
//				return nil
//			})
//			if err != nil {
//				t.Errorf("reader %d: %q", i, err)
//			}
//		}(i)
//
//		// wait for each reader to become ready
//		<-ready
//	}
//
//	e := fmt.Errorf("testerror")
//	var readers []string
//	err := env.ReaderList(func(msg string) error {
//		readers = append(readers, msg)
//		return e
//	})
//	if err == nil {
//		t.Errorf("expected error")
//	}
//	if !errors.Is(err, e) {
//		t.Errorf("unexpected error: %q (!= %q)", err, e)
//	}
//	if len(readers) != 1 {
//		t.Errorf("unexpected reader list size: %d (!= %d)", len(readers), 1)
//	}
//}
//
//func TestEnv_ReaderList_envInvalid(t *testing.T) {
//	err := (&Env{}).ReaderList(func(msg string) error {
//		t.Logf("%s", msg)
//		return nil
//	})
//	if err == nil {
//		t.Errorf("expected error")
//	}
//}
//
//func TestEnv_ReaderList_nilFunc(t *testing.T) {
//	env, err := NewEnv()
//	if err != nil {
//		t.Fatal(err)
//	}
//	err = env.ReaderList(nil)
//	if err == nil {
//		t.Errorf("expected error")
//	}
//}

func TestEnv_ReaderCheck(t *testing.T) {
	env := setup(t)

	numDead, err := env.ReaderCheck()
	if err != nil {
		t.Error(err)
	}
	if numDead != 0 {
		t.Errorf("unexpected dead readers: %v (!= %v)", numDead, 0)
	}
}

//func TestEnv_Copy(t *testing.T) {
//	testEnvCopy(t, 0, false, false)
//}
//
//func TestEnv_CopyFlags(t *testing.T) {
//	testEnvCopy(t, CopyCompact, true, false)
//}
//
//func TestEnv_CopyFlags_zero(t *testing.T) {
//	testEnvCopy(t, 0, true, false)
//}
//
//func TestEnv_CopyFD(t *testing.T) {
//	testEnvCopy(t, 0, false, true)
//}
//
//func TestEnv_CopyFDFlags(t *testing.T) {
//	testEnvCopy(t, CopyCompact, true, true)
//}
//
//func TestEnv_CopyFDFlags_zero(t *testing.T) {
//	testEnvCopy(t, 0, true, true)
//}
//
//func testEnvCopy(t *testing.T, flags uint, useflags bool, usefd bool) {
//	dircp, err := ioutil.TempDir("", "test-env-copy-")
//	if err != nil {
//		t.Fatal(err)
//	}
//	defer os.RemoveAll(dircp)
//
//	var fd uintptr
//	if usefd {
//		path := filepath.Join(dircp, "data.mdb")
//		f, err := os.Create(path)
//		if err != nil {
//			t.Error(err)
//			return
//		}
//		fd = f.Fd()
//		defer f.Close()
//	}
//
//	env := setup(t)
//
//
//	item := struct{ k, v []byte }{
//		[]byte("k0"),
//		[]byte("v0"),
//	}
//
//	err = env.Update(func(txn *Txn) (err error) {
//		db, err := txn.OpenRoot(0)
//		if err != nil {
//			return err
//		}
//		return txn.Put(db, item.k, item.v, 0)
//	})
//	if err != nil {
//		t.Error(err)
//	}
//
//	switch {
//	case usefd && useflags:
//		err = env.CopyFDFlag(fd, flags)
//	case usefd && !useflags:
//		err = env.CopyFD(fd)
//	case !usefd && useflags:
//		err = env.CopyFlag(dircp, flags)
//	case !usefd && !useflags:
//		err = env.Copy(dircp)
//	}
//	if err != nil {
//		t.Error(err)
//	}
//
//	envcp, err := NewEnv()
//	if err != nil {
//		t.Error(err)
//	}
//	err = envcp.Open(dircp, 0, 0644)
//	defer envcp.Close()
//	if err != nil {
//		t.Error(err)
//		return
//	}
//
//	err = envcp.View(func(txn *Txn) (err error) {
//		db, err := txn.OpenRoot(0)
//		if err != nil {
//			return err
//		}
//		v, err := txn.Get(db, item.k)
//		if err != nil {
//			return err
//		}
//		if !bytes.Equal(v, item.v) {
//			return fmt.Errorf("unexpected value: %q (!= %q)", v, "v0")
//		}
//		return nil
//	})
//	if err != nil {
//		t.Error(err)
//	}
//}

func TestEnv_Sync(t *testing.T) {
	env := setupFlags(t, SafeNoSync)

	item := struct{ k, v []byte }{[]byte("k0"), []byte("v0")}

	err := env.Update(func(txn *Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return txn.Put(db, item.k, item.v, 0)
	})
	if err != nil {
		t.Error(err)
	}

	err = env.Sync(true, false)
	if err != nil {
		t.Error(err)
	}
}

func setup(t testing.TB) *Env {
	return setupFlags(t, 0)
}

func setupFlags(t testing.TB, flags uint) *Env {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("env: %s", err)
	}
	path := t.TempDir()
	err = env.SetOption(OptMaxDB, 1024)
	if err != nil {
		t.Fatalf("setmaxdbs: %v", err)
	}
	const pageSize = 4096
	err = env.SetGeometry(-1, -1, 64*1024*pageSize, -1, -1, pageSize)
	if err != nil {
		t.Fatalf("setmaxdbs: %v", err)
	}
	err = env.Open(path, flags, 0664)
	if err != nil {
		t.Fatalf("open: %s", err)
	}
	t.Cleanup(func() {
		env.Close()
	})
	return env
}

type T interface {
	Errorf(format string, vals ...interface{})
	Fatalf(format string, vals ...interface{})
}

func TestEnv_MaxKeySize(t *testing.T) {
	env := setup(t)

	n := env.MaxKeySize()
	if n <= 0 {
		t.Errorf("invaild maxkeysize: %d", n)
	}
}

func TestEnv_MaxKeySize_nil(t *testing.T) {
	var env *Env
	n := env.MaxKeySize()
	if n < -1 {
		t.Errorf("invaild maxkeysize: %d", n)
	}
	t.Logf("mdb_env_get_maxkeysize: %d", n)
}

func TestEnv_CloseDBI(t *testing.T) {
	env := setup(t)

	const numdb = 1000
	for i := 0; i < numdb; i++ {
		dbname := fmt.Sprintf("db%d", i)

		var dbi DBI
		err := env.Update(func(txn *Txn) (err error) {
			dbi, err = txn.CreateDBI(dbname)
			return err
		})
		if err != nil {
			t.Errorf("%s", err)
		}

		env.CloseDBI(dbi)
	}

	stat, err := env.Stat()
	if err != nil {
		t.Errorf("%s", err)
		return
	}

	//nolint:goerr113
	if stat.Entries != numdb {
		t.Errorf("unexpected entries: %d (not %d)", stat.Entries, numdb)
	}
}
