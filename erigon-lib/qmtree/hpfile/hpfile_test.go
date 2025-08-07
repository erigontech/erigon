package hpfile

/// Temporary directory for unit test
type tempDir struct {
    dir: String,
}

impl TempDir {
    /// Create a new TempDir
    pub fn new(dir: &str) -> Self {
        remove_dir_all(dir).unwrap_or(()); // ignore error
        create_dir(dir).unwrap_or(()); // ignore error
        Self {
            dir: dir.to_string(),
        }
    }

    /// Return the path of this temporary directory
    pub fn to_str(&self) -> String {
        self.dir.clone()
    }

    /// Return the names of the files in this directory
    pub fn list(&self) -> Vec<String> {
        TempDir::list_dir(&self.dir)
    }

    /// Return the names of the files in `dir`
    pub fn list_dir(dir: &str) -> Vec<String> {
        let mut result = vec![];
        let paths = std::fs::read_dir(Path::new(dir)).unwrap();
        for path in paths {
            result.push(path.unwrap().path().to_str().unwrap().to_string());
        }
        result.sort();
        result
    }

    /// Create a new file in this directory
    pub fn create_file(&self, name: &str) {
        let file_path = Path::new(&self.dir).join(Path::new(name));
        File::create_new(file_path).unwrap();
    }

    /// Return the names of the files in `path` and its subdirectories recursively
    pub fn list_all(path: &Path) -> Vec<String> {
        let mut vec = Vec::new();
        TempDir::_list_files(&mut vec, path);
        vec.sort();
        vec
    }

    fn _list_files(vec: &mut Vec<String>, path: &Path) {
        if metadata(path).unwrap().is_dir() {
            let paths = read_dir(path).unwrap();
            for path_result in paths {
                let full_path = path_result.unwrap().path();
                if metadata(&full_path).unwrap().is_dir() {
                    TempDir::_list_files(vec, &full_path);
                } else {
                    vec.push(String::from(full_path.to_str().unwrap()));
                }
            }
        }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        remove_dir_all(&self.dir).unwrap_or(()); // ignore error
    }
}


func Test_hp_file_new(t *testing.T) {
        let dir = TempDir::new("hp_file_test");
        let buffer_size = 64;
        let segment_size = 128;
        let hp = HPFile::new(buffer_size, segment_size, dir.to_str(), false).unwrap();
        assert_eq!(hp.buffer_size, buffer_size);
        assert_eq!(hp.segment_size, segment_size);
        assert_eq!(hp.file_map.len(), 1);

        let slice0 = [1; 44];
        let mut buffer = vec![];
        let mut pos = hp.append(slice0.as_ref(), &mut buffer).unwrap();
        assert_eq!(0, pos);
        assert_eq!(44, hp.size());

        let slice1a = [2; 16];
        let slice1b = [3; 10];
        let mut slice1 = vec![];
        slice1.extend_from_slice(&slice1a);
        slice1.extend_from_slice(&slice1b);
        pos = hp.append(slice1.as_ref(), &mut buffer).unwrap();
        assert_eq!(44, pos);
        assert_eq!(70, hp.size());

        let slice2a = [4; 25];
        let slice2b = [5; 25];
        let mut slice2 = vec![];
        slice2.extend_from_slice(&slice2a);
        slice2.extend_from_slice(&slice2b);
        pos = hp.append(slice2.as_ref(), &mut buffer).unwrap();
        assert_eq!(70, pos);
        assert_eq!(120, hp.size());

        let mut check0 = [0; 44];
        hp.read_at(&mut check0, 0).unwrap();
        assert_eq!(slice0.to_vec(), check0.to_vec());

        hp.flush(&mut buffer, false).unwrap();

        let mut check1 = [0; 26];
        hp.read_at(&mut check1, 44).unwrap();
        assert_eq!(slice1, check1);

        let mut check2 = [0; 50];
        hp.read_at(&mut check2, 70).unwrap();
        assert_eq!(slice2, check2);

        let slice3 = [0; 16];
        pos = hp.append(slice3.to_vec().as_ref(), &mut buffer).unwrap();
        assert_eq!(120, pos);
        assert_eq!(136, hp.size());

        hp.flush(&mut buffer, false).unwrap();

        #[cfg(not(feature = "all_in_mem"))]
        hp.close();

        #[cfg(feature = "all_in_mem")]
        let hp_new = hp;
        #[cfg(not(feature = "all_in_mem"))]
        let hp_new = HPFile::new(64, 128, dir.to_str(), false).unwrap();

        hp_new.read_at(&mut check0, 0).unwrap();
        assert_eq!(slice0.to_vec(), check0.to_vec());

        hp_new.read_at(&mut check1, 44).unwrap();
        assert_eq!(slice1, check1);

        hp_new.read_at(&mut check2, 70).unwrap();
        assert_eq!(slice2, check2);

        let mut check3 = [0; 16];
        hp_new.read_at(&mut check3, 120).unwrap();
        assert_eq!(slice3.to_vec(), check3.to_vec());

        hp_new.prune_head(64).unwrap();
        hp_new.truncate(120).unwrap();
        assert_eq!(hp_new.size(), 120);
        let mut slice4 = vec![];
        hp_new.read_at(&mut slice4, 120).unwrap();
        assert_eq!(slice4.len(), 0);
    }


