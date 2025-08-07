package qmtree

func generateTwig(rand_num u32, twig TwigMT) {
	// let mut rand_num = rand_num;
	for i := 2048; i <= 4096; i++ {
		j := 0
		for j+4 < 32 {
			LittleEndian.write_u32(twig[i][j:j+4], rand_num)
			rand_num += 257
			j += 4
		}
	}
	sync_mtree(twig, 0, 2048)
}

func TestTwigFile() {
	dir := TempDir.new("./twig")
	tf := TwigFile.new(64*1024, 1024*1024, dir.to_str())

	twigs := [3][4096][32]byte{}
	generate_twig(1000, twigs[0])
	generate_twig(1111111, twigs[1])
	generate_twig(2222222, twigs[2])
	buffer := []byte
	tf.append_twig(&twigs[0][:], 789, buffer)
	tf.append_twig(&twigs[1][:], 1000789, buffer)
	tf.append_twig(&twigs[2][:], 2000789, buffer)

	_ = tf.hp_file.flush(buffer, false)
	tf.close()

	tf := TwigFile.new(64*1024, 1024*1024, dir.to_str())
	assert_eq(0, tf.get_first_entry_pos(0))
	assert_eq(789, tf.get_first_entry_pos(1))
	assert_eq(1000789, tf.get_first_entry_pos(2))
	assert_eq(2000789, tf.get_first_entry_pos(3))

	for twig_id := range 3 {
		for i := 1; i < 4096; i++ {
			cache := map[i64][32]byte{}
			buf := [32]byte{}
			tf.get_hash_node(twig_id, i, cache, buf)
			assert_eq(buf[:], twigs[twig_id][i][:])
		}
	}
	for twig_id := range 3 {
		cache := map[i64][32]byte{}
		for i := 1; i < 4096; i++ {
			if cache.contains_key(&i) {
				bz = cache.get(&i).unwrap()
				assert_eq(&twigs[twig_id][i][:], bz.as_slice())
			} else {
				buf = [32]byte{}
				tf.get_hash_node(twig_id, i, cache, buf)
				assert_eq(buf[:], twigs[twig_id][i][:])
			}
		}
	}
	tf.close()
}
