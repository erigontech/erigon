### How to regenerate illustrations
Illustrations used in this guide are auto-generated from the actual LMDB databases created by some test code. Therefore, they reflect the behaviour of the actual LMDB code currently embedded into `turbo-geth` via `lmdb-go` repository here: https://github.com/ledgerwatch/lmdb-go. To understand which illustration is which, this guide will give its name, and will also show the test code that generates the database for each illustration. The illustrations can be reproduced by running:
```
make hack
./build/bin/hack --action defrag
```
This creates the `.dot` and `.png` files in the current directory.

## Empty database
Test code does nothing:
```
func nothing(kv ethdb.KV, _ ethdb.Tx) (bool, error) {
	return true, nil
}
```
Here is what we see:

![vis1_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis1_0.png)

Each LMDB database has a meta page (actually it has two meta pages, but any reader or writer always chooses the one that was the last updated). Meta page has lots of interesting information, but the most important pieces for this guide are two "page pointers": `FREE_DBI` and `MAIN_DBI`. By page pointers here we mean a 64-bit page ID, which also happens to be the offset of that page in the `data.md` file, divided by the page size (4096 bytes). This means that the page with ID 0 has offset `0 * 4096 = 0` from the beginning of the file (this happens to be the first meta page), the page with ID 1 has offset 4096 (this is the second meta page), page with ID 2 has offset `2 * 4096 = 8192` and so on.

**Q: How do readers and writers choose which of the two meta pages to use?** One of the fields within a meta page is the ID of the last transaction that has updated that meta page. Transactions with even IDs update the meta page 0, and transactions with odd IDs update the meta page 1. So readers and writers look at both meta pages and simply choose the one that has the higher transaction ID.

## One table, two records in it
Test code that generates it:
```
func generate2(tx ethdb.Tx, entries int) error {
	c := tx.Cursor("t")
	defer c.Close()
	for i := 0; i < entries; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Append([]byte(k), []byte("very_short_value")); err != nil {
			return err
		}
	}
	return nil
}
```
This function takes parameter `entries`, which in this example is `2` to create two entries. And here is the result:

![vis2_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis2_0.png)

The test code snippet above gives an impression that there is only DB transaction happening. You should note, however, that the code does not create table `t`, it starts using it (by making a cursor for it) assuming that it is already created. This table was created by some "convenience" wrappers and it was transaction with ID `1000001`. Transaction that is shown in the snippet is the one with ID `1000002`.

**Q: Why do transaction IDs in an empty database start with 1000001 and not with 1?** This is a recent change that we have made to our version of LMDB. We have noticed that the behaviour of LMDB regarding free space is dependent (surprisingly) on the numerical value of transaction ID, and the lower the ID, the more problematic behaviour becomes for large amount of free space (this will be explained later, because it is actually one of the main reasons this guide is being written in the first place). So a simple fix is just to start with a higher transaction ID, and value 1 million is round, so that people will notice straight away it is not random, and it is high enough that we should not see the problem we were seeing for the amount of free space up to 2Tb.

First of all, lets list all the pages that we can see (or not see) on the picture. We know that pages 0 and 1 are meta pages, and are always present. Page 2 is mentioned in a freelist record, inside the blue box. To make illustrations more clear, we use blue boxes to show pages belonging to `FREE_DBI`, which is a sub-database storing the collection of so-called freelists. On the picture above, the `FREE_DBI` sub-database resides on one page, page 5 (this is the value of the "page pointer" FREE_DBI). It contains one freelist, currently associated with the transaction ID `1000002`. And this freelist has only one element, which is page 2. The fact that page 2 is on the freelist, means that in subsequent transactions, page 2 can be recycled, i.e. filled up with some other data and/or structures.

**Q: If a freelist is associated with some transaction ID, does it mean that that transaction has created this freelist (meaning that it has freed the pages mentioned in this freelist)?** In many cases yes, but not always. It will be shown later that sometimes freelist associated with transaction IDs that did not create them, or indeed with transactions that never happened (for example, a freelist can be associated with a transaction ID `999997`, which never happens, because we start with `1000001`). This will be explained in more details later.

We continue listing all the pages. Page 3 is the "root page" of the sub-database for the table `t`. This can be seen from the yellow box. To make illustrations more clear, we use yellow boxes to show pages belonging to `MAIN_DBI`, which is a sub-database storing collection of more sub-databases we call "tables". Pages for data and structure belonging to the tables are shown in brown (for branches) and green (for leaves). So page 3 is actually that green box with two entries in it. It belongs to the table `t`. And finally, page 4 is the yellow box containing the only entry inside `MAIN_DBI`. You can see that the yellow box is page 4 by looking at the "page pointer" `MAIN_DBI` coming from meta page.

In this small example, neither `MAIN_DBI` nor table `t` are large enough to require any so-called "branch pages". All information fits into a single page, which here is 4096 (LMDB default). In the leaf page of the table `t`, one can see keys and values separated by colons.

## 61 empty tables
Test code snippet:
```
func generate3(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	for i := 0; i < 61; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.(ethdb.BucketMigrator).CreateBucket(k); err != nil {
			return false, err
		}
	}
	return true, nil
}
```
This code creates 61 empty tables within a single writeable transaction. Why 61? Because this is just enough not to fit on a single page, so that we can see how `MAIN_DBI` begins to have branch pages. Here is how it looks:

![vis3_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis3_0.png)

To keep illustrations small, we use some "folding" of repeated data, here shown as `...53 more records here...`. This will allows us to create examples that are large enough to have certain features (like branch pages), without overburdening the illustrations.

Branch pages of the `MAIN_DBI` sub-database are shown as purple. We can see such a branch page which has ID 4 (it is on the `MAIN_DBI` "page pointer"). One may notice that there is no `FREE_DBI` sub-database here, because there are no freelists. There was just one single transaction (we did not need table `t` pre-created like before), and it could not have freed anything up, because it started with an empty database.

## One table with 200 entries
It is the same code as we saw previously:
```
func generate2(tx ethdb.Tx, entries int) error {
	c := tx.Cursor("t")
	defer c.Close()
	for i := 0; i < entries; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Append([]byte(k), []byte("very_short_value")); err != nil {
			return err
		}
	}
	return nil
}
```
But this time, we pass value `200` as `entries`. And here is the result:

![vis4_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis4_0.png)

As in the example with one table and two records, we see one freelist, exactly the same as before, and for the same reason (table `t` used to be empty, so that the page containing pointer to it has to be replaced with a new page).

Since we now have 200 entries in the table `t`, and they do not fit into a single page anymore, we observe a branch page (brown box), as well as two leaf pages (green boxes). Nothing much more interesting here.

## One table, one key with a short value, one key with a very long value
Here is the test code that generates it:
```
func generate4(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	c := tx.Cursor("t")
	defer c.Close()
	if err := c.Append([]byte("k1"), []byte("very_short_value")); err != nil {
		return false, err
	}
	if err := c.Append([]byte("k2"), []byte(strings.Repeat("long_value_", 1000))); err != nil {
		return false, err
	}
	return true, nil
}
```
The value for the second key ends up being `11 * 1000 = 11000` bytes long, which clearly does not fit on a single page, but would fit on three pages. This is how it looks like indeed:

![vis5_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis5_0.png)

When confronted with such large values, LMDB places 64-bit number instead, which is interpreted as the first page ID of where the actual value resides. These pages where the actual value resides are called "overflow pages". LMDB insists that overflow pages belonging to the same value have consecutive page IDs, i.e. are stored together in the `data.md` file. We suspect that this is done to avoid "jumping around" the database file looking for the pieces of some value. In this example, the first overflow page ID is 4 (looking at `OVERFLOW(4)`), which means that pages with IDs 4, 5, and 6 contain that very long value. This matches us with the rest of the picture, because page 7 is the yellow box, page 8 is the blue box, page 2 is free (it is on the freelist), and page 3 is the first green box.

We are now coming closer to the main motivation for writing this guide. Overflow pages present serious challenges when combined with the specific way the freelists are handled in LMDB.

**Q: If overflow pages present a serious challenge, would it make sense to try to avoid them?** Yes, this is what we will definitely attempt to do. Wish we knew all this much earlier.

## One DupSort table, and keys with three, two and one value
Here is the test code:
```
func generate5(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	c := tx.CursorDupSort("t")
	defer c.Close()
	if err := c.AppendDup([]byte("key1"), []byte("value11")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key1"), []byte("value12")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key1"), []byte("value13")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key2"), []byte("value21")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key2"), []byte("value22")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key3"), []byte("value31")); err != nil {
		return false, err
	}
	return true, nil
}
```

![vis6_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis6_0.png)

It appears that the `DupSort` feature of LMDB has been inherited from its predecessor, BerkleyDB, in somewhat more restricted form. This feature allows having multiple values per key (therefore `Dup`). In BerkleyDB, values for such "duplicated" keys could be stored either unsorted or sorted, but in LMDB they are always stored as sorted. We suspect that the main motivation for bringing this feature to LMDB is simpler implementation of secondary indices. If you use a table for mapping `Key => (Attr1, Attr2, Attr3)`, and then you want to have secondary index `Attr2 => Key`, it is very likely that for many values of `Attr2` there will be more than one matching `Key`s. So you can use `DupSort` feature to store such index. LMDB will not automatically keep secondary indices consistent with the "primary" table though.

The picture simply demonstrates a key with a single value (`key3`), a duplicated key with two values (`key2`), and a duplicated key with three values (`key1`). All three reside on the same page.

There is, however, one restriction about `DupSort` tables that make them relevant to our challenges with freelists and overflow pages. The size of any value associated with a key in such a table may not exceed 512 bytes. This means that for `DupSort` tables, overflow pages never occur. This might actually be the reason for introducing such restriction - overflow pages might have been too complicated to combine with large number of values for the same duplicated key, as shown in the next example.

## One DupSort table, with a large number of values for one of the keys
Test code:
```
func generate6(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	c := tx.CursorDupSort("t")
	defer c.Close()
	for i := 0; i < 1000; i++ {
		v := fmt.Sprintf("dupval_%05d", i)
		if err := c.AppendDup([]byte("key1"), []byte(v)); err != nil {
			return false, err
		}
	}
	if err := c.AppendDup([]byte("key2"), []byte("value21")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key2"), []byte("value22")); err != nil {
		return false, err
	}
	if err := c.AppendDup([]byte("key3"), []byte("value31")); err != nil {
		return false, err
	}
	return true, nil
}
```
The difference between this one and the previous illustration is that `key1` has `1000` values instead of three. This number of values does not fit in a single page, so this demonstrates what LMDB does in such cases:

![vis7_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis7_0.png)

It creates a sub-database for every such key. In this example, the sub-database for `key1` has one branch page and a few leaf nodes. It is important to note that sub-database are only creates when values for one key do not fit in a single page. In this sense, sub-database is similar to the overflow pages. However, the crucial difference is that, since every individual value cannot be more than 512 bytes long, LMDB does not require for any pages of such sub-databases to be laid out on the consecutive pages. This makes DupSort sub-databases "exempt" from the challenges with the freelists that very long values have (due to overflow pages).

## One table with 1000 records, then cleared out
Test code is the sequence of two operations, first to generate 1000 records, using the same code as before, but called with `entries = 1000`:
```
func generate2(tx ethdb.Tx, entries int) error {
	c := tx.Cursor("t")
	defer c.Close()
	for i := 0; i < entries; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Append([]byte(k), []byte("very_short_value")); err != nil {
			return err
		}
	}
	return nil
}
```
and the second - to clear the table:
```
func dropT(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	if err := tx.(ethdb.BucketMigrator).ClearBucket("t"); err != nil {
		return false, err
	}
	return true, nil
}
```
So in this example we have two pictures, one before the clearing out:

![vis8_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis8_0.png)

and another - after the clearing out:

![vis8_1](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis8_1.png)

The first picture looks similar to what we have seen before (but this time 1000 records instead of 200 records), but the second one displays a larger freelist than we have seen before. The syntax is similar to how you would specify pages when you print a document, there are intervals and individual pages separated by commas. In this example, `15,13-2(12)` means that there are 13 free pages in this list, with IDs `15, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2`. First thing to notice is that pages are listed in the reverse order. This is how LMDB sorts them (we don't know why). Second thing to notice is that after each interval, like `13-2` there is the size of that interval in parenthesis, like `(12)`. The number in parenthesis becomes important when the freelist gets used for recycling pages. If we, for example, would require a consecutive run of 13 or 14 pages (for overflowing values), we would not be able to use this freelist, but if we only required 5 consecutive pages, then we could take pages `13, 12, 11, 10, 9`, and reduce the remaining freelist to `15,8-2(7)`.

Where did the page 14 go? It is the one which is represented by the yellow box, now containing info about the empty table `t`.

## Two tables with 1000 records each, then one of the tables cleared out
As before, the test code is the sequence of two operations:
```
func generate7(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	c1 := tx.Cursor("t1")
	defer c1.Close()
	c2 := tx.Cursor("t2")
	defer c2.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c1.Append([]byte(k), []byte("very_short_value")); err != nil {
			return false, err
		}
		if err := c2.Append([]byte(k), []byte("very_short_value")); err != nil {
			return false, err
		}
	}
	return true, nil
}
```
```
func dropT1(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	if err := tx.(ethdb.BucketMigrator).ClearBucket("t1"); err != nil {
		return false, err
	}
	return true, nil
}
```

And here are the two pictures, one before the clearing, and another - after the clearing:

![vis10_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis10_0.png)

![vis10_1](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis10_1.png)

If we look closely into the test code that inserts 1000 record into two tables, we notice that instead of inserting 1000 into one, and then inserting 1000 into another one, we insert each key/value pair into both tables, and then go to the next pair. This is deliberate. We know that two tables will be residing in different sub-databases, so they will be allocated their own pages. When there is nothing in the freelist, new pages are allocated by extending the database, and allocated page IDs will monotonically increase: `4, 5, 6, 7, ...`. If we were to insert 1000 records into one table first, LMDB would likely to allocate consecutive run of pages to it, for example `4-13` or something. If we, however, insert records in both, one pair at a time, it is more likely that one table will most get odd page IDs, and another one - even page IDs. This is what we want - because we would like to create a so-called "fragmented" freelist, where there aren't many long consecutive runs of page IDs.

Now if one looks at the second picture, after the table `t1` is cleared, it is obvious that we almost succeeded. Most pages previously occupied by the table `t1` is now in the freelist, and the freelist looks fragmented, containing mostly odd page IDs (with some exceptions, like `22` and `6`).

## Two tables with 1000 records each, then both tables cleared out one after another
This example is based on the previous one, but the test code is the sequence of three operations instead of two. Here is the test code of the third operation:
```
func dropT2(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	if err := tx.(ethdb.BucketMigrator).ClearBucket("t2"); err != nil {
		return false, err
	}
	return true, nil
}
```
The two first pictures in this example are identical to the ones in the previous example, therefore here is the third picture showing the database after the table `t2` is also cleared out:

![vis11_2](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis11_2.png)

Here we can see that because clearing out tables `t1` and `t2` happened in two separate transactions, we have two freelists. And, as designed, these freelists are fragmented.

## 100 tables, with 1000 records in each, then half of them is removed
Now we are starting to work with larger-scale examples. The test code is the sequence of three operations. First operation creates 100 tables:
```
func generate8(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.(ethdb.BucketMigrator).CreateBucket(k); err != nil {
			return false, err
		}
	}
	return false, nil
}
```
Second operation populates them with 1000 records each (note that we use the same approach as before to make sure the tables do not occupy consecutive runs of pages):
```
func generate9(tx ethdb.Tx, entries int) error {
	var cs []ethdb.Cursor
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		c := tx.Cursor(k)
		defer c.Close()
		cs = append(cs, c)
	}
	for i := 0; i < entries; i++ {
		k := fmt.Sprintf("%08d", i)
		for _, c := range cs {
			if err := c.Append([]byte(k), []byte("very_short_value")); err != nil {
				return err
			}
		}
	}
	return nil
}
```
The third operation removes half of the tables, all of them with the even numbers in their names. Removal is not performed in one single transaction, instead each table is removed in a separate transaction:
```
func dropGradually(kv ethdb.KV, tx ethdb.Tx) (bool, error) {
	tx.Rollback()
	for i := 0; i < 100; i += 2 {
		k := fmt.Sprintf("table_%05d", i)
		if err := kv.Update(context.Background(), func(tx1 ethdb.Tx) error {
			return tx1.(ethdb.BucketMigrator).DropBucket(k)
		}); err != nil {
			return false, err
		}
	}
	return true, nil
}
```

The two following picture shows the state of the database after the creation and population of the 100 tables with 1000 records each:

![vis12_1](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis12_1.png)

The small freelist with 3 free pages on it occurred because the second operation (populating tables with records) replaced these 3 pages in the `MAIN_DBI` that used to contain the records for the empty tables.

The next picture shows what it looks like after half of the tables were removed, each table in its own transaction:

![vis12_2](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis12_2.png)

Because each table was dropped in a separate transaction, we see quite a few freelists instead of just one large one. However, one may notice that even though we removed 50 tables, there are fewer than 50 freelists (there are about 39). Where is the other freelists go? They were "consumed". The free pages in those freelists were allocated to the subsequent transactions. Notice that the freelist associated with the Tx ID `1000015` has only 3 items in it. The others were already recycled for constant re-writing of the pages of the `MAIN_DBI`.

But we still ended up with a fair number of freelists, and these freelists are quite fragmented. Because these freelists are quite large and they would obstruct the illustration too much, they are shortened and ellipsis `...` are added at the end. The important characteristics of these freelist is displayed though - maximum gap. One can think of this as how large is the run of consecutive page IDs that can be allowed from this freelist. In our example, the max gap is 2 for most of the freelist - so we again succeeded in making them fragmented.

## 100 tables, with 10k records in each, then all of them removed (in a single transaction)
Here is the similar example to the previous one, with two differences. Firstly, we populate each of 100 tables with 10 thousand instead of 1000 records. Secondly, we drop all the tables in one single transaction, instead of one by one. Here is only the test code for the third operation (dropping all tables at once):
```
func dropAll(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("table_%05d", i)
		if err := tx.(ethdb.BucketMigrator).DropBucket(k); err != nil {
			return false, err
		}
	}
	return true, nil
}
```
We will not show pictures for the intermediate stages, only for what happens in the end:

![vis13_2](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis13_2.png)

Because all of the removals happened within a single transaction, all freed pages were ended up clamped into a single freelist. And this freelist is so large, it does not fit into a single 4096 page.

**Q: How many items does a freelist need to have not to fit into a single page?** Most pages have headers, which are 16 bytes long. So out of 4096 bytes, only 4080 are useable by data. Each data node requires 2 byte "node pointer" (these pointers are at the beginning of the page, in the order of sorted keys, right after the header, whereas the actual node data are at the end of the page, in arbitrary order). Further 4 bytes is spent on the data size, 2 bytes on the node flags, and another 2 bytes - on the key size. The key of the freelist records is always 8 byte long (transaction ID), and the actual list of 8-byte pageIDs is always pre-pended by the list size (another 8 bytes). In total, there are `2+4+2+2+8+8=26` bytes are overhead of the node. So only `4080-26=4054` bytes are left for the actual list of page IDs. It can contain maximum of 506 page IDs. LMDB code currently estimates it to 509, because it does not take into account node overhead we calculated above.

## 100 tables, with 300k records in each, then half removed in separate transactions
This is almost a repeat of one of the previous examples, except instead of 10k records per table here we have 300k records per table. As before, we remove half them "gradually", each table in a separate transaction.

We only show the "end result" picture:

![vis14_2](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis14_2.png)

There are couple of interesting things to notice here. Firstly, although removing a table with 300k records would have definitely resulted in freeing more than 509 pages, and therefore, in more than 1 overflow page per freelist, this did not happen here. Only 1 overflow page per freelist. Secondly, a lot of freelist records are associated with transaction IDs smaller than `1000001`. As we mentioned earlier, this may happen, and this is an example of how it happens. Both of these interesting things are the consequences of the fix that we have applied to LMDB. The fix was to not allocate transaction IDs lower than `1000001` to the new writeable transactions. How does this help?

When LMDB is confronted with a very large freelist that needs to be persisted into the `FREE_DBI` sub-database (blue boxes), it tries to split it up into the chunks that would ideally fit into one page. At the same time, it wants to associate each freelist records with the transaction IDs. This is important for maintaining the "MVCC" (Multi Version Concurrency Control) property. LMDB sees the transaction IDs allocated to the existing readers of the database, and it ensures that no writers can disrupt their view of the database. "Disrupting their view of the database" means potentially modifying the pages that the reader may still find (observe). But if there are no active readers for any transaction IDs lower than `X`, then any specific transaction ID `< X` loses its meaning, and those those IDs can be used arbitrarily, they won't affect any readers. Knowing that, LMDB tries to "spread" freelist to as many "past" transaction IDs as it can. If `X=10`, for example, there there are only 9 possible Tx IDs to spread things to. So, if LMDB has a very large freelist (say, 900k items), spreading them across 9 records will still result in very large records, and extensive use of overflow pages. If however, `X = 1000010` instead (after our fix), then those 900k items can be "comfortably" spread such that each records has at most 509 items.

This is why in the picture above one sees those two interesting things: freelists do not use more than 1 overflow page each, and transaction IDs lower than `1000001` are being used.

## One table with 1000 records with flip-flops (without reader)
The test code for this example executes the sequence of 4 types of operations. First type was described previously, by the function `generate2`, which appends specified number of records into the table `t`. For this example we set `entries = 1000` to append 1000 records.
Next, the values in all records are being over-written by the function `change1`:
```
func change1(tx ethdb.Tx) (bool, error) {
	c := tx.Cursor("t")
	defer c.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Put([]byte(k), []byte("another_short_value_1")); err != nil {
			return false, err
		}
	}
	return true, nil
}
```
Functions `change2` and `change3` are almost identical to `change1`, the only difference are the values that is set for all 1000 records, these are `another_short_value_2` and `another_short_value_3` correspondingly.
```
func change2(tx ethdb.Tx) (bool, error) {
	c := tx.Cursor("t")
	defer c.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Put([]byte(k), []byte("another_short_value_2")); err != nil {
			return false, err
		}
	}
	return true, nil
}
func change3(tx ethdb.Tx) (bool, error) {
	c := tx.Cursor("t")
	defer c.Close()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%05d", i)
		if err := c.Put([]byte(k), []byte("another_short_value_3")); err != nil {
			return false, err
		}
	}
	return true, nil
}
```
The reason why this example is called "with flip-flops", because the sequence in which the operations are performed invokes `change2` and `change3` in a flip-flop fashion 3 times one after another, like this:
```
if err := defragSteps("vis15", oneBucketCfg,
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return true, generate2(tx, 1000) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change1(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change2(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change3(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change2(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change3(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change2(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change3(tx) },
	); err != nil {
		return err
	}
```
Below is the sequence of pictures showing the state of the database after each operation:

**AFTER `generate2` V**
![vis15_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_0.png)

**AFTER `change1` V**
![vis15_1](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_1.png)

**AFTER `change2` V**
![vis15_2](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_2.png)

**AFTER `change3` V**
![vis15_3](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_3.png)

**AFTER `change2` V**
![vis15_4](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_4.png)

**AFTER `change3` V**
![vis15_5](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_5.png)

**AFTER `change2` V**
![vis15_6](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_6.png)

**AFTER `change3` V**
![vis15_7](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis15_7.png)

The main observation these pictures should illustrate is that there is only one freelist at a time. Its composition changes, but it never goes from 1 records to more records. This is because, in the absence of any readers, the writeable transactions are free to recycle the pages that were freed by the previously committed transaction. For example, if one looks at the penultimate picture, the freelist created by the transaction `1000008` is `37-36(2),29-14(16),2`. Those pages are recycled by the transaction in the very last picture. This can be seen by observing the brown box (branch page) that lists IDs `15`, `16`, ..., `28`, `29`, which are IDs corresponding to the leaf pages (green boxed on the last picture). There page IDs were clearly in the freelist on the penultimate picture, and they have been recycled straight after. This is where we have made another modification to LMDB. Previously, it only allowed the free pages created by the pre-previous transaction to be recycled. If we created the same pictures with un-modified version of LMDB, they would have 2 freelist records in most pictures, instead of one. Our modification speeds up the recycling of the free pages, and so makes the growth of the freelists in the absence of readers slower.

## One table with 1000 records with flip-flops (with a reader)
This example is almost identical to the previous one. The only difference is that there is a reader (modelled by a go-routine), which is "connected" to the database after `change1`. This reader must observe values `another_short_value_1` until it disconnects, regardless of what the latest values are (they are flip-flopping between `another_short_value_2` and `another_short_value_3` three times).
Although the readers connect (creates a read-only transaction) after `change1`, it does not start to actually reading until after all the flip-flops. And then `checkReader` function verified that the reader has indeed seen the values `another_short_value_1` for all 1000 records). Here is how this is orchestrated:
```
if err := defragSteps("vis16", oneBucketCfg,
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return true, generate2(tx, 1000) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change1(tx) },
		func(kv ethdb.KV, tx ethdb.Tx) (bool, error) {
			return launchReader(kv, tx, "another_short_value_1", readerStartCh, readerErrorCh)
		},
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change2(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change3(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change2(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change3(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change2(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) { return change3(tx) },
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
			return startReader(tx, readerStartCh)
		},
		func(_ ethdb.KV, tx ethdb.Tx) (bool, error) {
			return checkReader(tx, readerErrorCh)
		},
	); err != nil {
		return err
	}
```
And below are the sequence of the pictures that are similar to the sequence of picture in the previous example:

**AFTER `generate2` V**
![vis16_0](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_0.png)

**AFTER `change1` V**
![vis16_1](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_1.png)

**AFTER `change2` V**
![vis16_3](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_3.png)

**AFTER `change3` V**
![vis16_4](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_4.png)

**AFTER `change2` V**
![vis16_5](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_5.png)

**AFTER `change3` V**
![vis16_6](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_6.png)

**AFTER `change2` V**
![vis16_7](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_7.png)

**AFTER `change3` V**
![vis16_8](https://github.com/ledgerwatch/erigon/blob/main/docs/lmdb/vis16_8.png)

The difference between these pictures and the ones in the previous example is the growing freelists. The reader was connected after `change1`, and that reader's transaction ID was `1000003` (the transaction ID of a connecting reader is the same as the transaction ID of the last committed transaction at the moment of connection). The transaction that committed `change1` was `1000003`, we can see this on the second picture. Due to the presence of the reader with Tx ID `1000003`, no freelist associated with transaction IDs higher than that, could be recycled, until the reader disconnects. But note that the freelist associated with the Tx ID `1000003` itself, can be recycled. This is the change from the original version of LMDB, in which that freelist would also be untouched. We produced this illustration and also extra tests to convince ourselves that this modification indeed makes sense and is safe.
