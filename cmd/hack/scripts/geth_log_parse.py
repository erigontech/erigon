import datetime

with open('/Users/alexeyakhunov/workspace/src/github.com/erigontech/erigon/badger.log') as f:
    lines = f.readlines()

print(len(lines))

seconds_before_reset = 0
seconds_since_reset = 0
last_reset = None
last_block = 0
trie_nodes = 0
db_size = 0
heap = 0

with open('/Users/alexeyakhunov/workspace/src/github.com/erigontech/erigon/badger.csv', 'w') as w:

    for l in lines:
        if l.startswith('Oct'):
            l = l[49:]
        if l.startswith('INFO ['):
            # Line with date time, parse it INFO [01-29|01:31:18]
            # datetime.datetime(year, month, day[, hour[, minute[, second[, microsecond[, tzinfo]]]]])
            dt = datetime.datetime(2018, int(l[6:8]), int(l[9:11]), int(l[12:14]), int(l[15:17]), int(l[18:20]))
        if 'Block synchronisation started' in l:
            print(l)
            last_reset = dt
            print('Before Reset. Hours before reset:', (seconds_before_reset/3600.0), '. Hours since last reset:', (seconds_since_reset/3600.0))
            seconds_before_reset += seconds_since_reset
            seconds_since_reset = 0
            print('After Reset. Hours before reset:', (seconds_before_reset / 3600.0), '. Hours since last reset:', (seconds_since_reset / 3600.0))
        if ('Memory' in l) and ('nodes=' in l):
            ns = l.index('nodes=') + len('nodes=')
            ne = len(l)
            if ' ' in l[ns:]:
                ne = l.index(' ', ns)
            trie_nodes = int(l[ns:ne])
        if ('Database' in l) and ('size=' in l):
            ns = l.index('size=') + len('size=')
            ws = l.index(' written=', ns)
            s = int(l[ns:ws])
            if s > db_size:
                db_size = s
        if ('Memory' in l) and ('alloc=' in l):
            ns = l.index('alloc=') + len('alloc=')
            ne = l.index(' ', ns)
            heap = int(l[ns:ne])
        if 'Imported new chain segment' in l:
            ns = l.index('number=') + len('number=')
            ne = l.index(' ', ns)
            block = int(l[ns:ne])
            if block > last_block:
                if not last_reset is None:
                    seconds_since_reset = (dt - last_reset).total_seconds()
                print(block/1000000.0, ',', \
                    (seconds_before_reset + seconds_since_reset)/3600.0, ',', \
                    db_size/1024.0/1024.0/1024.0, ',', \
                    trie_nodes/1000000.0, ',', \
                    heap/1024.0/1024.0, file=w)
