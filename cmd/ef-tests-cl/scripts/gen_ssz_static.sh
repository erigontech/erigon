#! /bin/bash



find tests | grep "ssz_static" | cut -d'/' -f5 | sort | uniq | awk '{print "With(\""$1"\", getSSZStaticConsensusTest(&cltypes."$1"{}))."}'
