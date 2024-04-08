package bn256

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestG1Marshal(t *testing.T) {
	_, ga, err := RandomG1(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ma := ga.Marshal()

	gb := new(G1)
	_, err = gb.Unmarshal(ma)
	if err != nil {
		t.Fatal(err)
	}
	mb := gb.Marshal()

	if !bytes.Equal(ma, mb) {
		t.Fatal("bytes are different")
	}
}

func TestG2Marshal(t *testing.T) {
	_, Ga, err := RandomG2(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ma := Ga.Marshal()

	Gb := new(G2)
	_, err = Gb.Unmarshal(ma)
	if err != nil {
		t.Fatal(err)
	}
	mb := Gb.Marshal()

	if !bytes.Equal(ma, mb) {
		t.Fatal("bytes are different")
	}
}

func TestBilinearity(t *testing.T) {
	for i := 0; i < 2; i++ {
		a, p1, _ := RandomG1(rand.Reader)
		b, p2, _ := RandomG2(rand.Reader)
		e1 := Pair(p1, p2)

		e2 := Pair(&G1{curveGen}, &G2{twistGen})
		e2.ScalarMult(e2, a)
		e2.ScalarMult(e2, b)

		if *e1.p != *e2.p {
			t.Fatalf("bad pairing result: %s", e1)
		}
	}
}

func TestTripartiteDiffieHellman(t *testing.T) {
	a, err := rand.Int(rand.Reader, Order)
	require.NoError(t, err)
	b, err := rand.Int(rand.Reader, Order)
	require.NoError(t, err)
	c, err := rand.Int(rand.Reader, Order)
	require.NoError(t, err)

	pa, pb, pc := new(G1), new(G1), new(G1)
	qa, qb, qc := new(G2), new(G2), new(G2)

	_, err = pa.Unmarshal(new(G1).ScalarBaseMult(a).Marshal())
	require.NoError(t, err)
	_, err = qa.Unmarshal(new(G2).ScalarBaseMult(a).Marshal())
	require.NoError(t, err)
	_, err = pb.Unmarshal(new(G1).ScalarBaseMult(b).Marshal())
	require.NoError(t, err)
	_, err = qb.Unmarshal(new(G2).ScalarBaseMult(b).Marshal())
	require.NoError(t, err)
	_, err = pc.Unmarshal(new(G1).ScalarBaseMult(c).Marshal())
	require.NoError(t, err)
	_, err = qc.Unmarshal(new(G2).ScalarBaseMult(c).Marshal())
	require.NoError(t, err)

	k1 := Pair(pb, qc)
	k1.ScalarMult(k1, a)
	k1Bytes := k1.Marshal()

	k2 := Pair(pc, qa)
	k2.ScalarMult(k2, b)
	k2Bytes := k2.Marshal()

	k3 := Pair(pa, qb)
	k3.ScalarMult(k3, c)
	k3Bytes := k3.Marshal()

	if !bytes.Equal(k1Bytes, k2Bytes) || !bytes.Equal(k2Bytes, k3Bytes) {
		t.Errorf("keys didn't agree")
	}
}

func BenchmarkG1(b *testing.B) {
	x, _ := rand.Int(rand.Reader, Order)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		new(G1).ScalarBaseMult(x)
	}
}

func BenchmarkG2(b *testing.B) {
	x, _ := rand.Int(rand.Reader, Order)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		new(G2).ScalarBaseMult(x)
	}
}
func BenchmarkPairing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Pair(&G1{curveGen}, &G2{twistGen})
	}
}
