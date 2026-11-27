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

package downloadercfg

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTTLDNSDialerNoAddresses(t *testing.T) {
	const host = "no-address.example"
	d := &TTLDNSDialer{
		cache: map[string]ttlEntry{
			host: {expiry: time.Now().Add(time.Minute)},
		},
	}

	conn, err := d.DialContext(t.Context(), "tcp", net.JoinHostPort(host, "80"))
	if conn != nil {
		require.NoError(t, conn.Close())
	}
	require.Nil(t, conn)
	require.Error(t, err)
	var dnsErr *net.DNSError
	require.ErrorAs(t, err, &dnsErr)
	require.Equal(t, host, dnsErr.Name)
	require.True(t, dnsErr.IsNotFound)
}
