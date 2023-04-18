package beacon_changeset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListChangeset(t *testing.T) {
	pre := []int{6, 8, 9}
	changeset := NewListChangeSet[int](3, true)
	changeset.AddChange(1, 45)
	changeset.AddChange(1, 1)
	changeset.AddChange(2, 45)
	require.Equal(t, changeset.list.Len(), 2)
	post, changed := changeset.ApplyChanges(pre)
	require.Equal(t, post, []int{6, 1, 45})
	require.Equal(t, changed, true)
}

func TestListChangesetWithReverse(t *testing.T) {
	pre := []int{6, 8, 9}
	changeset := NewListChangeSet[int](3, false)
	changeset.AddChange(1, 45)
	changeset.AddChange(1, 1)
	changeset.AddChange(2, 45)
	require.Equal(t, changeset.list.Len(), 2)
	post, changed := changeset.ApplyChanges(pre)
	require.Equal(t, post, []int{6, 45, 45})
	require.Equal(t, changed, true)
}
