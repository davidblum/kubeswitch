package store

import (
	"testing"

	storetypes "github.com/danielfoehrkn/kubeswitch/pkg/store/types"
	"github.com/danielfoehrkn/kubeswitch/types"
)

func TestDeriveStoreName(t *testing.T) {
	tests := []struct {
		name     string
		store    storetypes.KubeconfigStore
		path     string
		expected string
	}{
		{
			name:     "uses store ID when set",
			store:    &fakeStore{id: strPtr("my-store"), kind: "filesystem"},
			path:     "/home/user/.kube/production/config",
			expected: "my-store",
		},
		{
			name:     "filesystem falls back to parent directory name when ID nil",
			store:    &fakeStore{id: nil, kind: "filesystem"},
			path:     "/home/user/.kube/production/config",
			expected: "production",
		},
		{
			name:     "filesystem falls back to parent directory name when ID empty",
			store:    &fakeStore{id: strPtr(""), kind: "filesystem"},
			path:     "/home/user/.kube/production/config",
			expected: "production",
		},
		{
			name:     "non-filesystem falls back to store kind",
			store:    &fakeStore{id: nil, kind: "vault"},
			path:     "vault/secret/path",
			expected: "vault",
		},
		{
			name:     "nested filesystem path without ID",
			store:    &fakeStore{id: nil, kind: "filesystem"},
			path:     "/home/user/.kube/team/project/config",
			expected: "project",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DeriveStoreName(tt.store, tt.path)
			if got != tt.expected {
				t.Errorf("DeriveStoreName() = %q, want %q", got, tt.expected)
			}
		})
	}
}

type fakeStore struct {
	storetypes.KubeconfigStore
	id   *string
	kind string
}

func (f *fakeStore) GetKind() types.StoreKind {
	return types.StoreKind(f.kind)
}

func strPtr(s string) *string { return &s }

func (f *fakeStore) GetStoreConfig() types.KubeconfigStore {
	return types.KubeconfigStore{
		ID:   f.id,
		Kind: types.StoreKind(f.kind),
	}
}
