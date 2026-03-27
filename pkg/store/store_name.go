package store

import (
	"path/filepath"

	storetypes "github.com/danielfoehrkn/kubeswitch/pkg/store/types"
	"github.com/danielfoehrkn/kubeswitch/types"
)

// DeriveStoreName returns a human-readable name for the store that provided
// the kubeconfig at the given path. Priority: store ID > parent directory
// (filesystem) > store kind.
func DeriveStoreName(s storetypes.KubeconfigStore, path string) string {
	if cfg := s.GetStoreConfig(); cfg.ID != nil && *cfg.ID != "" {
		return *cfg.ID
	}
	if s.GetKind() == types.StoreKindFilesystem {
		return filepath.Base(filepath.Dir(path))
	}
	return string(s.GetKind())
}
