package group

import (
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/service"
)

type selectionTestCache struct {
	adapter.CacheFile
	enabled bool
	loads   int
}

func (c *selectionTestCache) StoreSelectedEnabled() bool { return c.enabled }
func (c *selectionTestCache) LoadSelected(string) string {
	c.loads++
	return "remembered"
}

func TestSelectorUsesConfiguredChoiceUnlessSelectionCacheIsEnabled(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		cache := &selectionTestCache{enabled: enabled}
		ctx := service.ContextWith[adapter.CacheFile](t.Context(), cache)
		for _, configured := range []string{"first-connection", "second-connection"} {
			selector := &Selector{
				Adapter:    outbound.NewAdapter(C.TypeSelector, "select", nil, nil),
				ctx:        ctx,
				defaultTag: configured,
				outbounds: map[string]adapter.Outbound{
					configured:   &urlTestSelectionOutbound{tag: configured},
					"remembered": &urlTestSelectionOutbound{tag: "remembered"},
				},
			}
			selected, err := selector.outboundSelect()
			if err != nil {
				t.Fatal(err)
			}
			want := configured
			if enabled {
				want = "remembered"
			}
			if selected.Tag() != want {
				t.Fatalf("store_selected=%v: got %s, want %s", enabled, selected.Tag(), want)
			}
		}
		if !enabled && cache.loads != 0 {
			t.Fatal("disabled selection cache was read")
		}
	}
}
