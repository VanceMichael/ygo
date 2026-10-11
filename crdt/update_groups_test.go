package crdt

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/encoding"
)

// Every group except a client's last is superseded, across bitset words and
// for ordered, reversed and shuffled wires. Groups carry every content kind,
// so a framing slip would misread the following group headers.
func TestUnit_SupersededGroups_MatchLastGroupPerClient(t *testing.T) {
	source := New()
	defer source.Destroy()
	subdoc := New()
	defer subdoc.Destroy()
	contents := []Content{
		NewContentString("А🐷z"), NewContentDeleted(3), NewContentJSON("a", nil, 7), NewContentBinary([]byte{1, 2}),
		NewContentEmbed(map[string]any{"n": []any{1, true}}), NewContentFormat("bold", true),
		NewContentType(&NewMapPrelim().abstractType), NewContentType(&NewYXmlElement("div").abstractType),
		NewContentAny([]any{1, "a"}, map[string]any{"key": 2}), NewContentDoc(subdoc), NewContentMove(&ID{Client: 9}, 2),
	}
	parent := ID{Client: 999}
	rng := rand.New(rand.NewSource(305))
	for _, order := range []string{"ascending", "descending", "repeats", "shuffled"} {
		for _, version := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/V%d", order, version), func(t *testing.T) {
				clients := make([]ClientID, 0, 200)
				for i := 0; i < 200; i++ {
					switch order {
					case "ascending":
						clients = append(clients, ClientID(i+1))
					case "descending":
						clients = append(clients, ClientID(200-i))
					default:
						clients = append(clients, ClientID(1+rng.Intn(80)))
					}
				}
				if order == "shuffled" {
					rng.Shuffle(len(clients), func(i, j int) { clients[i], clients[j] = clients[j], clients[i] })
				}
				groups := make([][]*Item, len(clients))
				for i, c := range clients {
					var clock uint64
					for j := 0; j < 3; j++ {
						key := fmt.Sprint("k", (i+j)%5)
						content := contents[(i*3+j)%len(contents)]
						groups[i] = append(groups[i], &Item{ID: ID{Client: c, Clock: clock}, parentID: &parent, ParentSub: &key, Content: content})
						clock += uint64(content.Len())
					}
				}
				update := pendingGuardUpdate(source, groups, nil, version)
				var v2 *v2Decoder
				var rest encoding.Decoder
				if version == 2 {
					var err error
					v2, err = newV2Decoder(update)
					require.NoError(t, err)
					rest = *v2.restDec
				}
				set, err := supersededGroups(update, v2, rest)
				require.NoError(t, err)
				last := map[ClientID]int{}
				for i, c := range clients {
					last[c] = i
				}
				for i, c := range clients {
					require.Equal(t, last[c] != i, set.has(uint64(i)), "group %d client %d", i, c)
				}
				if order == "ascending" || order == "descending" {
					require.Nil(t, set, "a strictly ordered wire needs no index")
				}
			})
		}
	}
}
