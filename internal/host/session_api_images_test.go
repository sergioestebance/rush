package host

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var (
	apiTestJPEG = []byte{0xFF, 0xD8, 0xFF, 0xE0, 'j', 'p', 'e', 'g'}
	apiTestPNG  = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n', 'p', 'n', 'g'}
)

func TestRuntimeSendRequestImagesShape(t *testing.T) {
	conversation, _ := NewSessionID()
	operation, _ := NewSessionID()
	hostIncarnation, _ := NewSessionID()
	agentIncarnation, _ := NewSessionID()
	installation, _ := NewSessionID()
	base := RuntimeSendRequest{OperationID: operation, RequireTurnState: "idle", Text: "Look at [Image #1]",
		Expected: RuntimeIdentity{ConversationID: conversation, HostID: "host", HostIncarnation: hostIncarnation, AgentIncarnation: agentIncarnation,
			Metadata: RuntimeOwnership{Version: "1", Installation: installation, Chat: "chat", Operation: operation}},
		Settings: RuntimeSettings{Model: "opus", Effort: "medium", Mode: "auto"}}
	many := make([]RuntimeImage, APIImageCount+1)
	for i := range many {
		many[i] = RuntimeImage{Path: "/attach/a.png", MediaType: "image/png"}
	}
	for _, tc := range []struct {
		name   string
		text   string
		images []RuntimeImage
		ok     bool
	}{
		{"no images", "hello", nil, true},
		{"jpeg and png", "[Image #1] [Image #2]", []RuntimeImage{{"/attach/a.jpg", "image/jpeg"}, {"/attach/b.png", "image/png"}}, true},
		{"markers only", "[Image #1]", []RuntimeImage{{"/attach/a.jpg", "image/jpeg"}}, true},
		{"blank text with images", "  ", []RuntimeImage{{"/attach/a.jpg", "image/jpeg"}}, false},
		{"gif", "[Image #1]", []RuntimeImage{{"/attach/a.gif", "image/gif"}}, false},
		{"no media type", "[Image #1]", []RuntimeImage{{"/attach/a.jpg", ""}}, false},
		{"relative path", "[Image #1]", []RuntimeImage{{"attach/a.jpg", "image/jpeg"}}, false},
		{"unclean path", "[Image #1]", []RuntimeImage{{"/attach/../etc/a.jpg", "image/jpeg"}}, false},
		{"nul in path", "[Image #1]", []RuntimeImage{{"/attach/a\x00.jpg", "image/jpeg"}}, false},
		{"too many", "[Image #1]", many, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			r.Text, r.Images = tc.text, tc.images
			if err := r.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok %v", err, tc.ok)
			}
		})
	}
}

func TestAPIReadImagesChecksEveryFileBeforeUse(t *testing.T) {
	root := t.TempDir()
	write := func(name string, b []byte) string {
		t.Helper()
		p := filepath.Join(root, name)
		if err := os.WriteFile(p, b, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	jpeg := write("a.jpg", apiTestJPEG)
	png := write("b.png", apiTestPNG)
	empty := write("empty.png", nil)
	big := write("big.jpg", append(append([]byte{}, apiTestJPEG...), bytes.Repeat([]byte{0}, APIImageBytes)...))
	link := filepath.Join(root, "link.jpg")
	if err := os.Symlink(jpeg, link); err != nil {
		t.Fatal(err)
	}
	folder := filepath.Join(root, "folder.png")
	if err := os.Mkdir(folder, 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		images []RuntimeImage
		want   [][]byte
	}{
		{"in list order", []RuntimeImage{{png, "image/png"}, {jpeg, "image/jpeg"}}, [][]byte{apiTestPNG, apiTestJPEG}},
		{"missing file", []RuntimeImage{{jpeg, "image/jpeg"}, {filepath.Join(root, "gone.jpg"), "image/jpeg"}}, nil},
		{"empty file", []RuntimeImage{{empty, "image/png"}}, nil},
		{"jpeg declared as png", []RuntimeImage{{jpeg, "image/png"}}, nil},
		{"png declared as jpeg", []RuntimeImage{{png, "image/jpeg"}}, nil},
		{"over the size limit", []RuntimeImage{{big, "image/jpeg"}}, nil},
		{"symlink", []RuntimeImage{{link, "image/jpeg"}}, nil},
		{"directory", []RuntimeImage{{folder, "image/png"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := apiReadImages(tc.images)
			if tc.want == nil {
				if err == nil || got != nil {
					t.Fatalf("apiReadImages accepted a bad image: %v", err)
				}
				if strings.Contains(err.Error(), root) {
					t.Fatalf("error leaks the path: %v", err)
				}
				return
			}
			if err != nil || len(got) != len(tc.want) {
				t.Fatalf("apiReadImages() = %d images, %v", len(got), err)
			}
			for i := range got {
				if got[i].MediaType != tc.images[i].MediaType || !bytes.Equal(got[i].Data, tc.want[i]) {
					t.Fatalf("image %d = %s %x", i, got[i].MediaType, got[i].Data)
				}
			}
		})
	}
}
