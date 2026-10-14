package host

import (
	"bytes"
	"errors"
	"io"
	"os"
	"syscall"

	"github.com/0xdeafcafe/rush/internal/agent"
)

// The daemon admits at most 4 images of 2 MiB. These bounds only keep one
// send from holding unbounded memory; 5 MiB is the provider's own image limit.
const (
	APIImageCount = 8
	APIImageBytes = 5 << 20
)

var apiImageMagic = map[string][]byte{
	"image/jpeg": {0xFF, 0xD8, 0xFF},
	"image/png":  {0x89, 'P', 'N', 'G', '\r', '\n', 0x1A, '\n'},
}

// apiReadImages reads every image into memory, in order, and checks it before
// any content is submitted. Errors never name the path, and nothing returned
// keeps it: the daemon deletes the file once the message leaves its queue.
func apiReadImages(images []RuntimeImage) ([]agent.Image, error) {
	var out []agent.Image
	for _, image := range images {
		if err := image.Validate(); err != nil {
			return nil, err
		}
		b, err := apiReadImage(image.Path)
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(b, apiImageMagic[image.MediaType]) {
			return nil, errors.New("runtime: image bytes do not match its media type")
		}
		out = append(out, agent.Image{MediaType: image.MediaType, Data: b})
	}
	return out, nil
}

// apiReadImage opens without following a final symlink and without blocking
// on a pipe, then refuses anything but a nonempty regular file within limit.
func apiReadImage(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("runtime: image file is unreadable")
	}
	defer f.Close()
	if info, err := f.Stat(); err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("runtime: image is not a plain file")
	}
	b, err := io.ReadAll(io.LimitReader(f, APIImageBytes+1))
	switch {
	case err != nil:
		return nil, errors.New("runtime: image file is unreadable")
	case len(b) == 0:
		return nil, errors.New("runtime: image file is empty")
	case len(b) > APIImageBytes:
		return nil, errors.New("runtime: image file exceeds limit")
	}
	return b, nil
}
