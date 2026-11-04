package tools

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"unicode/utf16"

	"github.com/jackc/pgx/v5"
)

const (
	resultLimit = 50000
	tailLimit   = 4000
)

const (
	truncationMark = "\n[result truncated: the full result is longer than the 50,000 character limit for MCP tool results; narrow the query or lower the limit]\n"
	lineCutMark    = " [line cut]\n"
)

type output struct {
	redact    redactor
	b         strings.Builder
	tail      []string
	used      int
	truncated bool
}

func (o *output) Line(format string, args ...any) bool {
	return o.add(fmt.Sprintf(format, args...) + "\n")
}

func (o *output) Tail(format string, args ...any) {
	o.tail = append(o.tail, o.redact.apply(fmt.Sprintf(format, args...)+"\n"))
}

func (o *output) JSON(label string, value any) bool {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return o.Line("%s: (could not encode: %v)", label, err)
	}
	if label == "" {
		return o.add(string(encoded) + "\n")
	}
	return o.add(label + ":\n" + string(encoded) + "\n")
}

func (o *output) add(text string) bool {
	if o.truncated {
		return false
	}
	text = o.redact.apply(text)
	room := resultLimit - tailLimit - utf16Len(truncationMark) - o.used
	size := utf16Len(text)
	if size <= room {
		o.b.WriteString(text)
		o.used += size
		return true
	}
	o.b.WriteString(cutUTF16(text, room))
	o.used += room
	o.truncated = true
	return false
}

type jsonRaw []byte

func (r jsonRaw) MarshalJSON() ([]byte, error) { return r, nil }

func compactJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func utf16Len(text string) int {
	n := 0
	for _, r := range text {
		n += utf16.RuneLen(r)
	}
	return n
}

func cutUTF16(text string, limit int) string {
	n := 0
	for i, r := range text {
		size := utf16.RuneLen(r)
		if n+size > limit {
			return text[:i]
		}
		n += size
	}
	return text
}

type redactor struct {
	secrets []string
}

func PasswordIsLoginName(databaseURL string) bool {
	cfg, err := pgx.ParseConfig(databaseURL)
	return err == nil && cfg.Password != "" && (cfg.Password == cfg.User || cfg.Password == cfg.Database)
}

func newRedactor(databaseURL, adminURL, adminToken string) redactor {
	var secrets []string
	add := func(value string) {
		if value != "" {
			secrets = append(secrets, value)
		}
	}
	add(adminToken)
	if adminURL != "" {
		add(strings.TrimRight(adminURL, "/"))
		add(adminURL)
		if parsed, err := url.Parse(adminURL); err == nil {
			add(parsed.Host)
		}
	}
	if cfg, err := pgx.ParseConfig(databaseURL); err == nil && !PasswordIsLoginName(databaseURL) {
		add(cfg.Password)
		add(url.QueryEscape(cfg.Password))
		add(url.PathEscape(cfg.Password))
	}
	for _, secret := range secrets {
		if encoded, err := json.Marshal(secret); err == nil && string(encoded[1:len(encoded)-1]) != secret {
			add(string(encoded[1 : len(encoded)-1]))
		}
	}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	return redactor{secrets: secrets}
}

func (r redactor) apply(text string) string {
	for _, secret := range r.secrets {
		text = strings.ReplaceAll(text, secret, "***")
	}
	return text
}

func (o *output) finish() string {
	body := o.b.String()
	if o.truncated {
		body += truncationMark
	}
	return strings.ToValidUTF8(body+fitTail(o.tail, tailLimit), "?")
}

func fitTail(entries []string, limit int) string {
	sizes := make([]int, len(entries))
	total := 0
	for i, entry := range entries {
		sizes[i] = utf16Len(entry)
		total += sizes[i]
	}
	if total <= limit {
		return strings.Join(entries, "")
	}
	order := make([]int, len(entries))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return sizes[order[a]] < sizes[order[b]] })
	share, room := 0, limit
	for n, i := range order {
		share = room / (len(order) - n)
		if sizes[i] > share {
			break
		}
		room -= sizes[i]
	}
	var fitted strings.Builder
	for i, entry := range entries {
		if sizes[i] <= share {
			fitted.WriteString(entry)
			continue
		}
		if share < utf16Len(lineCutMark) {
			fitted.WriteString(cutUTF16(entry, share))
			continue
		}
		fitted.WriteString(cutUTF16(entry, share-utf16Len(lineCutMark)) + lineCutMark)
	}
	return fitted.String()
}
