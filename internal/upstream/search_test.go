// search_test.go /v1/search 上游侧单测：请求形态/响应解析/错误分类 + 倍率缓存升序。
package upstream

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

func TestParseCreditRate(t *testing.T) {
	cases := []struct {
		raw  string
		want float64
		ok   bool
	}{
		{"x0.05", 0.05, true},
		{"x0.00 credits", 0, true},
		{"x0.29", 0.29, true},
		{"X0.10", 0.10, true},
		{"0.05 credits", 0.05, true},
		{"", math.Inf(1), false},        // 空 → 未知
		{"credits", math.Inf(1), false}, // 无数字 → 未知
		{"free", math.Inf(1), false},    // 非数字 → 未知
		{"x-1", math.Inf(1), false},     // 负数拒绝（不编造）
	}
	for _, c := range cases {
		got, ok := parseCreditRate(c.raw)
		if ok != c.ok {
			t.Errorf("parseCreditRate(%q) ok=%v want %v", c.raw, ok, c.ok)
			continue
		}
		if got != c.want {
			t.Errorf("parseCreditRate(%q) = %v want %v", c.raw, got, c.want)
		}
	}
}

func TestSearchRequestAndParsing(t *testing.T) {
	var gotPath, gotMethod, gotAuth, gotCT, gotBody string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		return jsonResp(200, `{"query":"go 1.23","type":"text2text","provider":"0",
			"results":[{"title":"t1","url":"u1","snippet":"s1"}]}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", Domain: "copilot.tencent.com"}
	out, err := c.Search(context.Background(), a, "go 1.23", 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if gotMethod != "POST" || gotPath != agenttoolSearchPath {
		t.Errorf("method/path = %s %s want POST %s", gotMethod, gotPath, agenttoolSearchPath)
	}
	if gotAuth != "Bearer at" {
		t.Errorf("Authorization=%q want Bearer at", gotAuth)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type=%q want application/json", gotCT)
	}
	wantBody := `{"query":"go 1.23","type":"text2text","max_results":5}`
	if gotBody != wantBody {
		t.Errorf("body=%s want %s", gotBody, wantBody)
	}
	if out.Provider != "0" || len(out.Results) != 1 || out.Results[0].Title != "t1" || out.Results[0].URL != "u1" {
		t.Errorf("parsed=%+v", out)
	}
}

func TestSearchMaxResultsCap(t *testing.T) {
	var gotBody string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		return jsonResp(200, `{"provider":"0","results":[]}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1"}
	if _, err := c.Search(context.Background(), a, "q", 100); err != nil {
		t.Fatalf("search: %v", err)
	}
	if !strings.Contains(gotBody, `"max_results":20`) {
		t.Errorf("body=%s want max_results capped at 20", gotBody)
	}
}

func TestSearchErrorStatuses(t *testing.T) {
	// 400 + 上游信封 → 分类 *Error；401 网关 HTML → 分类 *Error（非 SessionDead）。
	cases := []struct {
		status int
		body   string
		kind   ErrKind
	}{
		{400, `{"code":15003,"msg":"query is required"}`, ErrClient},
		{401, `<html>401 Authorization Required</html>`, ErrClient},
		{500, `boom`, ErrServer},
	}
	for _, tc := range cases {
		c := testClient(func(r *http.Request) (*http.Response, error) {
			return jsonResp(tc.status, tc.body), nil
		})
		_, err := c.Search(context.Background(), &auth.Auth{AccessToken: "at"}, "q", 5)
		if err == nil {
			t.Errorf("status %d: want error", tc.status)
			continue
		}
		var ue *Error
		if !errors.As(err, &ue) {
			t.Errorf("status %d: want *upstream.Error, got %T", tc.status, err)
			continue
		}
		if ue.Kind != tc.kind {
			t.Errorf("status %d: kind=%v want %v", tc.status, ue.Kind, tc.kind)
		}
	}
}

func TestRefreshModelRatesSortsAscending(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v3/config"):
			return jsonResp(200, `{"code":0,"data":{"models":[
				{"id":"expensive","credits":"x0.29","maxOutputTokens":1000},
				{"id":"free","credits":"x0.00","maxOutputTokens":1000},
				{"id":"cheap","credits":"x0.05","maxOutputTokens":1000},
				{"id":"unknown","maxOutputTokens":1000}
			]}}`), nil
		default:
			// 企业端点 404 不拖累 v3 结果（FetchModels 两路独立容错）。
			return jsonResp(404, `{}`), nil
		}
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1", Domain: "copilot.tencent.com"}
	rates, err := c.RefreshModelRates(a)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	var ids []string
	for _, r := range rates {
		ids = append(ids, r.Model)
	}
	want := []string{"free", "cheap", "expensive", "unknown"} // 升序；费率未知排最后
	if strings.Join(ids, ",") != strings.Join(want, ",") {
		t.Errorf("order=%v want %v", ids, want)
	}
	if rates[3].Rate != math.Inf(1) {
		t.Errorf("unknown rate=%v want +Inf", rates[3].Rate)
	}
	// 快照与缓存一致。
	snap, fetched := ModelRatesSnapshot()
	if len(snap) != len(rates) || fetched.IsZero() {
		t.Errorf("snapshot=%+v fetched=%v", snap, fetched)
	}
}

// errorsAs 局部别名（errors.As），避免测试文件各处 import 差异。
func errorsAs(err error, target any) bool { return errors.As(err, target) }
