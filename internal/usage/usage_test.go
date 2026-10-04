package usage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 成功/失败尝试计数、total 的 pt+ct 兜底口径、按域/账号聚合。
func TestAddAndTotals(t *testing.T) {
	r := New("")
	now := time.Now()
	r.Add(now, "cn", "uid1", "glm-5.2", Delta{PromptTokens: 100, HasPromptTokens: true, CompletionTokens: 50, HasCompletion: true, LatencyMs: 200, HasLatency: true}, true)
	// 失败尝试：无 usage → 只计请求数与失败数，token 不加。
	r.Add(now, "global", "uid1", "claude-4.6", Delta{}, false)
	// 上游没给 total 时用 pt+ct 兜底，保证总量口径连续。
	r.Add(now, "cn", "uid1", "glm-5.2", Delta{PromptTokens: 10, HasPromptTokens: true, CompletionTokens: 5, HasCompletion: true}, true)

	s := r.Snapshot(24, nil)
	if s.Totals.Requests != 3 || s.Totals.Errors != 1 {
		t.Fatalf("requests/errors = %d/%d, want 3/1", s.Totals.Requests, s.Totals.Errors)
	}
	if s.Totals.PromptTokens != 110 || s.Totals.CompletionTok != 55 {
		t.Fatalf("pt/ct = %d/%d, want 110/55", s.Totals.PromptTokens, s.Totals.CompletionTok)
	}
	if s.Totals.TotalTokens != 165 {
		t.Fatalf("tt = %d, want 165（无 total 时按 pt+ct 兜底）", s.Totals.TotalTokens)
	}
	if s.Totals.AvgLatencyMs != 200 {
		t.Fatalf("avg latency = %v, want 200", s.Totals.AvgLatencyMs)
	}
	if len(s.ByRealm) != 2 {
		t.Fatalf("by_realm = %d 项, want 2", len(s.ByRealm))
	}
	if s.ByAccount[0].Realm == "" {
		t.Fatal("by_account 行缺 realm 标注")
	}
}

// Rollup 把超出 hourlyKeep 的小时桶折叠为日桶，且幂等：重复折叠不重复计数。
// 窗口口径：24h 窗口不含 100 天前的日桶；hours=0（全部历史）才含日点。
func TestRollupIdempotent(t *testing.T) {
	r := New("")
	old := time.Now().AddDate(0, 0, -100) // 100 天前，超出 90 天小时保留
	r.Add(old, "cn", "u", "m", Delta{PromptTokens: 7, HasPromptTokens: true}, true)
	r.Add(old, "cn", "u", "m", Delta{PromptTokens: 7, HasPromptTokens: true}, true)
	r.Add(time.Now(), "cn", "u", "m", Delta{PromptTokens: 1, HasPromptTokens: true}, true)

	r.Rollup(time.Now())
	after := r.Snapshot(24, nil)
	if after.Totals.Requests != 1 || after.Totals.PromptTokens != 1 {
		t.Fatalf("24h 窗口 totals = %d/%d, want 1/1（窗口外日桶不进聚合）", after.Totals.Requests, after.Totals.PromptTokens)
	}
	if len(after.Series) != 1 || after.Series[0].Scope != "hour" {
		t.Fatalf("series = %+v, want 仅当前小时 1 个点", after.Series)
	}

	all := r.Snapshot(0, nil)
	if all.Totals.Requests != 3 || all.Totals.PromptTokens != 15 {
		t.Fatalf("全部历史 totals = %d/%d, want 3/15", all.Totals.Requests, all.Totals.PromptTokens)
	}
	if len(all.Series) != 2 || all.Series[0].Scope != "day" || all.Series[1].Scope != "hour" {
		t.Fatalf("series = %+v, want 日点在前 + 小时点在后", all.Series)
	}

	r.Rollup(time.Now())
	again := r.Snapshot(0, nil)
	if again.Totals.Requests != 3 || again.Totals.PromptTokens != 15 {
		t.Fatalf("二次折叠后 totals = %d/%d, want 3/15（幂等被破坏）", again.Totals.Requests, again.Totals.PromptTokens)
	}
}

// 落盘→新实例恢复，数据不丢；落盘结构带版本号。
func TestFlushLoadRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.json")
	r1 := New(path)
	r1.Add(time.Now(), "cn", "u1", "glm-5.2", Delta{PromptTokens: 42, HasPromptTokens: true, TotalTokens: 42, HasTotal: true}, true)
	r1.Save()

	r2 := New(path)
	s := r2.Snapshot(24, nil)
	if s.Totals.Requests != 1 || s.Totals.TotalTokens != 42 {
		t.Fatalf("恢复后 totals = %d/%d, want 1/42", s.Totals.Requests, s.Totals.TotalTokens)
	}
	raw, _ := os.ReadFile(path)
	var f file
	if err := json.Unmarshal(raw, &f); err != nil || f.Version != 1 || len(f.Buckets) != 1 {
		t.Fatalf("落盘文件异常: err=%v buckets=%d", err, len(f.Buckets))
	}
}

// Snapshot 全口径窗口过滤：窗口外的数据不进**任何**聚合（卡片/表格/时序），
// 切窗口数字随之变化；hours=0 全部历史。Buckets 为窗口内命中的桶数。
func TestSnapshotWindowFilter(t *testing.T) {
	r := New("")
	now := time.Now()
	r.Add(now.Add(-48*time.Hour), "cn", "u", "m", Delta{PromptTokens: 5, HasPromptTokens: true}, true) // 窗口(24h)外
	r.Add(now, "cn", "u", "m", Delta{PromptTokens: 3, HasPromptTokens: true}, true)                    // 窗口内
	s := r.Snapshot(24, nil)
	if s.Totals.Requests != 1 || s.Totals.PromptTokens != 3 {
		t.Fatalf("24h 窗口 totals = %d/%d, want 1/3（48h 前的数据应被过滤）", s.Totals.Requests, s.Totals.PromptTokens)
	}
	if len(s.Series) != 1 || s.Series[0].Scope != "hour" || s.Series[0].PromptTokens != 3 {
		t.Fatalf("series = %+v, want 仅窗口内 1 个小时点", s.Series)
	}
	if s.Buckets != 1 {
		t.Fatalf("buckets = %d, want 1（窗口内命中桶数）", s.Buckets)
	}

	all := r.Snapshot(0, nil)
	if all.Totals.Requests != 2 || all.Totals.PromptTokens != 8 {
		t.Fatalf("全部历史 totals = %d/%d, want 2/8", all.Totals.Requests, all.Totals.PromptTokens)
	}
	// since 是全库数据起点，不受窗口影响。
	if all.Since == "" || s.Since != all.Since {
		t.Fatalf("since 应为全库起点且不随窗口变化: all=%q windowed=%q", all.Since, s.Since)
	}
}

// 点数聚合进既有口径（Snapshot 不再内联明细）。
func TestCreditAgg(t *testing.T) {
	r := New("")
	now := time.Now()
	r.Add(now, "cn", "u1", "glm-5.2", Delta{PromptTokens: 100, HasPromptTokens: true, CompletionTokens: 50, HasCompletion: true, LatencyMs: 200, HasLatency: true, Credit: 1.5, HasCredit: true}, true)
	r.Add(now.Add(time.Second), "cn", "u1", "glm-5.2", Delta{Credit: 2.0, HasCredit: true}, true)
	// 失败尝试：无 usage/无 credit，但明细仍要出现（重试放大要看得见）。
	r.Add(now.Add(2*time.Second), "global", "u2", "claude-4.6", Delta{}, false)

	s := r.Snapshot(24, nil)
	if s.Totals.Credits != 3.5 {
		t.Fatalf("totals credits = %v, want 3.5", s.Totals.Credits)
	}
	if len(s.ByModel) != 2 {
		t.Fatalf("by_model = %d 项, want 2", len(s.ByModel))
	}
	// 按模型点数聚合（glm-5.2 两次共 3.5）。
	for _, m := range s.ByModel {
		if m.Key == "glm-5.2" && m.Credits != 3.5 {
			t.Fatalf("by_model glm-5.2 credits = %v, want 3.5", m.Credits)
		}
	}
}

// 明细分页：时间倒序、默认页大小 100、size 越界钳到上限、越界页返回空。
func TestRequestsPaging(t *testing.T) {
	r := New("")
	now := time.Now()
	r.Add(now, "cn", "u1", "glm-5.2", Delta{PromptTokens: 100, HasPromptTokens: true, CompletionTokens: 50, HasCompletion: true, Credit: 1.5, HasCredit: true}, true)
	r.Add(now.Add(time.Second), "global", "u2", "claude-4.6", Delta{Credit: 2.0, HasCredit: true}, true)
	r.Add(now.Add(2*time.Second), "cn", "u3", "hy3-x", Delta{}, false)

	p := r.Requests(1, 0)
	if p.Size != defaultPageSize || p.Total != 3 || len(p.Items) != 3 {
		t.Fatalf("第一页 = size %d total %d items %d, want %d/3/3", p.Size, p.Total, len(p.Items), defaultPageSize)
	}
	// 时间倒序：最新在最前。
	if p.Items[0].Model != "hy3-x" || p.Items[2].Model != "glm-5.2" {
		t.Fatalf("明细顺序错误（应时间倒序）: %+v", p.Items)
	}
	if !p.Items[1].HasCred || p.Items[1].Credit != 2.0 {
		t.Fatalf("items[1] credit = %v/%v, want 2.0/true", p.Items[1].Credit, p.Items[1].HasCred)
	}
	if p.Items[2].TT != 150 {
		t.Fatalf("items[2] total = %d, want 150（无 total 时 pt+ct 兜底）", p.Items[2].TT)
	}
	if p.Items[0].OK || p.Items[0].HasCred {
		t.Fatalf("失败尝试应 ok=false 且无 credit: %+v", p.Items[0])
	}
	// 第二页起为空（总数不足一页），但 total 仍报真实总数。
	if p2 := r.Requests(2, 100); len(p2.Items) != 0 || p2.Total != 3 {
		t.Fatalf("第二页 items = %d total = %d, want 0/3", len(p2.Items), p2.Total)
	}
	if p3 := r.Requests(99, 100); len(p3.Items) != 0 {
		t.Fatalf("越界页应返回空, got %d 条", len(p3.Items))
	}
	if p4 := r.Requests(1, 10000); p4.Size != maxPageSize {
		t.Fatalf("size 越界 = %d, want %d", p4.Size, maxPageSize)
	}
}

// 明细环形按 recentCap 截断：超出后淘汰最旧条目，只保留最近 recentCap 条。
func TestRecentRingCap(t *testing.T) {
	r := New("")
	now := time.Now()
	for i := range recentCap + 50 {
		r.Add(now.Add(time.Duration(i)*time.Millisecond), "cn", "u1", fmt.Sprintf("m%05d", i), Delta{}, false)
	}
	p := r.Requests(1, defaultPageSize)
	if p.Total != recentCap {
		t.Fatalf("total = %d, want %d", p.Total, recentCap)
	}
	// 第一页第一条 = 全库最新（第 recentCap+50-1 次写入）。
	if got := p.Items[0].Model; got != fmt.Sprintf("m%05d", recentCap+49) {
		t.Fatalf("最新条 = %q, want m%05d", got, recentCap+49)
	}
	// 最旧保留的那条落在最后一页（第 51 次写入）。
	last := r.Requests(recentCap/defaultPageSize, defaultPageSize)
	oldest := last.Items[len(last.Items)-1]
	if got := oldest.Model; got != "m00050" {
		t.Fatalf("最旧保留条 = %q, want m00050（应淘汰前 50 条）", got)
	}
}

// Stop 触发最终落盘（Start 后未到防抖间隔也要落）。
func TestLifecycleFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.json")
	r := New(path)
	r.Start()
	r.Add(time.Now(), "cn", "u", "m", Delta{PromptTokens: 9, HasPromptTokens: true}, true)
	r.Stop()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Stop 后应有落盘文件: %v", err)
	}
}
