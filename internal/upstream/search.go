// search.go /v1/search 网关的上游侧：agenttool 内置搜索客户端 + 模型倍率缓存。
//
// 搜索端点（2026-09-28 实测）：
//
//	POST https://copilot.tencent.com/agenttool/v1/search   ← chatBase（CN 域）
//	Authorization: Bearer <accessToken>
//	Content-Type: application/json
//
//	{"query": "...", "type": "text2text", "max_results": 5}
//
// 成功响应（HTTP 200，**无统一信封**，顶层平铺——与 doJSON 的 {code,msg,data}
// 信封不同，解析时勿套信封）：
//
//	{"query": "...", "type": "text2text", "provider": "0",
//	 "results": [{"title": "...", "url": "...", "snippet": "..."}]}
//
// 失败响应：400 {"code":15003,"msg":"query is required"}（信封形态）/
// 401 网关 HTML（openresty）——统一走 Classify 分类，pool 据此冷却。
//
// 模型倍率缓存（/v1/search 选号的成本导向路由）：从 FetchModels（CN 目录）
// 解析 credits 字段（"x0.05" → 0.05），按倍率**升序**缓存进程内包级单例；
// 费率解析不出/缺失的模型排最后（未知费率不优先——搜索路由要省积分，但不编造）。
// 包级单例与 catalogState 同模式；handler/cmd/server 负责「何时刷新」（挑号 +
// 定时器），本包只负责「怎么刷」与只读快照。
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// agenttoolSearchPath WorkBuddy 内置搜索端点路径（挂在 chatBase 下，CN 域）。
const agenttoolSearchPath = "/agenttool/v1/search"

// SearchMaxResultsCap 单请求结果数上限（上游不校验上限，网关侧兜底防滥用）。
const SearchMaxResultsCap = 20

// searchDefaultMaxResults 客户端未指定时的结果数（与官方 agenttool 默认一致）。
const searchDefaultMaxResults = 5

// SearchResult 单条搜索结果。
type SearchResult struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet"`
}

// SearchResponse 标准化搜索响应（透给 /v1/search 的响应体字段）。
type SearchResponse struct {
	Query    string         `json:"query"`
	Provider string         `json:"provider"`
	Results  []SearchResult `json:"results"`
}

// searchRequest 上游请求体（type 固定 text2text——网关当前唯一支持的搜索形态）。
type searchRequest struct {
	Query      string `json:"query"`
	Type       string `json:"type"`
	MaxResults int    `json:"max_results"`
}

// Search 调 WorkBuddy 内置搜索（text2text）。
// maxResults 缺省 5、上限 searchMaxResultsCap（客户端未指定/越界时兜底）。
func (c *Client) Search(ctx context.Context, a *auth.Auth, query string, maxResults int) (*SearchResponse, error) {
	if maxResults <= 0 {
		maxResults = searchDefaultMaxResults
	}
	if maxResults > SearchMaxResultsCap {
		maxResults = SearchMaxResultsCap
	}
	body, err := json.Marshal(searchRequest{Query: query, Type: "text2text", MaxResults: maxResults})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.chatBase(a)+agenttoolSearchPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	// AccessToken 加锁快照（见 auth.AccessTokenValue：keepalive 刷新在 a.mu 内改写）。
	if at := a.AccessTokenValue(); at != "" {
		req.Header.Set("Authorization", "Bearer "+at)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if a.UID != "" {
		req.Header.Set("X-User-Id", a.UID)
	}
	if ua := c.userAgent(a); ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	return c.doSearch(req)
}

// doSearch 发请求 + 解析响应。>=400 → *Error（Classify 分类，pool 据此冷却）；
// 200 但无法解析（非法 JSON/空体）→ ErrClient 类 *Error（搜索端点无信封，
// 200 平铺形态，解析失败视为上游异常响应，罚号口径与 4xx 兜底一致）；
// 连接层错误原样返回（传输故障不喂熔断，与 doJSON 同口径）。
func (c *Client) doSearch(req *http.Request) (*SearchResponse, error) {
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, &Error{Kind: Classify(resp.StatusCode, string(raw)), Status: resp.StatusCode, Msg: truncate(string(raw), 200)}
	}
	var out SearchResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, &Error{Kind: Classify(http.StatusBadRequest, string(raw)), Status: resp.StatusCode,
			Msg: "parse failed: " + truncate(string(raw), 120)}
	}
	if out.Results == nil {
		out.Results = []SearchResult{} // null → 空数组（下游遍历安全）
	}
	return &out, nil
}

// ---- 模型倍率缓存（包级单例，catalogState 同模式）----

// SearchRate 单模型倍率条目。Rate=Inf 表示费率未知（排最后，不编造）。
type SearchRate struct {
	Model string
	Rate  float64
}

// parseCreditRate 从上游 credits 原文解析倍率数值："x0.05"/"x0.05 credits" → 0.05。
// 空/无法解析/负数 → (Inf, false)：费率未知排最后（不优先省积分口径，也不编造）。
// 与 handler.fmtCreditsPrefix（展示用原文提取）口径并列：一个出数字一个出文案，
// 各自面向不同消费方，不强行归一。
func parseCreditRate(raw string) (float64, bool) {
	s := strings.TrimSpace(raw)
	s = strings.TrimSuffix(s, "credits")
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "x")
	s = strings.TrimPrefix(s, "X")
	if s == "" {
		return math.Inf(1), false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 {
		return math.Inf(1), false
	}
	return f, true
}

// modelRatesCache 包级单例：全进程一份倍率缓存（rates 恒升序）。
var modelRatesCache struct {
	sync.Mutex
	rates   []SearchRate
	fetched time.Time // 最近一次成功刷新时间（TTL 判定由调用方做）
}

// RefreshModelRates 拉取 CN 模型目录并重建倍率缓存（升序）。
// 成功落缓存并返回快照；失败返回 nil（旧缓存保留，调用方按需回退——
// 目录拉不出即上游不可用，不悄悄伪造空费率名单）。
func (c *Client) RefreshModelRates(a *auth.Auth) ([]SearchRate, error) {
	infos, err := c.FetchModels(a)
	if err != nil {
		return nil, err
	}
	rates := make([]SearchRate, 0, len(infos))
	for _, mi := range infos {
		// 不滤 nonChatModel：搜索端点不吃 model 参数，「模型」只是选号路由键，
		// 免费倍率（x0.00）常挂在非对话模型上，滤掉会把最便宜的键排除。
		r, _ := parseCreditRate(mi.Credits)
		rates = append(rates, SearchRate{Model: mi.ID, Rate: r})
	}
	sort.SliceStable(rates, func(i, j int) bool { return rates[i].Rate < rates[j].Rate })
	modelRatesCache.Lock()
	modelRatesCache.rates = rates
	modelRatesCache.fetched = time.Now()
	modelRatesCache.Unlock()
	return rates, nil
}

// ModelRatesSnapshot 只读倍率快照（升序）与最近一次成功刷新时间。零上游调用。
func ModelRatesSnapshot() ([]SearchRate, time.Time) {
	modelRatesCache.Lock()
	defer modelRatesCache.Unlock()
	return modelRatesCache.rates, modelRatesCache.fetched
}

// ParseCreditRate 测试/外部接线用：credits 原文解析倍率。
func ParseCreditRate(raw string) (float64, bool) { return parseCreditRate(raw) }

// SeedModelRatesForTest 跨包测试钩子：直接灌入倍率缓存（server 包 handler 测试用，
// 绕过 FetchModels 网络路径；与 ResetLookupChainForTest 同模式）。仅测试引用。
func SeedModelRatesForTest(rates []SearchRate) {
	modelRatesCache.Lock()
	modelRatesCache.rates = rates
	modelRatesCache.fetched = time.Now()
	modelRatesCache.Unlock()
}
