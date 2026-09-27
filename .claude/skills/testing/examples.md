# Testing examples

Sketches written against the APIs in ARCHITECTURE.md. Adjust names to match the code as it exists.

## 1. TDD: policy as spec sentences (layer 1)

Rows encode STRATEGY §3 ordering, not tuned constants.

```go
func TestPick(t *testing.T) {
	warm := func(id uint16) Candidate {
		return Candidate{TargetID: id, Warm: Some(true), FreeSlots: 1,
			PromptTokens: 2000, PrefillSecTok: Some(0.001), ServiceSec: Some(5.0)}
	}
	cold := func(id uint16) Candidate {
		c := warm(id)
		c.Warm, c.LoadSec, c.FitsIfCold = Some(false), Some(20.0), Some(true)
		return c
	}
	tests := []struct {
		name  string
		cands []Candidate
		want  uint16
	}{
		{"warm_beats_cold", []Candidate{cold(1), warm(2)}, 2},
		{"cached_prefix_beats_uncached_when_both_warm", []Candidate{
			warm(1), func() Candidate { c := warm(2); c.MatchedTokens = 1800; return c }(),
		}, 2},
		// Rows that expect "no target" assert that explicitly. Never use ID 0 as a sentinel; 0 is a valid target ID.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Pick(RouteReq{}, tt.cands, DefaultConfig())
			if d.TargetID != tt.want {
				t.Fatalf("picked %d, want %d; reason=%q scores=%v", d.TargetID, tt.want, d.Reason, d.Scores)
			}
		})
	}
}
```

Merge laws with `testing/quick`:

```go
func TestMergeOrderIndependent(t *testing.T) {
	f := func(ops []PrefixOp, seed int64) bool {
		a, b := New(), New()
		a.Merge(ops)
		shuffled := shuffle(ops, seed)
		b.Merge(append(shuffled, shuffled...)) // reordered + duplicated
		return reflect.DeepEqual(a.Export(), b.Export())
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}
```

## 2. Engine path: spike → live test → fixture → replay

### Spike (scratchpad, not committed)

```sh
docker run -d -p 8000:8000 <vllm-cpu-image>:<pinned> --model <tiny>
for i in 1 2 3 4; do curl -s localhost:8000/v1/completions -d '{"model":"<tiny>","prompt":"...","max_tokens":200}' & done
curl -s localhost:8000/metrics | grep -E 'num_requests_(running|waiting)|kv_cache'
```

Finding: "`vllm:num_requests_running` reached 4 while 4 requests were in flight on <version>. `gpu_cache_usage_perc` is absent." That's now a fact for this version. Everything else stays [U].

### Layer 4: assert behavior

```go
//go:build integration

func TestVLLMRunningTracksConcurrency(t *testing.T) {
	base := engineURL(t, "vllm") // skips if the compose service isn't up
	const n = 4
	stop := startSlowRequests(t, base, n) // long max_tokens, streaming
	defer stop()

	snap := eventually(t, 10*time.Second, func() (engine.Snapshot, bool) {
		_, s, err := engine.Resolve(ctx(t), http.DefaultClient, base, engine.VLLM, nil) // no own probes
		// Running is verified for this version, so unknown fails here as well.
		return s, err == nil && s.Load[""].Running.OK && s.Load[""].Running.V == n
	})
	_ = snap
	// When PHAROS_RECORD=1, save the RAW body of every path in the vLLM recipe (404s included)
	// and meta.yaml to testdata/vllm/<version>/loaded/. Never save the parsed Snapshot:
	// replaying the parser's own output back into it proves nothing.
	recordRaw(t, base, engine.VLLM, "loaded")
}
```

`eventually` polls with a deadline. It's the only acceptable wait, and only in layer 4.

### Layer 2: replay the recording

```go
// Every library HTTP probe runs against every capture that contains its path, from any engine.
// Log probes get the same loop over engine.log, one line at a time through Plan.Follow.
func TestProbesReplay(t *testing.T) {
	for _, dir := range captureDirs(t, "testdata") { // <engine>/<version>/<state>
		for _, p := range engine.Library {
			body, ok := readCapture(dir, p.Feed.Path)
			if !ok {
				continue
			}
			t.Run(dir+"/"+p.Name, func(t *testing.T) {
				got, err := p.Parse(body)
				if err != nil {
					t.Fatal(err)
				}
				// Expected values live in this Go file, not in testdata. No entry means
				// the probe must leave its signal unknown on this capture.
				want := expected[dir][p.Name]
				if diff := cmpSignal(p.Signal, want, got); diff != "" {
					t.Fatal(diff)
				}
			})
		}
	}
}

// Resolve against a whole capture: which probes the plan keeps, and the merged snapshot.
// For a replaced probe, the old version's capture keeps the old probe and the new one keeps the new.
func TestResolveReplay(t *testing.T) {
	for _, dir := range captureDirs(t, "testdata") {
		t.Run(dir, func(t *testing.T) {
			srv := serveCapture(t, dir) // replays recorded status + body per path
			plan, s, err := engine.Resolve(context.Background(), srv.Client(), srv.URL, kindOf(dir), nil)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := activeNames(plan), expectedPlan[dir]; !slices.Equal(got, want) {
				t.Fatalf("active probes %v, want %v; dropped=%v", got, want, plan.Dropped)
			}
			if diff := cmpSnap(expectedMerged[dir], s); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestVLLMRunningProbeEdges(t *testing.T) {
	tests := []struct {
		name    string
		in      string // trimmed from a real capture
		running Opt[int]
		wantErr bool
	}{
		{"missing_metric_is_unknown_not_zero", "vllm:num_requests_waiting 0\n", Opt[int]{}, false},
		{"reported_zero_is_known_zero", "vllm:num_requests_running 0\n", Opt[int]{V: 0, OK: true}, false},
		{"present_but_garbage_is_error", "vllm:num_requests_running abc\n", Opt[int]{}, true},
	}
	// ...
}
```

## 3. Component: wiring with fake engines (layer 3)

The fake engine serves recorded fixtures for probe endpoints. Only its latency/cache model is synthetic.

```go
func TestSecondTurnFollowsPrefixAcrossInstances(t *testing.T) {
	e1 := fakeengine.New(t, fakeengine.FromFixtures("../engine/testdata/llamacpp/<ver>/loaded"),
		fakeengine.PrefillPerTok(time.Millisecond)) // synthetic, labeled
	e2 := fakeengine.New(t, /* same */)
	cluster := newCluster(t, 3, e1, e2) // 3 pharos instances, in-process transport

	turn1 := chat("sys", "long prompt ...")
	cluster.Instance(0).Do(t, turn1)
	got := cluster.Instance(1).Do(t, append(turn1, "user", "follow-up"))

	if got.Target != cluster.LastTarget(0) {
		t.Fatalf("turn 2 went to %s, want the prefix holder %s", got.Target, cluster.LastTarget(0))
	}
}
```

## 4. Spike path: tuning, no tests yet

Question: "What default `prefillSecPerTok` should an Apple M2 Ollama target get before any samples?"

- Run 20 real prompts of varying length against the box and log TTFT versus uncached tokens.
- Fit a line. Write the number and the method into the config default's comment.
- Delete the script. Test only the *behavior* (with no samples, the policy uses the fleet median and then the config default), not the number.

## 5. Bug fix at the lowest reproducing layer

Report: "Status page shows 0 running on SGLang 0.x".

1. Capture the real `/metrics` from that version (`pharos doctor --record`). Suspect a prefix change (`sglang_` vs `sglang:`).
2. Add the capture as `testdata/sglang/<version>/loaded/` and run the replay tests. They fail: `Running` is read as known 0 instead of the real value.
3. Add a newer probe for the observed name ahead of the old one in the SGLang recipe. Replay passes on both the old and new captures, each keeping one probe in its plan. Layer 4 for that version confirms it, and the support matrix cell for that version is updated.
4. Until a release ships the probe, the affected team can add the same probe to that backend's `probes:` in config.
