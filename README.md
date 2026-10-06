# commitwatermark

并发安全的 Commit Watermark Tracker。纯 Go 标准库实现，无第三方依赖，只包含库、最小演示 CLI 和自动化测试。

## 语义

- `Issue` 返回严格递增、不重复的 ID（从构造时指定的起始序号开始）。
- `Complete(id)` 允许乱序到达；`Committed` 只表示已连续完成的最大 ID。
  例如 1、3 完成时水位停在 1，2 完成后一次性推进到 3。
- 重复完成返回 `ErrAlreadyCompleted`，完成未签发 ID 返回 `ErrNotIssued`。
- `Wait(ctx, id)` 阻塞到水位 >= id；多个 waiter 可并发等待不同目标，`ctx` 取消只影响自身。
- `Close` 后 `Issue`/`Complete` 返回 `ErrClosed`，所有阻塞中的 `Wait` 被唤醒并返回 `ErrClosed`。
- `Snapshot` 返回当前水位、最后签发 ID、乱序待提交 ID 列表和 waiter 数量的独立副本。

## API

| 方法 | 说明 |
| --- | --- |
| `New(start uint64) (*Tracker, error)` | 创建 Tracker，首个签发 ID 为 `start`（须 >= 1），初始水位 `start-1` |
| `Issue() (uint64, error)` | 签发下一个 ID |
| `Complete(id uint64) error` | 标记完成并尽可能推进水位 |
| `Wait(ctx context.Context, id uint64) error` | 等待水位 >= id |
| `Committed() uint64` | 当前水位 |
| `Snapshot() Snapshot` | 状态快照（独立副本） |
| `Close()` | 关闭 Tracker，幂等 |

## 示例

```go
tr, _ := commitwatermark.New(1)
a, _ := tr.Issue() // 1
b, _ := tr.Issue() // 2
c, _ := tr.Issue() // 3

_ = tr.Complete(a) // 水位 1
_ = tr.Complete(c) // 水位仍为 1（2 未完成）
_ = tr.Complete(b) // 水位跳到 3

_ = tr.Wait(context.Background(), 3) // 立即返回 nil
tr.Close()
```

## 运行

```sh
go test -race ./...          # 测试（含竞态检测）
go run ./cmd/watermark-demo  # CLI 演示：乱序完成、水位跳跃、Wait、Close
```
