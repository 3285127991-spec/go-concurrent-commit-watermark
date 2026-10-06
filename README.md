# commitwatermark

并发安全的 Commit Watermark Tracker（Go 库 + 最小 CLI + 测试），无第三方依赖。

## 语义

- \`NewTracker(start)\` 从 \`start\` 开始签发 ID；初始 watermark 为 \`start-1\`。
- \`Issue()\` 返回严格递增、不重复的 ID。
- \`Complete(id)\` 允许乱序到达；watermark 只越过已连续完成的 ID。
  例如 1、3 完成时 watermark 停在 1，2 完成后一次推进到 3。
- \`Wait(ctx, id)\` 阻塞到 watermark >= id；\`ctx\` 取消只影响当前调用。
- \`Close()\` 后 \`Issue\`/\`Complete\` 返回 \`ErrClosed\`，
  仍在等待的 \`Wait\` 被唤醒并返回 \`ErrClosed\`。

非法操作返回可 \`errors.Is\` 判断的错误：\`ErrNotIssued\`（未签发 ID）、
\`ErrAlreadyCompleted\`（重复完成）、\`ErrClosed\`（已关闭）。

## API

\`\`\`go
tr := commitwatermark.NewTracker(1)
id, _ := tr.Issue()
_ = tr.Complete(id)
_ = tr.Wait(ctx, id)        // 阻塞至 committed >= id
n := tr.Committed()         // 当前 watermark
snap := tr.Snapshot()       // {Committed, LastIssued, Pending, Waiters} 独立副本
_ = tr.Close()
\`\`\`

## 运行

\`\`\`sh
go test -race ./...     # 运行全部测试（含竞态检测）
go run ./cmd/wtdemo     # 演示乱序完成、watermark 跳跃、Wait 与 Close
\`\`\`

CLI 输出示例：

\`\`\`
issued: [1 2 3 4 5]
complete(3) -> committed=0
complete(1) -> committed=1
complete(2) -> committed=3
complete(4) -> committed=4
wait(4) returned: <nil>
wait(5) after close: commitwatermark: tracker closed
final snapshot: {Committed:4 LastIssued:5 Pending:[] Waiters:0}
\`\`\`
