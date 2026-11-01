# Agent instructions

- Do not add AI/Claude attribution anywhere:
  - no "🤖 Generated with Claude Code" or similar lines in pull request descriptions;
  - no `Co-Authored-By: Claude <noreply@anthropic.com>` trailer in commit messages.
- Commits and pull requests should list only the repository owner as author.

## Pre-commit mandatory checks (run before every commit)

All three must pass, do not commit if any fails:

```bash
gofmt -l . && echo "✅ gofmt 通过"
go vet ./... && echo "✅ go vet 通过"
go test ./... && echo "✅ go test 通过"
```

<!-- ## PR 后日志（每次提交 PR 后执行）

每提交一个 PR，就把这次排查和实现的真实过程追加到 `日志-导出.md`，用来积累工程经验：

- 在索引表追加一行：日期、Issue、一句话结论、PR 编号、详情链接。
- 在文件末尾追加 `devlog/NNN-<slug>.md` 段落，沿用现有结构：一句话 / 现象与复现 / 根因 / 方案取舍 / 实现要点 / 验证 / 收获 / 面试怎么讲 / 关联。
- 同步更新文件开头的拆分清单。
- 记录真实过程，包括 review 中发现的问题和返工，不要只抄最终 diff。
- PR 未合并时写“已提交/等待评审”，合并后再更新状态。 -->
