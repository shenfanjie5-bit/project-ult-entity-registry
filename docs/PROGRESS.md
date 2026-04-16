# entity-registry 项目进度总览

> 最后更新：2026-04-17

## 里程碑概览

| 阶段 | 里程碑 | Issues | 状态 | 退出条件 |
|------|--------|--------|------|----------|
| 阶段 0 | P1 最小实体锚点 | ISSUE-001, ISSUE-002 | 🔲 未开始 | 全部 A 股上市公司有正式 canonical_entity_id |
| 阶段 1 | P1-P2 确定性解析 | ISSUE-003, ISSUE-004 | 🔲 未开始 | 主系统和图谱能稳定读取 ENT_* 锚点 |
| 阶段 2 | P4 模糊解析与 LLM 辅助 | ISSUE-005, ISSUE-006 | 🔲 未开始 | 复杂新闻/公告 mention 能进入完整解析链 |
| 阶段 3 | P4-P5 批量回补与人工复核 | ISSUE-007, ISSUE-008 | 🔲 未开始 | 未解析引用不再静默堆积 |

## Issue 详细状态

| Issue | 标题 | 优先级 | 里程碑 | 依赖 | 状态 |
|-------|------|--------|--------|------|------|
| ISSUE-001 | 项目基础设施与核心领域对象模型 | P0 | milestone-0 | 无 | 🔲 未开始 |
| ISSUE-002 | stock_basic 初始化管线与别名生成 | P0 | milestone-0 | #001 | 🔲 未开始 |
| ISSUE-003 | 别名查表与确定性匹配引擎 | P1 | milestone-1 | #002 | 🔲 未开始 |
| ISSUE-004 | 实体画像查询与 resolve_mention 确定性路径 | P1 | milestone-1 | #003 | 🔲 未开始 |
| ISSUE-005 | HanLP NER 集成与 Splink 模糊候选生成 | P2 | milestone-2 | #004 | 🔲 未开始 |
| ISSUE-006 | reasoner-runtime LLM 辅助消歧与完整解析链 | P2 | milestone-2 | #005 | 🔲 未开始 |
| ISSUE-007 | 批量解析与未解析引用回补 | P2 | milestone-3 | #006 | 🔲 未开始 |
| ISSUE-008 | 人工复核队列与解析审计链 | P2 | milestone-3 | #007 | 🔲 未开始 |

## 依赖链

```
ISSUE-001 (核心模型)
  └── ISSUE-002 (stock_basic 初始化)
        └── ISSUE-003 (确定性匹配)
              └── ISSUE-004 (画像查询 + resolve_mention 确定性)
                    └── ISSUE-005 (HanLP + Splink 模糊)
                          └── ISSUE-006 (LLM 辅助消歧)
                                └── ISSUE-007 (批量回补)
                                      └── ISSUE-008 (人工复核)
```

## 关键指标红线

| 指标 | 红线 | 验证阶段 |
|------|------|----------|
| alias 解析延迟 | < 50ms（纯查表路径） | 阶段 1 |
| mention resolution 平均耗时 | < 2 秒 | 阶段 2 |
| A+H 错误合并率 | 0（零容忍） | 阶段 0 起持续验证 |
| 裸文本进入 formal 链路 | 0（零容忍） | 阶段 1 起持续验证 |
| unresolved 误判为 resolved | < 1% | 阶段 2 |
| A 股上市公司覆盖率 | 100% | 阶段 0 |
