# 示例：文档检查、归档与预览

这个示例使用自定义类型 `document.inspect`。检查服务返回页数，归档服务保留版本化 PDF 引用，预览服务只使用页数和标题。两个接收方用各自的队列和业务库。业务写入以处理任务的 `task_id` 为主键，提交事务后再 ACK。

## 开通

在 API、独立调度器和管理员 CLI 使用相同的 `DELIVERY_CONFIG_JSON`。用 Alembic 初始化或升级 MySQL 后，在部署环境执行以下命令。

```bash
export DELIVERY_CONFIG_JSON='{"task_types":[{"business_id":"editorial","task_type":"document.inspect","queue":"documents.inspect"}]}'

python -m app.admin create-service author --business editorial
python -m app.admin grant author submit --business editorial --task-type document.inspect
python -m app.admin grant author task_read --business editorial
python -m app.admin create-service inspector
python -m app.admin grant inspector pull --business editorial --queue documents.inspect
python -m app.admin create-service archive
python -m app.admin create-service preview
python -m app.admin open-subscription editorial document.inspect archive archive documents.archive
python -m app.admin open-subscription editorial document.inspect preview preview documents.preview
```

分别运行 `python -m app.admin issue <service>`，将输出中的 `token` 保存到对应服务的部署 Secret。以下示例使用 `AUTHOR_TOKEN`、`INSPECTOR_TOKEN`、`ARCHIVE_TOKEN` 和 `PREVIEW_TOKEN` 引用这些凭证。将 `SCHEDULER_URL` 设为调度器的 HTTPS 地址。使用本机明文服务时，在启动服务时设定 `REQUIRE_HTTPS=false`。

## HTTP/JSON

提交纯摘要任务。

```bash
curl -fsS "$SCHEDULER_URL/api/v1/tasks" \
  -H "Authorization: Bearer $AUTHOR_TOKEN" -H 'Content-Type: application/json' \
  --data '{"task_type":"document.inspect","lease_seconds":1200,"payload":{"title":"Report","pages":["A","B"]}}'

curl -fsS "$SCHEDULER_URL/api/v1/tasks/pull" \
  -H "Authorization: Bearer $INSPECTOR_TOKEN" -H 'Content-Type: application/json' \
  --data '{"queue":"documents.inspect","worker_id":"inspect-1","batch_size":1}'
```

领取响应给出 `task_id` 和 `lease_generation`。以下命令以首次领取为例，实际回执应使用响应中的代次和同一 Worker 实例 ID。只交付摘要时省略 `artifacts`。同时交付 PDF 时，在同一次 ACK 增加 `"artifacts":[{"storage":"https","uri":"https://example.com/report.pdf","meta":{"version":"v1"}}]`。

```bash
curl -fsS "$SCHEDULER_URL/api/v1/tasks/$TASK_ID/ack" \
  -H "Authorization: Bearer $INSPECTOR_TOKEN" -H 'Content-Type: application/json' \
  --data '{"queue":"documents.inspect","worker_id":"inspect-1","lease_generation":1,"summary":{"title":"Report","page_count":2}}'

curl -fsS "$SCHEDULER_URL/api/v1/tasks/$TASK_ID/deliveries" -H "Authorization: Bearer $AUTHOR_TOKEN"
curl -fsS "$SCHEDULER_URL/api/v1/tasks/$TASK_ID/artifacts" -H "Authorization: Bearer $AUTHOR_TOKEN"
```

两个下游分别向 `documents.archive` 和 `documents.preview` 发起同样的 pull。每条处理任务有独立的 `task_id`，并携带相同的 `source_task_id` 和 `completion_id`。完成业务写入后，用处理任务自己的 ID、队列、实例 ID 和代次确认。当实例在写入后退出，接手实例用同一处理任务 ID 去重。

## Python SDK

从仓库根目录构建独立 wheel，在业务环境中安装，然后用该环境运行示例。

```bash
uv build --wheel sdk/python
python -m pip install sdk/python/dist/coffee_scheduler_worker-0.1.0-py3-none-any.whl

SCHEDULER_TOKEN="$AUTHOR_TOKEN" python examples/document_worker.py submit
SCHEDULER_TOKEN="$AUTHOR_TOKEN" python examples/document_worker.py submit --with-artifact
SCHEDULER_TOKEN="$INSPECTOR_TOKEN" python examples/document_worker.py inspect --worker-id inspect-1
SCHEDULER_TOKEN="$ARCHIVE_TOKEN" python examples/document_worker.py archive --worker-id archive-1
SCHEDULER_TOKEN="$PREVIEW_TOKEN" python examples/document_worker.py preview --worker-id preview-1
```

每个 Worker 命令可加 `--once` 执行一条已就绪任务。归档和预览可用 `--database` 指定持久业务库文件，同一服务的示例实例使用同一文件。可在预览继续运行时关闭归档进程，再启动归档进程处理积压。
