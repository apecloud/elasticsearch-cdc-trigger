# Smoke Test Cases

这份文档提供一组最小可执行的 `curl` 测试用例，用来快速验证：

- 源端 trigger 是否能写入 `ape_es_trigger_idx`
- ES6 DDL 事件是否能产出 `CI` / `DI`
- sink 是否能从源端拉取并在目标端回放

下面的例子以：

- 源端 ES：`http://127.0.0.1:9200`
- 目标端 ES：`http://127.0.0.1:9201`
- 测试索引：`cdc_smoke_idx`
- ES6 type：`doc`

为例。

如果你的环境有用户名密码，可以在命令中追加：

```shell
-u user:password
```

## 准备

确认插件配置已经完成：

- 源端已安装 `es-trigger-es6`
- 目标端已安装 `es-sink-es6`
- 源端 `node.cdc_enable_idxs` 已命中 `cdc_smoke_idx`
- 目标端已配置好 source 地址和账密

定义变量：

```shell
export SRC_ES=http://127.0.0.1:9200
export TGT_ES=http://127.0.0.1:9201
export IDX=cdc_smoke_idx
export TYPE=doc
```

## 1. 打开目标端 sink

```shell
curl -X PUT "$TGT_ES/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{
    "persistent": {
      "node.source_trigger_enabled": true
    }
  }'
```

检查目标端 sink 状态索引：

```shell
curl "$TGT_ES/ape_es_trigger_sink_state/_search?pretty"
```

## 2. 清理旧测试数据

源端和目标端都清理：

```shell
curl -X DELETE "$SRC_ES/$IDX?pretty"
curl -X DELETE "$TGT_ES/$IDX?pretty"
```

可选：清理历史 trigger 数据，避免结果干扰。

```shell
curl -X POST "$SRC_ES/ape_es_trigger_idx/_delete_by_query?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "term": {
        "idx_name.keyword": "'"$IDX"'"
      }
    }
  }'
```

如果你的 `idx_name` 没有 `.keyword`，可以改成：

```json
{
  "query": {
    "term": {
      "idx_name": "cdc_smoke_idx"
    }
  }
}
```

## 3. CASE 1: CREATE_INDEX

在源端创建索引并显式打开 DML CDC：

```shell
curl -X PUT "$SRC_ES/$IDX?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "settings": {
      "index.number_of_shards": 1,
      "index.number_of_replicas": 0,
      "index.cdc_enabled": true
    },
    "mappings": {
      "'"$TYPE"'": {
        "properties": {
          "name": { "type": "keyword" },
          "age": { "type": "integer" }
        }
      }
    }
  }'
```

检查源端 trigger index 中是否出现 `CI`：

```shell
curl -X GET "$SRC_ES/ape_es_trigger_idx/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "sort": [
      { "scn": "asc" }
    ],
    "query": {
      "bool": {
        "must": [
          { "term": { "idx_name": "'"$IDX"'" } },
          { "term": { "event_type": "ci" } }
        ]
      }
    }
  }'
```

检查目标端索引是否已创建：

```shell
curl "$TGT_ES/$IDX?pretty"
```

预期：

- 源端 `ape_es_trigger_idx` 中出现一条 `event_type=ci`
- 目标端出现同名索引

## 4. CASE 2: INSERT

```shell
curl -X PUT "$SRC_ES/$IDX/$TYPE/1?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "alice",
    "age": 18
  }'
```

检查源端 trigger index：

```shell
curl -X GET "$SRC_ES/ape_es_trigger_idx/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "sort": [
      { "scn": "asc" }
    ],
    "query": {
      "bool": {
        "must": [
          { "term": { "idx_name": "'"$IDX"'" } },
          { "term": { "event_type": "i" } },
          { "term": { "pk": "1" } }
        ]
      }
    }
  }'
```

检查目标端文档：

```shell
curl "$TGT_ES/$IDX/$TYPE/1?pretty"
```

预期：

- 源端出现一条 `I`
- 目标端能查到 `_id=1`

## 5. CASE 3: UPDATE

```shell
curl -X PUT "$SRC_ES/$IDX/$TYPE/1?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "alice",
    "age": 20
  }'
```

检查源端 trigger index：

```shell
curl -X GET "$SRC_ES/ape_es_trigger_idx/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "sort": [
      { "scn": "desc" }
    ],
    "size": 5,
    "query": {
      "bool": {
        "must": [
          { "term": { "idx_name": "'"$IDX"'" } },
          { "term": { "event_type": "u" } },
          { "term": { "pk": "1" } }
        ]
      }
    }
  }'
```

检查目标端文档：

```shell
curl "$TGT_ES/$IDX/$TYPE/1?pretty"
```

预期：

- 源端出现一条 `U`
- 目标端文档 `age=20`

## 6. CASE 4: DELETE

```shell
curl -X DELETE "$SRC_ES/$IDX/$TYPE/1?pretty"
```

检查源端 trigger index：

```shell
curl -X GET "$SRC_ES/ape_es_trigger_idx/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "sort": [
      { "scn": "desc" }
    ],
    "size": 5,
    "query": {
      "bool": {
        "must": [
          { "term": { "idx_name": "'"$IDX"'" } },
          { "term": { "event_type": "d" } },
          { "term": { "pk": "1" } }
        ]
      }
    }
  }'
```

检查目标端文档：

```shell
curl "$TGT_ES/$IDX/$TYPE/1?pretty"
```

预期：

- 源端出现一条 `D`
- 目标端 `_id=1` 查询结果为 not found

## 7. CASE 5: DELETE_INDEX

```shell
curl -X DELETE "$SRC_ES/$IDX?pretty"
```

检查源端 trigger index 中是否出现 `DI`：

```shell
curl -X GET "$SRC_ES/ape_es_trigger_idx/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "sort": [
      { "scn": "desc" }
    ],
    "size": 5,
    "query": {
      "bool": {
        "must": [
          { "term": { "idx_name": "'"$IDX"'" } },
          { "term": { "event_type": "di" } }
        ]
      }
    }
  }'
```

检查目标端索引：

```shell
curl "$TGT_ES/$IDX?pretty"
```

预期：

- 源端 `ape_es_trigger_idx` 中出现一条 `DI`
- 目标端索引不存在

## 8. 顺序检查

验证同一个新索引里，`CI` 的 `scn` 早于首条 `I`：

```shell
curl -X GET "$SRC_ES/ape_es_trigger_idx/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d '{
    "sort": [
      { "scn": "asc" }
    ],
    "_source": ["scn", "idx_name", "event_type", "pk"],
    "query": {
      "bool": {
        "must": [
          { "term": { "idx_name": "'"$IDX"'" } },
          { "terms": { "event_type": ["ci", "i"] } }
        ]
      }
    }
  }'
```

预期：

- `CI` 的 `scn` 小于首条 `I`

## 9. 常见排查

如果源端没有写出 trigger 记录，优先检查：

- 源端插件是否安装在所有节点
- `node.cdc_enable_idxs` 是否命中测试索引
- 测试索引是否显式配置了 `index.cdc_enabled=false`
- `node.trigger_idx_host/user/password` 是否能让插件写回本地 `ape_es_trigger_idx`

如果源端有 trigger 记录但目标端没回放，优先检查：

- `node.source_trigger_enabled` 是否已打开
- 目标端 `ape_es_trigger_sink_state` 里是否有错误信息
- `node.source_trigger_idx_host/user/password` 是否正确
- 源端 `ape_es_trigger_idx` 是否存在
