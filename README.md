# elasticsearch-cdc-trigger

ElasticSearch CDC plugin，基于 [cloudcanal-es-trigger](https://github.com/ClouGence/cloudcanal-es-trigger) 改造。

当前仓库包含 4 个子模块：

- `es-trigger-es6`
- `es-trigger-es7`
- `es-trigger-es8`
- `es-sink-es6`

版本对应关系：

- `es-trigger-es6`：部署在 ES 6.x 源端集群
- `es-trigger-es7`：部署在 ES 7.x 源端集群
- `es-trigger-es8`：部署在 ES 8.x 源端集群
- `es-sink-es6`：部署在 ES 6.x 目标端集群

## 能力概览

### Trigger

trigger 插件部署在源端，把变更写入触发索引：

- trigger index：`ape_es_trigger_idx`

当前支持：

- `es-trigger-es6`
  - DML：`INSERT` / `UPDATE` / `DELETE`
  - DDL：`CREATE_INDEX` / `DELETE_INDEX`
- `es-trigger-es7`
  - DML：`INSERT` / `UPDATE` / `DELETE`
- `es-trigger-es8`
  - DML：`INSERT` / `UPDATE` / `DELETE`

### Sink

sink 插件部署在目标端，从源端 `ape_es_trigger_idx` 拉取事件并回放：

- sink state index：`ape_es_trigger_sink_state`

当前 `es-sink-es6` 支持回放：

- DML：`INSERT` / `UPDATE` / `DELETE`
- DDL：`CREATE_INDEX` / `DELETE_INDEX`

## 工作方式

整体链路：

```text
source cluster
  -> trigger plugin
  -> ape_es_trigger_idx
  -> es-sink-es6 poll
  -> replay to target es6 cluster
  -> checkpoint saved in ape_es_trigger_sink_state
```

回放规则：

1. sink 从源端 `ape_es_trigger_idx` 轮询事件
2. 按 `scn asc` 拉取
3. 按 `scn asc` 串行回放到目标端
4. checkpoint 写入 `ape_es_trigger_sink_state`

补充说明：

- 真正执行 sink 轮询和回放的是目标端当前 master 节点
- master 切换后，新的 master 会继续从 checkpoint 位置接管

## 构建

先按目标 ES 版本调整 `gradle.properties`：

```properties
cc.es6.version=6.8.23
cc.es7.version=7.10.1
cc.es8.version=8.15.5
```

按需构建：

```shell
sh ./all_build.sh es-trigger-es6
sh ./all_build.sh es-trigger-es7
sh ./all_build.sh es-trigger-es8
sh ./all_build.sh es-sink-es6
```

构建说明：

- `es-trigger-es6` / `es-trigger-es7` / `es-sink-es6` 需要 Java 8+
- `es-trigger-es8` 需要 Java 17+

产物位于各模块的 `build/dist/` 目录。

## 源端 Trigger 配置

### 部署

根据源端 ES 版本选择对应 trigger 模块：

- ES6 源端：`es-trigger-es6`
- ES7 源端：`es-trigger-es7`
- ES8 源端：`es-trigger-es8`

示例：

```shell
scp ./es-trigger-es6/build/dist/es-trigger-es6.zip es@127.0.0.1:${es_home}/plugins
cd ${es_home}/plugins
unzip es-trigger-es6.zip -d ./es-trigger-es6
```

建议在源端集群所有节点安装对应 trigger 插件。

### 配置项梳理

以 `es-trigger-es6/src/main/java/com/clougence/cloudcanal/es6/trigger/CcEs6IdxTriggerPlugin.java` 为准。

| 参数名 | Scope | 默认值 | 建议值 | 作用 | 动态 | 运行时生效方式 |
| --- | --- | --- | --- | --- | --- | --- |
| `node.trigger_idx_host` | `node` | 无 | 源端本集群可访问的 HTTP 地址，例如 `127.0.0.1:9200` | trigger 插件内部写 `ape_es_trigger_idx` 时使用的 HTTP 地址 | 是 | 配置变更后会重建内部 REST client |
| `node.trigger_idx_user` | `node` | 空字符串 | 按实际账号填写 | 写 trigger index 使用的用户名 | 是 | 配置变更后会重建内部 REST client |
| `node.trigger_idx_password` | `node` | 空字符串 | 按实际密码填写 | 写 trigger index 使用的密码 | 是 | 配置变更后会重建内部 REST client |
| `node.cdc_enable_idxs` | `node` | 空 | `*` | node 级默认采集范围，支持 `*`、逗号分隔列表、正则 | 是 | DML 监听和 ES6 DDL 监听都会刷新本地缓存并立即按新规则判断 |
| `index.cdc_enabled` | `index` | 未设置 | 大多数索引不配；仅少数索引显式设 `false` | 单索引 DML 开关，优先级高于 `node.cdc_enable_idxs` | 是 | 对该 index 的后续 DML 判断立即生效 |
| `index.cdc_max_scn` | `index` | 由插件维护 | 不建议人工配置 | trigger index 内部维护的 SCN 设置项 | 是 | 由插件内部读写，不作为业务配置使用 |

补充说明：

- `node.trigger_idx_host` 是插件自己创建的 REST client 使用的地址，不是 ES 节点内部 transport 通道。
- 这里的“动态”不是只指 `Setting.Property.Dynamic`，而是代码里已经实际注册了 update consumer。
- `node.cdc_enable_idxs` 推荐大多数场景直接配置成 `*`，只在少数索引上用 `index.cdc_enabled=false` 排除。
- `index.cdc_enabled` 当前只影响 DML。
- ES6 的索引级 DDL 事件由 `ClusterStateListener` 产出，当前只按 `node.cdc_enable_idxs` 控制，不读取 `index.cdc_enabled`。

`index.cdc_enabled` 的 DML 判定优先级：

1. `index.cdc_enabled=false`：不采集
2. `index.cdc_enabled=true`：采集
3. 未配置时，看 `node.cdc_enable_idxs`
4. `node.cdc_enable_idxs` 未命中：不采集

推荐配置：

```yaml
# elasticsearch.yml
node.trigger_idx_host: "127.0.0.1:9200"
node.trigger_idx_user: ""
node.trigger_idx_password: ""
node.cdc_enable_idxs: "*"
```

推荐做法：

- 默认让 node 级配置采集全部索引
- 对少数不希望记录 DML 的索引，单独关闭

```json
PUT some_index/_settings
{
  "index.cdc_enabled": false
}
```

## 目标端 Sink 配置

### 部署

目标端当前只支持 ES6，因此部署 `es-sink-es6`：

```shell
scp ./es-sink-es6/build/dist/es-sink-es6.zip es@127.0.0.1:${target_es_home}/plugins
cd ${target_es_home}/plugins
unzip es-sink-es6.zip -d ./es-sink-es6
```

建议在目标端 ES6 集群所有节点安装该插件。

### 配置项梳理

以 `es-sink-es6/src/main/java/com/clougence/cloudcanal/es6/sink/CcEs6SinkPlugin.java` 为准。

| 参数名 | Scope | 默认值 | 建议值 | 作用 | 动态 |
| --- | --- | --- | --- | --- | --- |
| `node.source_trigger_enabled` | `node` | `false` | `false`，需要时再动态打开 | sink 总开关，打开后当前 master 节点开始轮询源端 | 是 |
| `node.source_trigger_idx_host` | `node` | 无 | 源端可访问地址，例如 `10.0.0.1:9200` | 源端 ES HTTP 地址，sink 从这里读取 `ape_es_trigger_idx` | 否 |
| `node.source_trigger_idx_user` | `node` | 空字符串 | 按实际账号填写 | 源端用户名 | 否 |
| `node.source_trigger_idx_password` | `node` | 空字符串 | 按实际密码填写 | 源端密码 | 否 |
| `node.source_trigger_idx_name` | `node` | `ape_es_trigger_idx` | `ape_es_trigger_idx` | 源端 trigger index 名称 | 否 |
| `node.source_trigger_start_time` | `node` | 空字符串 | 明确配置一个起始时间 | 首次启动且本地没有 checkpoint 时的起始消费时间 | 否 |
| `node.source_trigger_poll_interval_ms` | `node` | `1000` | `1000` | 正常轮询间隔 | 否 |
| `node.source_trigger_idle_poll_interval_ms` | `node` | `3000` | `3000` | 空轮询间隔 | 否 |
| `node.source_trigger_error_backoff_ms` | `node` | `10000` | `10000` | 异常退避时间 | 否 |
| `node.source_trigger_batch_size` | `node` | `200` | `200` | 单批拉取大小 | 否 |

补充说明：

- `node.source_trigger_enabled` 是 sink 当前唯一有实际热更新语义的配置项。
- 其它 source 地址、账密、index 名、轮询参数修改后，都需要重启目标节点生效。
- `node.source_trigger_start_time` 只在第一次启动且本地还没有 checkpoint 时生效。
- 一旦 `ape_es_trigger_sink_state` 中已有 checkpoint，后续恢复按 checkpoint 继续。

动态打开示例：

```json
PUT /_cluster/settings
{
  "persistent": {
    "node.source_trigger_enabled": true
  }
}
```

推荐配置：

```yaml
# elasticsearch.yml
node.source_trigger_enabled: false
node.source_trigger_idx_host: "10.0.0.1:9200"
node.source_trigger_idx_user: ""
node.source_trigger_idx_password: ""
node.source_trigger_idx_name: "ape_es_trigger_idx"
node.source_trigger_start_time: "2026-03-31T00:00:00000"
node.source_trigger_poll_interval_ms: 1000
node.source_trigger_idle_poll_interval_ms: 3000
node.source_trigger_error_backoff_ms: 10000
node.source_trigger_batch_size: 200
```

### 运行与异常处理

sink 当前行为：

- 只有目标端当前 master 节点执行轮询
- 源端连不上、源端 trigger index 不存在，这类情况按可恢复错误处理
- 对这类错误会记录 `warn` 并延后重试，不会直接把组件打成不可恢复失败

回放规则：

- DML：按 `scn` 顺序执行 upsert / delete
- `CREATE_INDEX`：目标不存在则创建；已存在则做幂等兼容校验
- `DELETE_INDEX`：目标存在则删除；不存在则直接跳过

## Trigger Index 事件格式

`ape_es_trigger_idx` 中常见字段：

- `scn`：全局递增顺序号
- `idx_name`：索引名
- `event_type`：事件类型编码
- `pk`：文档 `_id`；DDL 事件中通常是索引名
- `row_data`：DML 文档内容，或 DDL payload
- `doc_type`：ES6 文档类型
- `create_time`：事件创建时间，格式 `yyyy-MM-dd'T'HH:mm:ssSSS`

常见事件类型：

| 类型 | 编码 | 说明 |
| --- | --- | --- |
| `INSERT` | `I` | 插入 |
| `UPDATE` | `U` | 更新 |
| `DELETE` | `D` | 删除 |
| `CREATE_INDEX` | `CI` | 创建索引 |
| `DELETE_INDEX` | `DI` | 删除索引 |

## 典型场景

### ES6 -> ES6

- 源端部署 `es-trigger-es6`
- 目标端部署 `es-sink-es6`
- 支持 DML + `CREATE_INDEX` / `DELETE_INDEX`

### ES7 -> ES6

- 源端部署 `es-trigger-es7`
- 目标端部署 `es-sink-es6`
- 当前主要支持 DML

### ES8 -> ES6

- 源端部署 `es-trigger-es8`
- 目标端部署 `es-sink-es6`
- 当前主要支持 DML

## 当前限制

- `es-sink-es6` 只能部署在目标端 ES6 集群
- ES7 / ES8 trigger 当前只支持 DML 捕获
- sink 当前只有 `node.source_trigger_enabled` 是动态配置
- sink 其它 source 地址、账密、轮询参数变更后需要重启目标节点
