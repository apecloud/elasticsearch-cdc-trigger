# elasticsearch-cdc-trigger

ElasticSearch CDC plugin，基于 [cloudcanal-es-trigger](https://github.com/ClouGence/cloudcanal-es-trigger) 改造。

这个项目当前包含 4 个子模块：

- `es-trigger-es6`
- `es-trigger-es7`
- `es-trigger-es8`
- `es-sink-es6`

其中支持范围需要先区分清楚：

- `es-trigger-es6`：只支持部署在 ES 6.x 源端集群
- `es-trigger-es7`：只支持部署在 ES 7.x 源端集群
- `es-trigger-es8`：只支持部署在 ES 8.x 源端集群
- `es-sink-es6`：只支持部署在 ES 6.x 目标端集群，但可以回放由 ES 6.x / 7.x / 8.x trigger 写入的 `ape_es_trigger_idx` 数据

原因很简单：

- 三个 `trigger` 模块都直接依赖各自版本的 Elasticsearch plugin API，所以必须和源端大版本匹配
- `CcEs6SinkPlugin` 本质上是读取源端 trigger index 中的标准事件数据，再在目标端 ES6 执行回放，因此它关注的是事件格式，而不是源端 trigger 插件本身的版本

## 模块与能力

### 1. Trigger 模块

三个 trigger 模块都部署在源端，用于把变更写入内部 trigger index：

- trigger index：`ape_es_trigger_idx`

#### `es-trigger-es6`

能力：

- 支持 DML 捕获：`INSERT` / `UPDATE` / `DELETE`
- 支持 DDL 捕获：
  - `CREATE_INDEX`
  - `DELETE_INDEX`
  - `UPDATE_MAPPING`
  - `UPDATE_ALIASES`

说明：

- ES6 的 DDL 捕获基于 `CcEs6ClusterStateListener`
- 当前代码里虽然有 `UPDATE_SETTINGS` 事件类型定义，但 ES6 trigger 还没有开启 settings change 捕获

#### `es-trigger-es7`

能力：

- 支持 DML 捕获：`INSERT` / `UPDATE` / `DELETE`

说明：

- 当前没有实现 ES7 的 DDL 捕获

#### `es-trigger-es8`

能力：

- 支持 DML 捕获：`INSERT` / `UPDATE` / `DELETE`

说明：

- 当前没有实现 ES8 的 DDL 捕获

### 2. Sink 模块

#### `es-sink-es6`

`es-sink-es6` 部署在目标端 ES6 集群，用于轮询源端 trigger index 并回放到目标端。

内部会在目标端维护一个状态索引：

- sink state index：`ape_es_trigger_sink_state`

能力：

- 支持回放 DML：
  - `INSERT`
  - `UPDATE`
  - `DELETE`
- 支持回放 DDL：
  - `CREATE_INDEX`
  - `DELETE_INDEX`
  - `UPDATE_MAPPING`
  - `UPDATE_SETTINGS`
  - `UPDATE_ALIASES`

说明：

- `CcEs6SinkPlugin` 运行在目标端 ES6
- 它可以消费 ES6 / ES7 / ES8 trigger 写入的 `ape_es_trigger_idx`
- 当前 ES6 trigger 实际会产出 `I/U/D/CI/DI/UM/UA`
- `UPDATE_SETTINGS` 回放能力已具备，但源端只有在 trigger 真正写出 `US` 事件时才会生效

## 工作方式

整体链路如下：

```text
source cluster
  -> trigger plugin
  -> ape_es_trigger_idx
  -> es-sink-es6 poll
  -> replay to target es6 cluster
  -> checkpoint saved in ape_es_trigger_sink_state
```

回放顺序与机制：

1. sink 轮询源端 `ape_es_trigger_idx`
2. 按 `scn` 升序拉取事件
3. 串行回放到目标端 ES6
4. 将 checkpoint 保存到 `ape_es_trigger_sink_state`

补充说明：

- 真正执行轮询和回放的是目标端集群当前 master 节点
- master 切换后，sink 会转移到新的 master 节点继续执行

## 构建

进入项目根目录后，先按目标 ES 版本调整 `gradle.properties`：

```properties
cc.es6.version=6.8.23
cc.es7.version=7.10.1
cc.es8.version=8.15.5
```

然后按需构建：

```shell
# trigger for ES6 source
sh ./all_build.sh es-trigger-es6

# trigger for ES7 source
sh ./all_build.sh es-trigger-es7

# trigger for ES8 source
sh ./all_build.sh es-trigger-es8

# sink for ES6 target
sh ./all_build.sh es-sink-es6
```

构建说明：

- `es-trigger-es6` / `es-trigger-es7` / `es-sink-es6` 需要 Java 8+
- `es-trigger-es8` 需要 Java 17+

构建产物在各子模块的 `build/dist/` 目录下。

## 源端使用

### 1. 部署 trigger 插件

根据源端 ES 版本选择对应 trigger 模块：

- 源端是 ES6：部署 `es-trigger-es6`
- 源端是 ES7：部署 `es-trigger-es7`
- 源端是 ES8：部署 `es-trigger-es8`

示例：

```shell
scp ./es-trigger-es6/build/dist/es-trigger-es6.zip es@127.0.0.1:${es_home}/plugins
cd ${es_home}/plugins
unzip es-trigger-es6.zip -d ./es-trigger-es6
```

建议在源端集群所有节点安装对应的 trigger 插件。

### 2. 配置 `elasticsearch.yml`

trigger 侧公共配置：

```yaml
node.trigger_idx_host: "127.0.0.1:9200"
node.trigger_idx_user: ""
node.trigger_idx_password: ""
```

参数说明：

- `node.trigger_idx_host`：trigger index 所在 ES 地址，必填
- `node.trigger_idx_user`：用户名，按需配置
- `node.trigger_idx_password`：密码，按需配置

### 3. 开启索引级 DML 捕获

DML 捕获通过索引设置 `index.cdc_enabled` 控制。

创建索引时开启：

```json
PUT test_idx
{
  "settings": {
    "index.cdc_enabled": true
  }
}
```

已有索引动态开启：

```json
PUT test_idx/_settings
{
  "index.cdc_enabled": true
}
```

说明：

- `INSERT` / `UPDATE` / `DELETE` 事件依赖 `index.cdc_enabled=true`
- ES6 的 DDL 事件不依赖这个开关

### 4. Trigger index 事件格式

`ape_es_trigger_idx` 中常见字段如下：

- `scn`：全局递增顺序号
- `idx_name`：索引名
- `event_type`：事件类型编码
- `pk`：文档 `_id`；DDL 事件中通常为索引名
- `row_data`：DML 文档内容，或 DDL payload
- `doc_type`：ES6 文档类型，ES7/ES8 场景下通常为空
- `create_time`：事件创建时间，格式 `yyyy-MM-dd'T'HH:mm:ssSSS`

事件类型编码：

| 类型 | 编码 | 说明 |
| --- | --- | --- |
| `INSERT` | `I` | 插入 |
| `UPDATE` | `U` | 更新 |
| `DELETE` | `D` | 删除 |
| `CREATE_INDEX` | `CI` | 创建索引 |
| `DELETE_INDEX` | `DI` | 删除索引 |
| `UPDATE_MAPPING` | `UM` | mapping 变更 |
| `UPDATE_SETTINGS` | `US` | settings 变更 |
| `UPDATE_ALIASES` | `UA` | alias 变更 |

## 目标端使用

### 1. 部署 sink 插件

目标端当前只支持 ES6，因此部署的是 `es-sink-es6`：

```shell
scp ./es-sink-es6/build/dist/es-sink-es6.zip es@127.0.0.1:${target_es_home}/plugins
cd ${target_es_home}/plugins
unzip es-sink-es6.zip -d ./es-sink-es6
```

建议在目标端 ES6 集群所有节点安装该插件。

### 2. 配置 `elasticsearch.yml`

```yaml
index.cdc_enabled: true

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

参数说明：

- `index.cdc_enabled`：sink 总开关，设为 `true` 后目标端 master 节点会启动轮询
- `node.source_trigger_idx_host`：源端 trigger index 所在 ES 地址，必填
- `node.source_trigger_idx_user`：源端用户名，按需配置
- `node.source_trigger_idx_password`：源端密码，按需配置
- `node.source_trigger_idx_name`：源端 trigger index 名，默认 `ape_es_trigger_idx`
- `node.source_trigger_start_time`：首次启动且还没有 checkpoint 时使用的起始时间，必填
- `node.source_trigger_poll_interval_ms`：正常轮询间隔，默认 `1000`
- `node.source_trigger_idle_poll_interval_ms`：空轮询间隔，默认 `3000`
- `node.source_trigger_error_backoff_ms`：异常退避时间，默认 `10000`
- `node.source_trigger_batch_size`：单批拉取数量，默认 `200`

说明：

- `node.source_trigger_start_time` 只在第一次启动且还没有 checkpoint 时生效
- 一旦 `ape_es_trigger_sink_state` 中已有 checkpoint，后续会从断点继续

### 3. 回放行为

`CcEs6SinkPlugin` 按 `scn` 升序拉取并串行回放。

回放规则：

- DML：按 `_id` 在目标端执行 upsert / delete
- `CREATE_INDEX`：目标不存在则创建；若已存在，则校验 settings / mappings / aliases 是否兼容
- `DELETE_INDEX`：目标存在则删除
- `UPDATE_MAPPING`：尝试更新 mapping，并在更新后再次校验
- `UPDATE_SETTINGS`：尝试更新 settings，并在更新后再次校验
- `UPDATE_ALIASES`：按源端状态增删 alias，并在更新后再次校验

回放状态保存在 `ape_es_trigger_sink_state`，包括：

- 当前 checkpoint
- 最近成功时间
- 最近错误信息
- 最近一次跳过的事件及原因

## 典型使用场景

### 场景 1：ES6 -> ES6

- 源端部署 `es-trigger-es6`
- 目标端部署 `es-sink-es6`
- 支持 DML + ES6 DDL 同步

### 场景 2：ES7 -> ES6

- 源端部署 `es-trigger-es7`
- 目标端部署 `es-sink-es6`
- 当前主要支持 DML 同步

### 场景 3：ES8 -> ES6

- 源端部署 `es-trigger-es8`
- 目标端部署 `es-sink-es6`
- 当前主要支持 DML 同步

## 当前限制

- `es-sink-es6` 只能部署在目标端 ES6 集群
- ES7 / ES8 trigger 当前只支持 DML 捕获
- ES6 trigger 当前支持 `CREATE_INDEX` / `DELETE_INDEX` / `UPDATE_MAPPING` / `UPDATE_ALIASES`，暂未开启 `UPDATE_SETTINGS` 捕获
