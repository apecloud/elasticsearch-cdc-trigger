## Intro

This is an ElasticSearch plugin for change data capture (CDC). Forked from [cloudcanal-es-trigger](https://github.com/ClouGence/cloudcanal-es-trigger).

Now supports ElasticSearch 8.x. Only index data changes will be captured in the internal trigger index. Changes to the index itself will not be captured for now.

Support for 7.x, 6.x and more other features are coming soon.

## Quick Start

- Enter the project root directory.

- Modify `gradle.properties`, and change the version corresponding to your target ElasticSearch cluster.

  ```sql
  cc.es7.version=7.10.1
  cc.es8.version=8.15.0
  ```

  > Tip: You only need to change one of them.

- Build the plugin.

  ```shell
  # for ES 7
  sh ./all_build.sh es-trigger-es7

  # for ES 8
  sh ./all_build.sh es-trigger-es8
  ```

  > Tip: Sub-projects require different versions of JAVA. Install and set JAVA_HOME if necessary.

- Copy the built cloudcanal-es-trigger plugin to the ElasticSearch plugins directory.

  ```shell
  # for ES 7
  scp ${project_root}/es-trigger-es7/build/dist/es-trigger-es7.zip es@127.0.0.1:${es_home}/plugins

  # for ES 8
  scp ${project_root}/es-trigger-es8/build/dist/es-trigger-es8.zip es@127.0.0.1:${es_home}/plugins
  ```

- Unzip the plugin.

  ```shell
  # for ES 7
  unzip es-trigger-es7.zip -d ./es-trigger-es7

  # for ES 8
  unzip es-trigger-es8.zip -d ./es-trigger-es8
  ```

- Configure CDC settings in `elasticsearch.yml`:

  ```
  node.trigger_idx_host: "127.0.0.1:9200"
  node.trigger_idx_user: ""
  node.trigger_idx_password: ""
  node.cdc_enable_idxs: "*" # will capture all indices
  # or: node.cdc_enable_idxs: "test.*, proc_db_*"
  ```

- Restart Elasticsearch

- Watching `ape_es_trigger_idx` to fetch CDC changes.
