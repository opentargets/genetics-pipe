## Loading the output of the genetics-pipe

Once genetics-pipe has succesfully run on cloud dataproc, you will want to use the data somehow.

This folder contains scripts and schemas that help you do that.

### bigquery
One option is to load it into bigquery. We do this to quickly analyze and diagnose issues with the data. It is great for debugging, but probably too expensive to support production traffic.

### Clickhouse
Another columnar database option is [clickhouse](https://clickhouse.yandex/). Loading time is a bit slower, but that's about the only disadvantage.
