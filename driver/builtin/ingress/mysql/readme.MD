### mysql -> go 类型映射
目前已经测试过的mysql类型和对应映射的go类型

| mysql type             |     go type |
|:-----------------------|------------:|
| bigint                 |       int64 |
| bigint unsigned        |      uint64 |
| int                    |       int64 |
| int unsigned           |      uint64 |
| varchar                |      string |
| char                   |      string |
| text                   |      string |
| float                  |     float64 |
| double                 |     float64 |
| DECIMAL                |     float64 |
| enum                   |      string |
| bit                    |       int64 |
| date                   | time.time{} |
| time                   | time.time{} |
| datetime               | time.time{} |
| timestamp              | time.time{} |
| VARBINARY              |      []byte |
| BINARY                 |      []byte |
| BLOB                   |      string |

