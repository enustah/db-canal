# 数据库同步

## 相关说明
纯go实现数据库同步. 将数据输入源和输出源抽象成驱动的形式,让不同数据库去实现,从而实现任意数据库的同步,
多数情况是关系型数据库同步到非关系型数据库. 目标是通过配置和少量代码甚至不需要代码实现数据库同步.
目前内置实现基于mysql binlog的数据输入源,clickhouse和elasticsearch的输出源.

go版本需要 >= 1.18

## example
参考 [example](example/readme.MD)

## 类型映射
mysql ingress -> go的类型映射 参考[mysql-ingress-go-type-cast](driver/builtin/ingress/mysql/readme.MD)


