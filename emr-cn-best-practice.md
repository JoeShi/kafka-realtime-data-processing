# EMR AWS China 最佳实践

## HIVE metadata store 保存到外部 MySQL

在 EMR Launch Configuration 中增加如下配置:
```json
{
"Classification": "hive-site",
"Properties": {
  "javax.jdo.option.ConnectionURL": "jdbc:mysql://joeshi.cluster-ccydswq08f7k.rds.cn-northwest-1.amazonaws.com.cn:3306/hive?createDatabaseIfNotExist=true",
  "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
  "javax.jdo.option.ConnectionUserName": "<username>",
  "javax.jdo.option.ConnectionPassword": "<password>"
}
}
```

## HUE 中查看中国区 S3 的内容

由于中国区 endpoint 和海外不同，需要修改 `hue.ini` 文件的配置, 在 EMR Launch Configuration 中增加如下配置:
```json
{
    "Classification": "hue-ini",
    "Properties": {},
    "Configurations": [
      {
        "Classification": "aws",
        "Properties": {},
        "Configurations": [
          {
            "Classification": "aws_accounts",
            "Properties": {},
            "Configurations": [
              {
                "Classification": "default",
                "Properties": {
                  "host": "s3.cn-northwest-1.amazonaws.com.cn",
                  "region": "cn-northwest-1"
                },
                "Configurations": []
              }
            ]
          }
        ]
      }
    ]
}
```

## HUE 的用户信息，操作信息保存到外部 MySQL

**必须在外部 MySQL 中先创建名为 `huedb` 的 database**

HUE 的信息默认会保存到 EMR master 本地的 MySQL 实例中，如果新建 EMR cluster，会导致之前保存的信息丢失，建议
保存到外部 MySQL. 在 EMR Launch Configuration 中增加如下配置:
```json
{
    "Classification": "hue-ini",
    "Properties": {},
    "Configurations": [
      {
        "Classification": "desktop",
        "Properties": {},
        "Configurations": [
          {
            "Classification": "database",
            "Properties": {
              "name": "huedb",
              "user": "<username>",
              "password": "<password>",
              "host": "joeshi.cluster-ccydswq08f7k.rds.cn-northwest-1.amazonaws.com.cn",
              "port": "3306",
              "engine": "mysql"
            },
            "Configurations": []
          }
        ]
      }
    ]
  }
```

