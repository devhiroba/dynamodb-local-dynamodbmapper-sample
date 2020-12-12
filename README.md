# dynamodb-local-dynamodbmapper-sample
本番環境で DynamoDB を使用する機会がありました。
開発環境は DynamoDB Local の Docker イメージを使用することになり、ユニットテストで DyanamoDB Local を使用する方法を調査しました。

## 調査内容
- DynamoDBMapperの基本的な使用方法
- シードデータ登録用の共通部品作成（DynamoDBMapper + GSON 使用）

## 関連技術
- Java
- JUnit5
- DynamoDB Local（Docker）
- DyanmoDBMapper
- lombok
- GSON
- IntelliJ
