# 所感
StructuredStreamingでは自由度高くtopicからデータを収集できる。ただ、avroのデータを解釈する必要があるため注意が必要で複雑な処理はパフォーマンスの問題があるかも。また、OSS版ではfrom_avroのパラメータでレジストリを直接指定できないため、Databricksとは異なる仕様がある。
AVROとConfluent Wireフォーマットの違いに注意が必要。

FAQ:
- 既存のテーブルにDebeziumを接続するとどうなる?
    - 既存のレコードのr(初回転送)のメッセージがTopicに流れる
    - Truncateなどもサポートできるがconnectorの設定が必要
- StructuredStreamingでの処理はどうする?
    - 各レコードで各opに対する処理を実装する必要がある
- deleteの実装
- truncateも確認
- scd2の実装
- バッチの取り込み
- スキーマ進化パターンの整理 