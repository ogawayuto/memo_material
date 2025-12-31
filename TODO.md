# 所感
StructuredStreamingでは自由度高くtopicからデータを収集できる。ただ、avroのデータを解釈する必要があるため注意が必要で複雑な処理はパフォーマンスの問題があるかも。また、OSS版ではfrom_avroのパラメータでレジストリを直接指定できないため、Databricksとは異なる仕様がある。
AVROとConfluent Wireフォーマットの違いに注意が必要。

- deleteの実装
- truncateも確認
- scd2の実装
- バッチの取り込み
- スキーマ進化パターンの整理 