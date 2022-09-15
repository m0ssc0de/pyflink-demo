# a pyflink demo

## env

- python 3.8
- java 11
- `python -m pip install apache-flink==1.15.2`

## run

- `python word_count.py `

## ethereum data

```
ethereumetl export_blocks_and_transactions --start-block 60000 --end-block 60010 \
--blocks-output blocks.csv --transactions-output transactions.csv \
--provider-uri https://mainnet.infura.io/v3/7aef3f0cd1f64408b163814b22cc643c
```