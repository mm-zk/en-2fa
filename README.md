# 2 FA tool

Set proper config in the .env file. Needs L1 connection and access to EN DB.

Then you can run a single batch like this:

```shell
cargo run -- --chain-address 0x32400084C286CF3E17e7B677ea9583e60a000324 --run-one-batch 506508 --dry-run 1
```

Or you can run it in the main mode like this:

```
cargo run
```
