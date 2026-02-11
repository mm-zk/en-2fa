# 2 FA tool

Set proper config in the .env file. Needs L1 connection and access to EN DB.

Then you can run a single batch like this:

```shell
cargo run -- --chain-address 0x32400084C286CF3E17e7B677ea9583e60a000324 --run-one-batch 506508 --dry-run 1
```

Or you can run it in the main mode like this:

```
cargo run -- --chain-address 0x32400084C286CF3E17e7B677ea9583e60a000324
```



2026-02-11T10:26:37.894815Z  INFO en_2fa: Contract threshold read threshold=0
Initializing MiniMerkleTree with start_index 32620 and binary_tree_size 32768
2026-02-11T10:26:41.537810Z  INFO en_2fa: Found batch ready for execute; building calldata batch=506510
Building proof for batch 506510
2026-02-11T10:26:42.964262Z  INFO en_2fa: Approving hash (tx on Ethereum mainnet) batch=506510 chain=0x3240…0324 from=506510 to=506510 hash=0xc89b…e0b2 data_len=1761
2026-02-11T10:26:42.964317Z  WARN en_2fa: DRY_RUN=1; not sending tx
2026-02-11T10:26:46.119794Z  INFO en_2fa: Found batch ready for execute; building calldata batch=506511

thread 'main' panicked at src/main.rs:475:13:
assertion `left == right` failed
  left: 3303343
 right: 3303344