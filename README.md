1gbbfs
======

To build:

```shell
ocamlbuild -use-ocamlfind make
```

To build with debug:

```shell
ocamlbuild -use-ocamlfind -cflag -ppopt -cflag -DEBUG make
```

Mount example:

```shell
bfs_client.native --name msk1-client1 -- -s -o large_read -o max_read=100000 -o hard_remove -o max_write=100000 -o big_writes /tmp/bfs/
```
