# clj-lmdb

[![Circle CI](https://circleci.com/gh/shriphani/clj-lmdb.svg?style=shield&circle-token=3ac09ef4532f9b7665baad776d399bcc58873338)](https://circleci.com/gh/shriphani/clj-lmdb)

Clojure wrappers for lmdb - the best no-nonsense, no-surprise, fast key-value store.

## Usage


[![Clojars Project](https://img.shields.io/clojars/v/clj-lmdb.svg)](https://clojars.org/clj-lmdb)

### Creating a database

`make-db`

```clojure
(use 'clj-lmdb.simple :reload)
nil
user> (def db (make-db "/tmp"))
#'user/db
```

You can specify the max memory map size as well:

```clojure
(use 'clj-lmdb.core :reload)
nil
user> (def db (make-db "/tmp" <size_in_bytes>))
#'user/db
```


### Inserting/Retrieving Values

`get!` and `put!`

```clojure
user> (put! db "foo" "bar")
nil
user> (get! db "foo")
"bar"
user> 
```

### Deleting Values

`delete!`

```clojure
user> (delete! db "foo")
true
user> (get! db "foo")
nil
user>
```

### Transactions:

`read-txn` to create a read-only transaction

`write-txn` to create a transaction that updates the db

This inserts a couple of entries:

```clojure
(with-txn [txn (write-txn db)]
  (put! db
        txn
        "foo"
        "bar")
  (put! db
        txn
        "foo1"
        "bar1"))
```

This retrieves them

```clojure
(with-txn [txn (read-txn db)]
  (= (get! db
           txn
           "foo")
     "bar") ; true

  (= (get! db
           txn
           "foo1")
     "bar1")) ; true
```

### Iterating through entries:

Inside a read-transaction you can use `items` or `items-from`
to iterate over the entries or to iterate from a particular key onwards.

```clojure
(with-txn [txn (read-txn db)]
 (count
  (doall
   (map
    (fn [[k v]]
      ...)
    (items db txn)))))
```

```clojure
(with-txn [txn (read-txn db)]
 (count
  (doall
   (map
    (fn [[k v]]
      ...)
    (items-from db txn "foo")))))
```

Use optional parameters `:order :asc` or `:order :desc` (default order is 
ascending) in `items` and `items-from` function calls to specify ascending or 
descending order of the keys.

```clojure
(with-txn [txn (read-txn db)]
 (count
  (doall
   (map
    (fn [[k v]]
      ...)
    (items db txn :order :desc)))))
```

```clojure
(with-txn [txn (read-txn db)]
 (count
  (doall
   (map
    (fn [[k v]]
      ...)
    (items-from db txn "foo" :order :desc)))))
```

For more examples, see the [tests](test/clj_lmdb/simple_test.clj)

## LICENSE

Copyright © 2016 Shriphani Palakodety

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
