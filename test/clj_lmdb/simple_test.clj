(ns clj-lmdb.simple-test
  (:require [clojure.test :refer :all]
            [clj-lmdb.simple :refer :all]))

(deftest non-txn-test
  (testing "Put + get without using a txn"
    (let [db (make-db "/tmp")]
      (put! db
            "foo"
            "bar")
      (is
       (= (get! db
                "foo")
          "bar"))

      (delete! db "foo")

      (is
       (nil?
        (get! db "foo"))))))

(deftest with-txn-test
  (testing "Results with a txn"
    (let [db (make-db "/tmp")]
      (with-txn [txn (write-txn db)]
        (put! db
              txn
              "foo"
              "bar")
        (put! db
              txn
              "foo1"
              "bar1"))

      (with-txn [txn (read-txn db)]
        (is (= (get! db
                     txn
                     "foo")
               "bar"))

        (is (= (get! db
                     txn
                     "foo1")
               "bar1")))

      (delete! db "foo")
      (delete! db "foo1"))))

(deftest iteration-test
  (testing "Iteration"
    (let [db (make-db "/tmp")]
      (with-txn [txn (write-txn db)]
        (dotimes [i 1000]
          (put! db txn (str i) (str i))))

      (with-txn [txn (read-txn db)]
        (let [num-items (count
                         (doall
                          (map
                           (fn [[k v]]
                             (is (= k v))
                             [k v])
                           (items db txn))))]
          (is (= num-items 1000))))

      (with-txn [txn (read-txn db)]
                (is (= ["999" "999"] (first (items db txn :order :desc))))
                (is (= ["0" "0"] (first (items db txn :order :asc)))))

      (with-txn [txn (read-txn db)]
        (let [num-items (count
                         (doall
                          (map
                           (fn [[k v]]
                             (is (= k v))
                             [k v])
                           (items-from db txn "500"))))]
          (is (= num-items 553)))) ; items are sorted in alphabetical order - not numerical

      (with-txn [txn (read-txn db)]
                (is (= 337 (count (items-from db txn "400" :order :desc))))
                (is (= 664 (count (items-from db txn "400" :order :asc))))
                (is (thrown? IllegalArgumentException
                             (items-from db txn "400" :order :other))))


      (with-txn [txn (write-txn db)]
        (dotimes [i 1000]
          (delete! db txn (str i)))

        (is (= (count (items-from db txn "400"))
               0))))))

(deftest named-db-test
  (testing "Create multiple databases in a single env."
    (let [db-record1 (make-named-db "/tmp"
                                    "db1")

          db-record2 (make-named-db "/tmp"
                                    "db2")]
      (put! db-record1
            "foo"
            "bar")
      (put! db-record2
            "foo"
            "baz")

      (is (= (get! db-record1
                   "foo")
             "bar"))

      (is (= (get! db-record2
                   "foo")
             "baz"))

      (drop-db! db-record1)
      (drop-db! db-record2))))
