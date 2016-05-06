(ns clj-lmdb.core
  (:import [org.fusesource.lmdbjni Database Env]))

(defrecord DB [env db])

(defrecord NamedDB [env db name])

(defrecord Txn [txn type])

(defn make-db
  "Initialize a new database, optionally specifying *max-size*, in
  bytes. By default, the maximum size of the memory map (and thus the
  database) is 10485760 bytes."
  ([dir-path max-size]
   (let [env (doto (Env. dir-path)
               (.setMapSize max-size))
         db  (.openDatabase env)]
     (DB. env db)))
  ([dir-path]
   (make-db dir-path 10485760)))

(defn make-named-db
  "Create a named database using an env.
  Returns a db record you can use with all
  the other functions"
  ([dir-path name max-size]
   (let [env (doto (Env. dir-path)
               (.setMapSize max-size))
         db  (.openDatabase env name)]
     (NamedDB. env
               db
               name)))
  ([dir-path name]
   (make-named-db dir-path
                  name
                  10485760)))

(defn read-txn
  [db-record]
  (let [env (:env db-record)
        txn (.createReadTransaction env)]
    (Txn. txn :read)))

(defn write-txn
  [db-record]
  (let [env (:env db-record)
        txn (.createWriteTransaction env)]
    (Txn. txn :write)))

(defmacro with-txn
  [bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-txn ~(subvec bindings 2)
                                  ~@body)
                                (finally
                                  (if (-> ~(bindings 0)
                                          :type
                                          (= :read))
                                    (-> ~(bindings 0)
                                        :txn
                                        (.abort))
                                    (-> ~(bindings 0)
                                        :txn
                                        (.commit))))))
    :else (throw (IllegalArgumentException.
                  "with-open only allows Symbols in bindings"))))

(defn put!
  ([db-record txn k v]
   (let [db (:db db-record)]
     (.put db
           (:txn txn)
           k
           v)))

  ([db-record k v]
   (let [db (:db db-record)]
     (.put db
           k
           v))))

(defn get!
  ([db-record txn k]
   (let [db (:db db-record)]
     (.get db
           (:txn txn)
           k)))
  
  ([db-record k]
   (let [db (:db db-record)]
     (.get db
           k))))

(defn delete!
  ([db-record txn k]
   (let [db (:db db-record)]
     (.delete db
              (:txn txn)
              k)))

  ([db-record k]
   (let [db (:db db-record)]
    (.delete db
             k))))

(defn drop-db!
  [named-db-record]
  (-> named-db-record
      :db
      (.drop true)))

(defn items
  [db-record txn]
  (let [db   (:db db-record)
        txn* (:txn txn)

        entries (-> db
                    (.iterate txn*)
                    iterator-seq)]
    (map
     (fn [e]
       [(.getKey e) (.getValue e)])
     entries)))

(defn items-from
  [db-record txn from]
  (let [db   (:db db-record)
        txn* (:txn txn)

        entries (-> db
                    (.seek txn*
                           from)
                    iterator-seq)]
    (map
     (fn [e]
       [(.getKey e) (.getValue e)])
     entries)))
 
