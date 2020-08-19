(ns jepsen.dqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os :as os]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+ throw+]]
            [clj-http.client :as http]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
; TODO: possibly implement cas this in dqlite-demo?
;(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(def rundir "/run/dqlite")
(def logfile "/var/log/dqlite.log")

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing dqlite" version)
      (c/su (c/exec "mkdir" "-p" rundir))
      (c/su (c/exec "chown" "dqlite" rundir))
      (c/su (c/exec "systemctl" "start" "dqlite"))
      (Thread/sleep 10000))

    (teardown! [_ test node]
      (info node "tearing down dqlite")
      (c/su (c/exec "systemctl" "stop" "dqlite"))
      (c/su (c/exec "rm" "-f" logfile)))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 8000))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
    (when s (Long/parseLong (str/trim-newline s))))

(defn run-read
  [conn key]
  (let [result (str (:body (http/get (str conn "/" key)
                                      {:socket-timeout 5000
                                      :connection-timeout 5000 })))]
    (if (str/includes? result "Error")
      (throw+ {:type :read-error :msg result})
      (parse-long result))))

(defn run-write
  [conn key value]
  (let [result (str (:body (http/put (str conn "/" value)
                            {:body (str value)
                            :socket-timeout 5000
                            :connection-timeout 5000 })))]
     (if (str/includes? result "Error")
       (throw+ {:type :write-error :msg (str/trim-newline result)})
       nil)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (client-url node)))

  (setup! [this test]
    ; Nothing to do here
    )

  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (try+
        (case (:f op)
          :read (assoc op :type :ok, :value
                       (independent/tuple k (run-read conn k)))
          :write (do (run-write conn k v)
                     (assoc op :type :ok)))
        (catch java.net.SocketTimeoutException e
          (assoc op
                :type (if (= :read (:f op)) :fail :info)
                :error :timeout))
        (catch [:type :read-error] e
          (assoc op :type :fail, :error
                 (if (= (:msg e) "Error: sql: no rows in result set\n")
                     :not-found
                     (str/trim-newline (:msg e)))))
        (catch [:type :write-error] e
          (assoc op :type :fail, :error (str/trim-newline (:msg e))))
        )))

  (teardown! [this test])

  (close! [_ test]
    ; Nothing to do here
    ))

(defn dqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "dqlite"
          :os os/noop
          :db (db "1.7.0")
          :client (Client. nil)
          ;:nemesis (nemesis/partition-random-halves)
          :checker   (checker/compose
              {:perf  (checker/perf)
              :indep (independent/checker
                        (checker/compose
                          {:linear   (checker/linearizable {:model (model/cas-register)
                                                            :algorithm :linear})
                          :timeline (timeline/html)}))})
          :generator  (->> (independent/concurrent-generator
                             10
                             (range)
                             (fn [k]
                               (->> (gen/mix [r w])
                                    (gen/stagger 1/10)
                                    (gen/limit 100))))
                           (gen/nemesis
                             (gen/seq (cycle [(gen/sleep 5)
                                              {:type :info, :f :start}
                                              (gen/sleep 5)
                                              {:type :info, :f :stop}])))
                           (gen/time-limit (:time-limit opts)))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dqlite-test})
                   (cli/serve-cmd))
            args))
