(defproject rask "0.1.0-SNAPSHOT"
  :description "A Clojure DSL for Flink"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.flink/flink-streaming-java_2.10 "1.3.2"]
                 [org.apache.flink/flink-java "1.3.2"]
                 [org.apache.flink/flink-clients_2.10 "1.3.2"]]
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"])
;; todo - move deps to provided