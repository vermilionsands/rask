(defproject vermilionsands/rask "0.1.0-SNAPSHOT"
  :description "FIXME"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.apache.flink/flink-streaming-java_2.11 "1.5.1"]
                 [org.apache.flink/flink-java "1.5.1"]
                 [org.apache.flink/flink-clients_2.11 "1.5.1"]]
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]
  :profiles {:examples {:dependencies [[org.clojure/tools.cli "0.3.7"]]
                        :source-paths ["examples/src/clojure"]
                        :aot :all
                        :jar-name "examples-no-deps.jar"
                        :uberjar-name "examples.jar"}})