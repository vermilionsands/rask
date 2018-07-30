(ns rask.streaming
  (:refer-clojure :exclude [map mapcat print])
  (:import [org.apache.flink.api.common.functions FlatMapFunction MapFunction]
           [org.apache.flink.api.common.typeinfo TypeHint TypeInformation]
           [org.apache.flink.streaming.api.datastream DataStream DataStreamSink KeyedStream SingleOutputStreamOperator]
           [org.apache.flink.streaming.api.environment StreamExecutionEnvironment]
           [org.apache.flink.util Collector]))

;; todo add remote and configuration options
(defn env
  ([]
   (env nil))
  ([options]
   (cond
     (:local? options)
     (StreamExecutionEnvironment/createLocalEnvironment)

     :else
     (StreamExecutionEnvironment/getExecutionEnvironment))))

(defn execute
  [env & [job-name]]
  (.execute env (or job-name (str "rask-streaming-job-" (name (gensym ""))))))

(defn ^SingleOutputStreamOperator returns
  [class-or-type ^SingleOutputStreamOperator stream]
  (cond
    (instance? Class class-or-type)           (.returns stream ^Class class-or-type)
    (instance? TypeHint class-or-type)        (.returns stream ^TypeHint class-or-type)
    (instance? TypeInformation class-or-type) (.returns stream ^TypeInformation class-or-type)))

(defn ^SingleOutputStreamOperator map
  ([f ^DataStream stream]
   (map f Object stream))
  ([f return-hint ^DataStream stream]
   (let [f' (reify MapFunction
              (map [_ x]
                (f x)))]
     (returns return-hint (.map stream f')))))

(defn ^SingleOutputStreamOperator mapcat
  ([f ^DataStream stream]
   (mapcat f Object stream))
  ([f return-hint ^DataStream stream]
   (let [f' (reify FlatMapFunction
               (^void flatMap [_ x ^Collector acc]
                  (doseq [y (f x)]
                    (.collect acc y))))]
     (returns return-hint (.flatMap stream f')))))

(defn ^KeyedStream key-by [k ^DataStream stream]
  (.keyBy stream ^ints (int-array [k])))

(defn ^SingleOutputStreamOperator sum [k stream]
  (cond
    (number? k) (.sum ^KeyedStream stream (int k))
    (string? k) (.sum ^KeyedStream stream ^String k)))

(defn ^DataStreamSink print [^DataStream stream]
  (.print stream))

;; todo
;; string from socket
;; string from file
;; collection
;; iterator
;; object stream from collection
;; source - kafka? fn?
(defn stream [^StreamExecutionEnvironment env spec]
  (let [{:keys [host port del max-retry]
         :or   {del "\n" max-retry 0}} spec]
    (.socketTextStream env ^String host ^int port ^String del ^long max-retry)))