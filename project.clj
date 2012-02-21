(defproject astrocomp "0.0.1"
  :source-path "src/clj"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [cascalog "1.8.6"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
					 [lein-marginalia "0.7.0-SNAPSHOT"]]
  :aot [compat.astro])
