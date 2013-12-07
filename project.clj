(defproject vignette "0.1.0-SNAPSHOT"
  :description "A simple, distributed, highly available, eventually-consistent sketch database that communicates entirely over UDP."
  :url "http://github.com/aconbere/clojure-vignette"
  :license {:name "MIT" }
  :main vignette.server
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [aleph "0.3.0"]
                 [clj-msgpack "0.2.0"]
                 [org.clojure/math.numeric-tower "0.0.2"]
                 [ac "0.1.0-SNAPSHOT"]
                 [com.google.guava/guava "15.0"]
                 ])
