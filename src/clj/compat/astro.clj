;; @paulosuzart
(ns compat.astro
  (:use cascalog.api)
  (:require [cascalog [vars :as v] [ops :as c]])
  (:gen-class))
    
;; ## Using this cascalog query
;; run hadoop then <br>
;; put the `data/users.txt` on hfs and then submit the job <br>
;; `bin/hadoop jar astrocomp-0.0.1-standalone.jar compat.astro /tmp/users /tmp/result/users`

(defn to-long 
	"Utility function to conver strings to long"
	[num] (Long/parseLong num))

(defn astro-score 
	"The distance between p1 p2 is the astro-score"
	[p1 p2] (java.lang.Math/abs (- p1 p2)))

(defn city-score 
	"Users in the same city "
	[c1 c2] (println "######" c1 c2) (if (= c1 c2) 1 0))

;; a filter opt to avoid matching a user with itself
(deffilterop isdiff? [u u2] (not (= u u2)))

(defn user-data 
	"Extracts fields from the hfs dir.
	Returns the parsed text separated by spaces"
	[dir]
    (let [source (hfs-textline dir)]
        (<- [?name ?birth ?city ?planet ?p1 ?p2 ?p3]
            (source ?line)
            (c/re-parse [#"[^\s]+"] ?line :> ?name ?birth ?city ?planet ?p1s ?p2s ?p3s)
			(to-long ?p1s :> ?p1)
			(to-long ?p2s :> ?p2)
			(to-long ?p3s :> ?p3))))

;; Queries the dataset to generate the combination of users and 
;; the corresponding scores

;; The query uses `(cross-join)` to match all users.
(defn computecomp [out-tap user-dir]
    (let [users (user-data user-dir)]
		(?<- out-tap [?user ?user2 ?score]
	      (users :#> 7 {0 ?user, 2 ?city, 4 ?p1})
	      (users :#> 7 {0 ?user2, 2 ?city2, 4 ?p2})
	      (isdiff? ?user ?user2)
	      (astro-score ?p1 ?p2 :> ?astro-score)
		  (city-score ?city ?city2 :> ?city-score)
		  (#'- ?astro-score ?city-score :> ?score)
	      (cross-join))))
   
(defn -main [user-dir out-dir]
    (computecomp (hfs-textline out-dir) user-dir))