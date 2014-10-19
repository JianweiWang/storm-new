;;test clojure proxy
(ns wjw.test
  (:import [java.util ArrayList HashMap List])
  (:import [java.util Collection Comparator]))
;(defn sort-capacity [ ^java.util.HashMap capacity-map]
;  (let [capacity-list1 (ArrayList. (.entrySet capacity-map))]
;    (.sort Collection capacity-list1 (proxy [Comparator] []
;
;                                       (compare [^java.util.Map.Entry o1
;                                                ^java.util.Map.Entry o2]
;                                         (
;                                           (.compareTo (.valueOf Double (.getValue o2)) (.valueOf Double (.getValue o1)))
;                                           )
;                                         )
;                                       )
;      )
;    )
;  )

;(defn test-fun []
;  (def my-map (HashMap.))
;  (doto
;    (loop [cnt 10 acc 1]
;      ( (if (> cnt 0) (recur (dec cnt) (.put my-map cnt (+ cnt 1))))
;        )
;      )
;    (println my-map)
;    (sort-capacity my-map )
;    (println my-map)
;    )
;  )
(defn test-return [x]
  (if (= x 1)
    true
    false))
(if (test-return 0)
  (println "Y")
  (println "N"))
;(test-fun)