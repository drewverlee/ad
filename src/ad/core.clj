(ns ad.core
  (:require [onyx-local-rt.api :as api]))

(defn watermark-fn [segment]
  (.getTime (:event-time segment)))

(defn foo
  [event window trigger {:keys [lower-bound upper-bound] :as window-data} state]
  window)

(def sim-heaing-cooling-job
    {:workflow [[:in :sim-heating-cooling]]
     :catalog [{:onyx/name :in
                :onyx/type :input
                :onyx/assign-watermark-fn ::watermark-fn
                :onyx/batch-size 20}
               {:onyx/name :sim-heating-cooling
                :onyx/type :reduce
                :onyx/batch-size 20}]
     :windows [{:window/id :collect-segments
                :window/task :sim-heating-cooling
                :window/type :fixed
                :window/aggregation :onyx.windowing.aggregation/conj
                :window/window-key :event-time
                :window/range [10 :minutes]
                :window/doc "Conj's segments in one hour fixed windows."}]
     :triggers [{:trigger/window-id :collect-segments
                 :trigger/id :sync-collect
                 :trigger/on :onyx.triggers/watermark
                 :trigger/state-context [:window-state]
                 :trigger/sync ::foo
                 :trigger/doc "Fires when this window's watermark has been exceeded"}]})


(-> (api/init sim-heaing-cooling-job)
    (api/new-segment :in {:event-id 1 :event-time #inst "2015-11-20T02:00:00.000-00:00"})
    (api/new-segment :in {:event-id 2 :event-time #inst "2015-11-20T02:05:00.000-00:00"})
    (api/new-segment :in {:event-id 3 :event-time #inst "2015-11-20T02:30:00.000-00:00"})
    (api/new-segment :in {:event-id 4 :event-time #inst "2015-11-20T02:45:00.000-00:00"})
    (api/drain)
    (api/stop)
    (api/env-summary))
