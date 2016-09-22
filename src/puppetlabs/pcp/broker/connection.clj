(ns puppetlabs.pcp.broker.connection
  (:require [clojure.string :as str]
            [puppetlabs.experimental.websockets.client :as websockets-client]
            [puppetlabs.kitchensink.core :as ks]
            [schema.core :as s])
  (:import (clojure.lang IFn)))

(def Websocket
  "Schema for a websocket session"
  Object)

(s/defn common-name :- s/Str
  [ws :- Websocket]
  (try
    (when-let [cert (first (websockets-client/peer-certs ws))]
      (ks/cn-for-cert cert))
    (catch Exception _
      "")))

(s/defn remote-address :- s/Str
  [ws :- Websocket]
  (try
    (second
      (str/split (.. (websockets-client/remote-addr ws) (toString)) #"/"))
    (catch Exception _
      "")))

(s/defn summarize
  "Summarize a connection for logging"
  [ws :- Websocket]
  {:commonname (common-name ws)
   :remoteaddress (remote-address ws)})
