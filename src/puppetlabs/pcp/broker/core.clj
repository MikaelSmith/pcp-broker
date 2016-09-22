(ns puppetlabs.pcp.broker.core
  (:require [metrics.gauges :as gauges]
            [puppetlabs.experimental.websockets.client :as websockets-client]
            [puppetlabs.pcp.broker.connection :as conn :refer [Websocket]]
            [puppetlabs.pcp.broker.metrics :as metrics]
            [puppetlabs.pcp.broker.protocol :as p :refer [Message]]
            [cheshire.core :as cheshire]
            [puppetlabs.metrics :refer [time!]]
            [puppetlabs.ssl-utils.core :as ssl-utils]
            [puppetlabs.structured-logging.core :as sl]
            [puppetlabs.trapperkeeper.authorization.ring :as ring]
            [puppetlabs.trapperkeeper.services.status.status-core :as status-core]
            [schema.core :as s]
            [puppetlabs.i18n.core :as i18n])
  (:import (clojure.lang IFn Atom)
           (java.util.concurrent ConcurrentHashMap)))

(def Broker
  {:authorization-check IFn
   :uri-map             ConcurrentHashMap ;; Mapping of Uri to Websocket
   :metrics-registry    Object
   :metrics             {s/Keyword Object}
   :state               Atom})

(s/defn build-and-register-metrics :- {s/Keyword Object}
  [broker :- Broker]
  (let [registry (:metrics-registry broker)]
    (gauges/gauge-fn registry ["puppetlabs.pcp.connections"]
                     (fn [] (count (keys (:connections broker)))))
    {:on-connect       (.timer registry "puppetlabs.pcp.on-connect")
     :on-close         (.timer registry "puppetlabs.pcp.on-close")
     :on-message       (.timer registry "puppetlabs.pcp.on-message")
     :message-queueing (.timer registry "puppetlabs.pcp.message-queueing")
     :on-send          (.timer registry "puppetlabs.pcp.on-send")}))

;; connection map lifecycle
(s/defn add-connection!
  "Add tracking of a websocket connection"
  [broker :- Broker ws :- Websocket]
  (let [uri (conn/common-name ws)]
    (.put (:uri-map broker) uri ws)))

(s/defn remove-connection!
  "Remove tracking of a connection from the broker"
  [broker :- Broker ws :- Websocket]
  (let [uri (conn/common-name ws)]
    ;; TODO: if a client establishes multiple connections, then disconnects the first one, it will be unreachable
    ;; if we add a connection that already existed, we should probably close the previous connection first
    (.remove (:uri-map broker) uri)))

(s/defn get-websocket :- (s/maybe Websocket)
  "Return the websocket a node identified by a uri is connected to, false if not connected"
  [broker :- Broker uri :- s/Str]
  (get (:uri-map broker) uri))

;;
;; Message processing
;;

(s/defn send-error-message
  [description :- s/Str ws :- Websocket]
  (let [error-msg {:message_type "http://puppetlabs.com/error_message"
                   :data description}]
    (try
      (websockets-client/send! ws (cheshire/generate-string error-msg))
      (catch Exception e
        (sl/maplog :debug e {:type :message-delivery-error}
                   (i18n/trs "Failed to deliver error message"))))))

(s/defn handle-delivery-failure
  "Log and send error for failed delivery."
  [broker :- Broker message :- Message ws :- Websocket reason :- s/Str]
  (sl/maplog :trace (assoc (p/summarize message)
                           :type :message-delivery-failure
                           :reason reason)
             (i18n/trs "Failed to deliver message for '{destination}': '{reason}'"))
  (send-error-message reason ws))

(s/defn deliver-message
  "Message consumer. Delivers a message to the websocket indicated by the :target field"
  [broker :- Broker message :- Message sender :- Websocket]
  (if-let [ws (get-websocket broker (:target message))]
    (try
      (sl/maplog
        :debug (merge (p/summarize message)
                      (conn/summarize ws)
                      {:type :message-delivery})
        (i18n/trs "Delivering message for '{destination}' to '{commonname}' at '{remoteaddress}'"))
      (let [xmsg (-> message
                     (assoc :sender (conn/common-name sender))
                     (dissoc :target))]
        (locking ws
          (time! (:on-send (:metrics broker))
                 (websockets-client/send! ws (cheshire/generate-string xmsg)))))
      (catch Exception e
        (sl/maplog :debug e {:type :message-delivery-error}
                   (i18n/trs "Failed to deliver message"))
        (handle-delivery-failure broker message sender (str e))))
    (handle-delivery-failure broker message sender (i18n/trs "not connected"))))

(s/defn find-clients
  "Return a list of matching connected clients. If an empty list is passed, return all connected clients."
  [broker :- Broker names :- [s/Str]]
  (if (empty? names)
    (.keySet (:uri-map broker))
    (filter (partial get-websocket broker) names)))

(s/defn process-inventory-request
  "Process a request for inventory data."
  [broker :- Broker message :- Message ws :- Websocket]
  (let [data (:data message)]
    (s/validate p/InventoryRequest data)
    (let [uris (find-clients broker data)
          response {:message_type "http://puppetlabs.com/inventory_response"
                    :data uris}]
      (try
        (websockets-client/send! ws (cheshire/generate-string response))
        (catch Exception e
          (sl/maplog
            :debug e (merge (conn/summarize ws)
                            {:type :message-delivery-error})
            (i18n/trs "Failed to deliver inventory to '{commonname}' at '{remoteaddress}'")))))))

(s/defn process-server-message!
  "Process a message directed at the middleware"
  [broker :- Broker message :- Message ws :- Websocket]
  (let [message-type (:message_type message)]
    (case message-type
      "http://puppetlabs.com/inventory_request" (process-inventory-request broker message ws)
      (do
        (sl/maplog
          :debug (assoc (conn/summarize ws)
                   :messagetype message-type
                   :type :broker-unhandled-message)
          (i18n/trs "Unhandled message type '{messagetype}' received from '{commonname}' '{remoteaddr}'"))))))

;;
;; Message validation
;;

(s/defn make-ring-request :- ring/Request
  [message :- Message ws :- Websocket]
  (let [{:keys [sender target message_type]} message
        form-params {}
        query-params {"sender" sender
                      "target" target
                      "message_type" message_type}
        request {:uri            "/pcp-broker/send"
                 :request-method :post
                 :remote-addr    (conn/remote-address ws)
                 :ssl-client-cert (first (websockets-client/peer-certs ws))
                 :form-params    form-params
                 :query-params   query-params
                 :params         (merge query-params form-params)}]
    request))

(s/defn authorized? :- s/Bool
  "Check if the message within the specified message is authorized"
  [broker :- Broker request :- Message ws :- Websocket]
  (let [ring-request (make-ring-request request ws)
        {:keys [authorization-check]} broker
        {:keys [authorized message]} (authorization-check ring-request)
        allowed (boolean authorized)]
    (sl/maplog
      :trace (merge (p/summarize request)
                    {:type         :message-authorization
                     :allowed      allowed
                     :auth-message message})
      (i18n/trs "Authorizing message for '{destination}' - '{allowed}': '{auth-message}'"))
    allowed))

;;
;; WebSocket onMessage handling
;;

(defn log-access
  [lvl message-data]
  (sl/maplog
    [:puppetlabs.pcp.broker.pcp_access lvl]
    message-data
    "{accessoutcome} {remoteaddress} {commonname} {source} {messagetype} {destination}"))

(s/defn process-message!
  "Authorize and process the specified raw message.
  Log the message authorization outcome via 'pcp-access' logger."
  [broker :- Broker ws :- Websocket message]
  (sl/maplog :trace {:type :incoming-message-trace :rawmsg message}
             (i18n/trs "Processing PCP message: '{rawmsg}'"))
  (s/validate Message message)
  (let [message-data (merge (conn/summarize ws)
                            (p/summarize message))]
    (if-not (authorized? broker message ws)
      (do
        (log-access :warn (assoc message-data :accessoutcome "AUTHORIZATION_FAILURE"))
        (send-error-message message (i18n/trs "Message not authorized") ws))
      (do
        (log-access :info (assoc message-data :accessoutcome "AUTHORIZATION_SUCCESS"))
        (if (empty? (:target message))
          (process-server-message! broker message ws)
          (deliver-message broker message ws))))))

(defn on-message!
  "If the broker service is not running, close the WebSocket connection.
   Otherwise process the message"
  [broker ws message]
  (time!
    (:on-message (:metrics broker))
    (if-not (= :running @(:state broker))
      (websockets-client/close! ws 1011 (i18n/trs "Broker is not running"))
      (process-message! broker ws (cheshire/parse-string message true)))))

(defn- on-text!
  "OnMessage (text) websocket event handler"
  [broker ws message]
  (on-message! broker ws message))

(defn- on-bytes!
  "OnMessage (binary) websocket event handler"
  [broker ws bytes offset len]
  (sl/maplog
    :debug {}
    (i18n/trs "Binary messages not supported")))

;;
;; Other WebSocket event handlers
;;

(defn- on-connect!
  "OnOpen WebSocket event handler. Close the WebSocket connection if the
   Broker service is not running or if the client common name is not obtainable
   from its cert. Otherwise set the idle timeout of the WebSocket connection
   to 5 min."
  [broker ws]
  (time!
    (:on-connect (:metrics broker))
    (if-not (= :running @(:state broker))
      (websockets-client/close! ws 1011 (i18n/trs "Broker is not running"))
      (if (empty? (conn/common-name ws))
        (do
          (sl/maplog :debug (assoc (conn/summarize ws)
                                   :type :connection-no-peer-certificate)
                     (i18n/trs "no client certificate, closing '{remoteaddress}'"))
          (websockets-client/close! ws 4003 (i18n/trs "No client certificate")))
        (do
          (add-connection! broker ws)
          (websockets-client/idle-timeout! ws (* 1000 60 5))
          (sl/maplog :debug (assoc (conn/summarize ws)
                                   :type :connection-open)
                     (i18n/trs "client '{commonname}' connected from '{remoteaddress}'")))))))

(defn- on-error
  "OnError WebSocket event handler. Just log the event."
  [broker ws e]
  (sl/maplog :error e (assoc (conn/summarize ws)
                             :type :connection-error)
             (i18n/trs "Websocket error '{commonname}' '{remoteaddress}'")))

(defn- on-close!
  "OnClose WebSocket event handler. Remove the connection from the broker's uri-map."
  [broker ws status-code reason]
  (time! (:on-close (:metrics broker))
         (sl/maplog
           :debug (assoc (conn/summarize ws)
                         :type :connection-close
                         :statuscode status-code
                         :reason reason)
           (i18n/trs "client '{commonname}' disconnected from '{remoteaddress}' '{statuscode}' '{reason}'"))
         (remove-connection! broker ws)))

(s/defn build-websocket-handlers :- {s/Keyword IFn}
  [broker :- Broker]
  {:on-connect (partial on-connect! broker)
   :on-error   (partial on-error broker)
   :on-close   (partial on-close! broker)
   :on-text    (partial on-text! broker)
   :on-bytes   (partial on-bytes! broker)})

;;
;; Broker service lifecycle, codecs, status service
;;

(def InitOptions
  {:add-websocket-handler IFn
   :authorization-check IFn
   :get-metrics-registry IFn})

(s/defn init :- Broker
  [options :- InitOptions]
  (let [{:keys [add-websocket-handler authorization-check
                get-metrics-registry]} options]
    (let [broker             {:authorization-check authorization-check
                              :metrics            {}
                              :metrics-registry   (get-metrics-registry)
                              :uri-map            (ConcurrentHashMap.)
                              :state              (atom :starting)}
          metrics            (build-and-register-metrics broker)
          broker             (assoc broker :metrics metrics)]
      (add-websocket-handler (build-websocket-handlers broker) {:route-id :v1})
      broker)))

(s/defn start
  [broker :- Broker]
  (let [{:keys [state]} broker]
    (reset! state :running)))

(s/defn stop
  [broker :- Broker]
  (let [{:keys [state]} broker]
    (reset! state :stopping)))

(s/defn status :- status-core/StatusCallbackResponse
  [broker :- Broker level :- status-core/ServiceStatusDetailLevel]
  (let [{:keys [state metrics-registry]} broker
        level>= (partial status-core/compare-levels >= level)]
    {:state  @state
     :status (cond-> {}
               (level>= :info) (assoc :metrics (metrics/get-pcp-metrics metrics-registry))
               (level>= :debug) (assoc :threads (metrics/get-thread-metrics)
                                       :memory (metrics/get-memory-metrics)))}))
