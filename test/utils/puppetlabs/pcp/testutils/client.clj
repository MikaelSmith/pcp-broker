(ns puppetlabs.pcp.testutils.client
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [timeout alts!! chan >!! <!!]]
            [http.async.client :as http]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.pcp.message-v1]
            [puppetlabs.pcp.message-v2]
            [puppetlabs.ssl-utils.core :as ssl-utils]))

(defn func-vs
  "Resolve a version-specific implementation of the named pcp.message function"
  [version function-name]
  (ns-resolve (symbol (str "puppetlabs.pcp.message-" version)) function-name))

;; A simple websockets client with some assertions - for non-testing uses use pcp-client.

(defprotocol WsClient
  (close [_])
  (sendbytes! [_ bytes])
  (send! [_ message])
  (recv! [_] [_ timeout]
    "Returns nil on timeout, [code reason] on close, message/Message on message"))

(defrecord ChanClient [http-client ws-client message-channel version]
  WsClient
  (close [_]
    (async/close! message-channel)
    (.close ws-client)
    (.close http-client))
  (sendbytes! [_ bytes]
    (http/send ws-client :byte bytes))
  (send! [_ message]
    (http/send ws-client :byte ((func-vs version 'encode) message)))
  (recv! [this] (recv! this (* 10 5 1000)))
  (recv! [_ timeout-ms]
    (let [[message channel] (alts!! [message-channel (timeout timeout-ms)])]
      message)))

(defn http-client-with-cert
  [certname]
  (let [cert        (format "./test-resources/ssl/certs/%s.pem" certname)
        private-key (format "./test-resources/ssl/private_keys/%s.pem" certname)
        ca-cert     "./test-resources/ssl/ca/ca_crt.pem"
        ssl-context (ssl-utils/pems->ssl-context cert private-key ca-cert)]
    (http/create-client :ssl-context ssl-context)))

(defn make-association-request
  [uri version]
  (-> ((func-vs version 'make-message))
      (assoc :message_type "http://puppetlabs.com/associate_request"
             :targets ["pcp:///server"]
             :sender uri)
      ((func-vs version 'set-expiry) 5 :seconds)))

(defn connect
  "Makes a client for testing"
  [& {:keys [certname uri version modify-association check-association]
      :or {modify-association identity
           check-association true
           version "v2"}}]
  (let [uri                 (or uri (str "pcp://" certname "/test"))
        association-request (modify-association (make-association-request uri version))
        client              (http-client-with-cert certname)
        message-chan        (chan)
        ws                  (http/websocket client (str "wss://127.0.0.1:8143/pcp/" version)
                                            :open  (fn [ws]
                                                     (http/send ws :byte ((func-vs version 'encode) association-request)))
                                            :byte  (fn [ws msg]
                                                     (>!! message-chan ((func-vs version 'decode) msg)))
                                            :close (fn [ws code reason]
                                                     (>!! message-chan [code reason])))
        wrapper             (ChanClient. client ws message-chan version)]
    (if check-association
      (let [response (recv! wrapper)]
        (is (= "http://puppetlabs.com/associate_response" (:message_type response)))
        (is (= (case version
                 "v1" nil
                 (:id association-request))
               (:in-reply-to response)))
        (is (= {:id (:id association-request)
                :success true}
               ((func-vs version 'get-json-data) response)))))
    wrapper))
