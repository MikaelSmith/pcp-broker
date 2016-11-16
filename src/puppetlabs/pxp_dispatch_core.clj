(ns puppetlabs.pxp-dispatch-core
  "PXP request dispatcher

  A service that maintains an inventory sourced from multiple brokers,
  and can deliver PXP requests to nodes connected to those brokers."
  (:require [cheshire.core :as json]
            [puppetlabs.structured-logging.core :as sl]
            [puppetlabs.i18n.core :as i18n]
            [puppetlabs.pxp.puppet :as pxp]
            [puppetlabs.pcp.client :as pcp]
            [schema.core :as s]
            [slingshot.slingshot :refer [try+ throw+]]
            [clj-semver.core :as semver])
  (:import (clojure.lang IFn Atom)))

(def Dispatcher
  {:server-urls [String]
   :ssl-cert String
   :ssl-key String
   :ssl-ca-cert String
   :servers Atom})

(s/defn init :- Dispatcher
  [options]
  ;; TODO: validate options
  (assoc options :servers (atom [])))

(def client-type "dispatcher")

(defn ^:private create-client
  "Builds and connects a pxp-client with the identity
  pcp://<certname>/dispatcher, standardizing the various possible failure
  cases into a single :pxp-connection-failure error kind."
  [server-url {:keys [ssl-cert ssl-key ssl-ca-cert pcp-timeout] :or {pcp-timeout 30}}]
  {:pre [(every? not-empty [ssl-cert ssl-key ssl-ca-cert])
         (number? pcp-timeout)]}
  (let [pxp-options {:server server-url
                     :cert ssl-cert
                     :private-key ssl-key
                     :cacert ssl-ca-cert
                     :type client-type
                     :timeout pcp-timeout
                     :message-expiry pcp-timeout
                     :max-message-size 64000000}]
    (try+ (pxp/open pxp-options)
      (catch [:type :puppetlabs.pxp.puppet/connection-error] {error :error}
        (sl/maplog :error {:type :dispatch-start} (i18n/trs "Could not connect to PCP broker at {0}: {1}" server-url error))
        (throw+ {:kind :pxp-connection-failure :message (i18n/tru "Could not connect to PCP broker")}))
      (catch [:type :puppetlabs.pxp.puppet/association-failure] {error :error}
        (sl/maplog :error {:type :dispatch-start} (i18n/trs "Could not associate with PCP broker at {0}: {1}" server-url error))
        (throw+ {:kind :pxp-connection-failure :message (i18n/tru "Could not connect to PCP broker")}))
      (catch Exception e
        (sl/maplog :error {:type :dispatch-start} (i18n/trs "Could not connect to PCP broker at {0}: {1}" server-url e))
        (throw+ {:kind :pxp-connection-failure :message (i18n/tru "Could not connect to PCP broker")})))))

(s/defn start
  [dispatcher :- Dispatcher]
  (sl/maplog :debug {:type :dispatch-start} (i18n/trs "Initializing PCP with {0}" dispatcher))
  ;; TODO: handle unavailable brokers; currently waits and then errors if any are offline
  (let [server-urls (:server-urls dispatcher)
        servers (doall (pmap #(create-client % dispatcher)
                             server-urls))]
    (reset! (:servers dispatcher) servers)))

(s/defn stop
  [dispatcher :- Dispatcher]
  ;; close connections
  (doall (pmap
           #(try+ (pxp/close %)
                  (catch [:kind :pxp-connection-failure] e))
           @(:servers dispatcher)))
  nil)

(s/defn get-connected-brokers
  [dispatcher :- Dispatcher]
  (doall (pmap #(when (pcp/connected? %1) (get-in %1 [:pcp-client :server])) @(:servers dispatcher))))

(defn ^:private client-connected-to
  [servers node]
  ;; TODO: more than one server might respond that the node's connected
  ;; TODO: handle exceptions; if a broker is not connected, we should still return a result
  ;; TODO: add a method to identify connected brokers
  (some identity (pmap #(when (not-empty (pxp/get-connected-agents %1 [node])) %1) servers)))

(s/defn client-connected?
  [dispatcher :- Dispatcher node :- String]
  ;; TODO: use inventory change announcements
  (boolean (client-connected-to @(:servers dispatcher) node)))

(s/defn clients-connected
  [dispatcher :- Dispatcher]
  (flatten (pmap #(pxp/get-connected-agents %1) @(:servers dispatcher))))

;; These flags are *always* passed to the agent in order to get the run
;; behavior we want. The behavior specified is: immediately retrieve the latest
;; catalog from the master and apply it in the foreground with verbose output,
;; failing if the catalog can't be retrieved.
(def ^:private ^:const puppet-flags-const
            ["--no-daemonize" "--onetime" "--verbose"
             "--no-use_cached_catalog"
             "--no-usecacheonfailure" "--show_diff" "--no-splay"])

(def puppet-env [])

(defn puppet-run-flags
  "Return a seq of the flags that we should use to run the Puppet agent in the
  given environment. If the noop, debug, trace or evaltrace options are
  specified, include the corresponding flags. If this run is enforcing
  environment, add the environment flag. If puppetversion >= 4.4.0 and
  environment is being enforced, specify --strict_environment_mode to cause the
  agent to fail if it can't run in the specified environment."
  [{:keys [enforce_environment] {:strs [puppetversion]} :facts :as options}]
  (cond-> puppet-flags-const
          enforce_environment (conj "--environment" (:environment options))
          (:noop options) (conj "--noop")
          (:debug options) (conj "--debug")
          (:trace options) (conj "--trace")
          (:evaltrace options) (conj "--evaltrace")
          (and puppetversion (< (semver/cmp puppetversion "4.4.0") 0)) (conj "--pluginsync")
          (and puppetversion enforce_environment (>= (semver/cmp puppetversion "4.4.0") 0)) (conj "--strict_environment_mode")))

(defn ^:private execution-failure
  "Builds a slingshot exception map representing a failed execution for the
  given node, with specified error message."
  [node message]
  {:kind :puppetlabs.orchestrator/execution-failure
   :msg message
   :details {:node node}})

(defn ^:private process-run-result
  "Handles the result of a run triggered by pxp-puppet. If the result has JSON
  stdout with a valid transaction uuid, the state and transaction uuid are
  returned. Otherwise, if there is an error message in the result, an exception
  is thrown with that error messagek. If there's no transaction uuid or error
  message, we throw a generic error indicating our confusion."
  ([node result state]
   (process-run-result node result))
  ([node result]
   (try
     (let [run-status (json/decode (:stdout result) true)
           transaction-uuid (java.util.UUID/fromString (:transaction_uuid run-status))]
       {:transaction-uuid transaction-uuid
        :status (:status run-status)})
     (catch Exception ex
            (sl/maplog :debug {:type :run-puppet} (i18n/trs "Error deploying {0}: {1}" node ex))
            (let [message (cond (= (:type result) :puppetlabs.pxp.puppet/timeout-error) (i18n/tru "Timed out waiting for status update from the agent")
                                (:error result) (:error result)
                                :else (i18n/tru "Could not determine the result of the Puppet run"))]
              (throw+ (execution-failure node message)))))))

(s/defn run-puppet
  "Runs puppet on the given node, returning the result of the run. If the run
  errors, a :puppetlabs.orchestrator/execution-failure exception will be
  thrown. Otherwise, the result will contain :status and :transaction-uuid
  keys, indicating whether the run was successful and how to find the report.
  If this function is interrupted, an execution-failure will be thrown."
  [dispatcher :- Dispatcher node :- String options]
  ;; identify broker with node
  ;; TODO: set transaction id so we can send the same transaction to multiple brokers and watch for a single response
  (let [run-callback (partial process-run-result node)
        flags (puppet-run-flags options)
        server (client-connected-to @(:servers dispatcher) node)
        run-future (pxp/run-agent server node puppet-env flags run-callback run-callback)]
    ;; TODO: associate server and server url so we can report message routing
    (sl/maplog :info {:type :run-puppet} (i18n/trs "Triggered PXP Puppet run on {0} via {1}" node (get-in server [:pcp-client :server])))
    (try (deref run-future)
         (catch InterruptedException e
                ;; Pass along the interrupt to the future running puppet and reset our own interrupt flag
                (future-cancel run-future)
                (.interrupt (Thread/currentThread))
                (throw+ (execution-failure node (i18n/tru "Puppet run was aborted due to shutdown")))))))

