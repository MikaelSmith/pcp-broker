(ns puppetlabs.pcp.broker.service
  (:require [puppetlabs.pcp.broker.core :as core]
            [puppetlabs.structured-logging.core :as sl]
            [puppetlabs.trapperkeeper.core :as trapperkeeper]
            [puppetlabs.trapperkeeper.services :refer [service-context]]
            [puppetlabs.trapperkeeper.services.status.status-core :as status-core]
            [puppetlabs.i18n.core :as i18n]))

(trapperkeeper/defservice broker-service
  [[:AuthorizationService authorization-check]
   [:ConfigService get-in-config]
   [:WebroutingService add-websocket-handler get-server get-route]
   [:MetricsService get-metrics-registry]
   [:StatusService register-status]]
  (init [this context]
    (sl/maplog :info {:type :broker-init} (i18n/trs "Initializing broker service"))
    (let [broker (core/init {:add-websocket-handler (partial add-websocket-handler this)
                             :authorization-check authorization-check
                             :get-metrics-registry get-metrics-registry})]
      (register-status "broker-service"
                       (status-core/get-artifact-version "puppetlabs" "pcp-broker")
                       1
                       (partial core/status broker))
      (assoc context :broker broker)))
  (start [this context]
    (sl/maplog :info {:type :broker-start} (i18n/trs "Starting broker service"))
    (let [broker (:broker (service-context this))]
      (core/start broker))
    (sl/maplog :debug {:type :broker-started} (i18n/trs "Broker service started"))
    context)
  (stop [this context]
    (sl/maplog :info {:type :broker-stop} (i18n/trs "Shutting down broker service"))
    (let [broker (:broker (service-context this))]
      (core/stop broker))
    (sl/maplog :debug {:type :broker-stopped} (i18n/trs "Broker service stopped"))
    context))
