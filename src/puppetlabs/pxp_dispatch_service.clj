(ns puppetlabs.pxp-dispatch-service
  (:require [puppetlabs.structured-logging.core :as sl]
            [puppetlabs.i18n.core :as i18n]
            [puppetlabs.pxp-dispatch-core :as core]
            [puppetlabs.trapperkeeper.core :as trapperkeeper]
            [puppetlabs.trapperkeeper.services :refer [service-context]]))

(defprotocol DispatchService
  (client-connected? [this node])
  (clients-connected [this])
  (run-puppet [this node options]))

(trapperkeeper/defservice dispatch-service
  DispatchService
  {:required [ConfigService]
   :optional [BrokerService]}
  (init [this context]
    (sl/maplog :info {:type :dispatch-init} (i18n/trs "Initializing dispatch service"))
    (let [get-in-config (:get-in-config ConfigService)
          dispatcher (core/init (get-in-config [:pxp-dispatch]))]
      (assoc context :dispatcher dispatcher)))
  (start [this context]
    (sl/maplog :info {:type :dispatch-start} (i18n/trs "Starting dispatch service"))
    (core/start (:dispatcher context))
    (sl/maplog :info {:type :dispatch-started} (i18n/trs "Dispatcher service started"))
    context)
  (stop [this context]
    (sl/maplog :info {:type :dispatch-stop} (i18n/trs "Shutting down dispatch service"))
    (core/stop (:dispatcher context))
    (sl/maplog :info {:type :dispatch-stopped} (i18n/trs "Dispatch service stopped"))
    context)
  (client-connected? [this node]
    (core/client-connected? (:dispatcher (service-context this)) node))
  (clients-connected [this]
    (core/clients-connected (:dispatcher (service-context this))))
  (run-puppet [this node options]
    (core/run-puppet (:dispatcher (service-context this)) node options)))
