(ns puppetlabs.pxp-dispatch-service-test
  (:require [clojure.test :refer :all]
            [puppetlabs.trapperkeeper.app :as app]
            [puppetlabs.trapperkeeper.testutils.bootstrap :refer [with-app-with-empty-config]]
            [puppetlabs.pxp-dispatch-service :as svc]))

