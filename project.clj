(def ks-version "1.3.0")
(def tk-version "1.4.1")
(def i18n-version "0.4.3")
(def tk-jetty9-version "1.5.7")

(defn deploy-info
  [url]
  {:url url
   :username :env/nexus_jenkins_username
   :password :env/nexus_jenkins_password
   :sign-releases false})

(defproject puppetlabs/pxp-dispatch "0.1.0-SNAPSHOT"
  :description "PXP request dispatch service"
  :url "http://github.com/puppetlabs/pxp-dispatch"
  :license {:name ""
            :url ""}

  :pedantic? :abort

  :repositories [["releases" "http://nexus.delivery.puppetlabs.net/content/repositories/releases/"]
                 ["snapshots" "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/"]]

  :plugins [[lein-release "1.0.5" :exclusions [org.clojure/clojure]]
            [puppetlabs/i18n ~i18n-version]]

  :deploy-repositories [["releases" ~(deploy-info "http://nexus.delivery.puppetlabs.net/content/repositories/releases/")]
                        ["snapshots" ~(deploy-info "http://nexus.delivery.puppetlabs.net/content/repositories/snapshots/")]]

  :dependencies [[org.clojure/clojure "1.8.0"]

                 ;; explicit versions of deps that would cause transitive dep conflicts
                 [org.clojure/tools.reader "1.0.0-beta1"]
                 [slingshot "0.12.2"]
                 ;; end explicit versions of deps that would cause transitive dep conflicts

                 [puppetlabs/clj-pxp-puppet "0.2.1"]

                 [grimradical/clj-semver "0.3.0" :exclusions [org.clojure/clojure]]
                 [org.clojure/tools.logging "0.3.1"]
                 [puppetlabs/structured-logging "0.1.0" :exclusions [org.clojure/clojure
                                                                     org.clojure/tools.analyzer.jvm]]
                 [puppetlabs/i18n ~i18n-version]
                 [puppetlabs/trapperkeeper ~tk-version]
                 [puppetlabs/trapperkeeper-webserver-jetty9 ~tk-jetty9-version]]

  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[puppetlabs/trapperkeeper ~tk-version :classifier "test" :scope "test"]
                                  [puppetlabs/kitchensink ~ks-version :classifier "test" :scope "test"]
                                  [clj-http "3.0.0"]
                                  [org.clojure/tools.namespace "0.2.11"]
                                  [ring-mock "0.1.5"]]}}

  :repl-options {:init-ns user}

  :aliases {"tk" ["trampoline" "run" "--config" "dev-resources/config.conf"]}

  :main puppetlabs.trapperkeeper.main

  )
