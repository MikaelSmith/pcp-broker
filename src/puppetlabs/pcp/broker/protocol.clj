(ns puppetlabs.pcp.broker.protocol
  (:require [clojure.string :as str]
            [schema.core :as s]))

(def Message
  "Defines the message format.
  Target is optional because the recipient doesn't need it, and blank means the server.
  Sender is optional because blank means the server."
  {(s/optional-key :sender) s/Str
   (s/optional-key :target) s/Str
   :message_type s/Str
   :data         s/Any})

(s/defn summarize
  "Get a loggable summary of a message"
  [message :- Message]
  {:messagetype (:message_type message)
   :source (or (:sender message) "<server>")
   :destination (or (:target message) "<server>")})

(def InventoryRequest
  "Data schema for http://puppetlabs.com/inventory_request"
  [s/Str])

(def InventoryResponse
  "Data schema for http://puppetlabs.com/inventory_response"
  [s/Str])

(def ErrorMessage
  "Data schema for http://puppetlabs.com/error_message"
  s/Str)
