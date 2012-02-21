(ns publicearth.search
  (:use [clojure.contrib.logging :as log :only ()]
        [publicearth.place :only (updated)] 
        clojure.contrib.str-utils))

(import '(org.apache.solr.client.solrj.impl StreamingUpdateSolrServer CommonsHttpSolrServer)
        '(org.apache.solr.common SolrInputDocument SolrInputField)
        '(it.rambow.master.javautils PolylineEncoder Track Trackpoint)
        '(com.vividsolutions.jts.io WKTReader)
        '(com.vividsolutions.jts.geom GeometryFactory PrecisionModel))

(def *srid* 4326)

; TODO:  Use StreamingUpdateSolrServer for the mass updater!
; (or (config :queue-size) 5000) (or (config :threads) 2))))

(defn connect [config]
  "Setup a reference to the Solr server under *solr* in this namespace"
  (def *solr* (CommonsHttpSolrServer. (str "http://" (or (config :host) "localhost") ":" (or (config :port) 8989)
      "/" (config :context)))))


(defn array-field [values field]
  "Generate a Solr input field for an array of values"
  (let [solr_field (SolrInputField. field)]
    (doall (for [value values] (. solr_field addValue value 1.0)))
    solr_field))

(defn contributor-id [contributor]
  "Appends either a + or ! to the ID if the contributor is visible or invisible, and a *
  if the contribtor is the creator"
  (str (contributor :id) (if (contributor :visible) "+" "!") (if (contributor :creator) "*")))

(defn read-wkt [wkt]
  "Read the WKT text into a JTS Geometry model"
  (. (WKTReader. (GeometryFactory. (PrecisionModel.) *srid*)) read wkt))

(defn track [wkt]
  "Convert a linestring (route, region) to a track for submission to PolylineEncoder"
  (let [track (Track.)
        geometry (read-wkt wkt)]
    (doall (for [coordinate (seq (. geometry getCoordinates))]
      (. track addTrackpoint (Trackpoint. (. coordinate y) (. coordinate x)))))
    track))

(defn point-string [wkt]
  "Convert a linestring into a human-readable form, e.g. (34.234,-104.546),(48.776,-102.391)"
  (let [geometry (read-wkt wkt)]
    (str-join \, (map (fn [coord] (str "(" (.y coord) "," (.x coord) ")")) (.getCoordinates geometry)))))

(defn boost [place]
  "Calculate the document boost based on richness of the place information"
  (+
    ; description gives you 1 point, but a long description gives you 5 points
    (if ((place :details) "description")
      (if (> (count (re-seq #"\w+" (first ((place :details) "description")))) 10) 5.0 1.0) 0.0)

    (if (place :photo)
      (if (> ((place :photo) :total_photos) 0) 5.0 0.0) 0.0)

    (if (> (count (place :details)) 3) 1.0 0.0)
    (if (> (count (place :details)) 6) 3.0 0.0)
    (if (> (count (place :moods)) 0) 2.0 0.0)
    (if (> (count (place :features)) 0) 2.0 0.0)
    (if (> (count (place :contributors)) 0) 2.0 0.0)
    (if (> (count (place :contributors)) 2) 3.0 0.0)
    (if (> (place :number_of_saved_places) 0) 4.0 0.0)
    (if (> (place :number_of_saved_places) 2) 8.0 0.0)
    (if (> (place :number_of_ratings) 2) (place :average_rating) 0.0)))

(defn document [place]
  "Convert a place into a Solr document (SolrJ)"
  (let [document (SolrInputDocument.)]
    (doto document
      (.addField "id" (place :id))
      (.addField "name" (first ((place :details) "name")))
      (.addField "slug" (place :slug))
      (.addField "average_rating" (place :average_rating))
      (.addField "number_of_ratings" (place :number_of_ratings))
      (.addField "latitude" (place :latitude))
      (.addField "longitude" (place :longitude))
      (.addField "created_at" (place :created_at))
      (.addField "updated_at" (place :updated_at))

      (.put "keyword" (array-field (place :keywords) "keyword"))
      (.put "feature" (array-field (place :features) "feature"))
      (.put "mood" (array-field (place :moods) "mood"))
      (.put "belongs_to" (array-field (place :belongs_to) "belongs_to"))
    )

    (let [category (place :category)]
      (doto document
        (.addField "category_id" (category :id))
        (.addField "category_name" (category :name))
        (.addField "category_slug" (category :slug))))

    (let [photo (place :photo)]
      (if photo (doto document
        (.addField "photo_id" (photo :id))
        (.addField "photo_url" (photo :original))
        (.addField "photo_square" (photo :square))
        (.addField "photo_large" (photo :large))
        (.addField "photo_map" (photo :map))
        (.addField "photo_details" (photo :details))
      )))

    (let [contributors (place :contributors)
          creator (first (filter #(true? (% :creator)) contributors))]
      (doto document
        (.put "contributor_id" (array-field (map #(contributor-id %) contributors) "contributor_id"))
        (.put "contributor" (array-field (map #(% :name) contributors) "contributor"))
        (.addField "creator_id" (contributor-id creator))
        (.addField "creator" (creator :name))))

    ; place details
    (doall (for [[definition values] (place :details)]
      (let [doc_field (str "attr_text_" definition)]
        (. document put doc_field (array-field values doc_field)))))

    ; routes
    (let [route (place :route)]
      (if route
        (let [encoder (PolylineEncoder.)
              track   (track route)
              encoded (. encoder dpEncode track)]
          (doto document
            (.addField "encoded_route" (get encoded "encodedPoints"))
            (.addField "encoded_route_levels" (get encoded "encodedLevels"))
            (.addField "encoded_route_zoom_factor" (. encoder getZoomFactor))
            (.addField "encoded_route_num_zoom_levels" (. encoder getNumLevels))
            (.addField "route" (point-string route))
            (.addField "route_length" (place :length_in_meters))
          )
        )))

    ; regions
    (let [region (place :region)]
      (if region
        (let [encoder (PolylineEncoder.)
              track   (track region)
              encoded (. encoder dpEncode track)]
          (doto document
            (.addField "encoded_region" (get encoded "encodedPoints"))
            (.addField "encoded_region_levels" (get encoded "encodedLevels"))
            (.addField "encoded_region_zoom_factor" (. encoder getZoomFactor))
            (.addField "encoded_region_num_zoom_levels" (. encoder getNumLevels))
            (.addField "region" (point-string region))
            (.addField "region_area" (place :area_in_sq_meters))
          )
        )))

    (.setDocumentBoost document (boost place))

    (log/info (str "Indexing " (first ((place :details) "name")) " (" (place :id) ") with a boost of " (boost place)))

    document))

(defn post [place]
  "Post a place to Solr"
  (let [doc (document place)]
    (let [response (.add *solr* doc)]
      (if (= (.getStatus response) 0)
        (do
          (updated (place :id))
          (log/info (str "Successfully indexed " (place :id) " with boost " (.getDocumentBoost doc))))
        (log/info (str "Solr failed to index " (place :id) ": " (.getStatus response) " - " response))))))
