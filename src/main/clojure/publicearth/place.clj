(ns publicearth.place
  (:use [clojure.contrib.sql :as sql :only ()]
        clojure.contrib.seq-utils clojure.contrib.str-utils))

(import '(org.apache.commons.pool.impl GenericObjectPool)
        '(org.apache.commons.dbcp DriverManagerConnectionFactory PoolableConnectionFactory PoolingDataSource)
        '(java.sql Timestamp))

(defstruct place :id :name :slug :average_rating :number_of_ratings :latitude :longitude :category_id,
  :keywords :features :moods :details :contributors :photo :created_at :updated_at :route :Length_in_meters
  :region :area_in_sq_meters :number_of_saved_places :belongs_to)
(defstruct category :id :name :slug)
(defstruct contributor :id :name :creator :visible)


; Connect to the database

(defn configure-data-source [config]
  "Create a DataSource using Commons DBCP"
  (let [pool (GenericObjectPool. nil 10)
        uri (str "jdbc:postgresql://" (or (config :host) "localhost") ":" (or (config :port) 5432) "/" (config :name))]
    (PoolableConnectionFactory. (DriverManagerConnectionFactory. uri (config :user) (config :password)) pool nil "select 1" false false)
    (PoolingDataSource. pool)))

(defn connect [config]
  "Assign the data source to this namespace"
  (let [data-source (configure-data-source config)]
    (def *db*
          {
            :datasource data-source
            :user (config :user)
            :password (config :password)
          })))


; Object creation

(defn create-contributor [res]
  (struct-map contributor :id (res :id) :name (res :name) :creator (res :creator) :visible (res :publicly_visible)))

(defn create-category [res]
  "Convert the map that comes in from the SQL query into a category"
  (struct-map category :id (res :category_id) :name (res :category_name) :slug (res :category_slug)))

(defn create-place [res]
  "Convert the basic map that comes back from the places SQL query into a place"
  (try
    (struct-map place :id (res :id) :name (res :name) :slug (res :slug) :latitude (res :latitude)
      :longitude (res :longitude) :created_at (res :created_at) :updated_at (res :updated_at)
      :category (create-category res) :route (res :route) :region (res :region)
      :length_in_meters (res :length_in_meters) :area_in_sq_meters (res :area_in_sq_meters))
  (catch Throwable e (throw (Exception. "Place not found")))))


; Database queries

(defn belongs-to
  "Get all the categories this place belongs to"
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select category_id from category_family_trees where family_member_id = ?" ((place :category) :id)]
      (assoc place :belongs_to (map #(% :category_id) (doall res))))))

(defn saved-places
  "Number of saved places"
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select count(1) as total from saved_places where place_id = ?" (place :id)]
      (let [result (first res)]
        (assoc place :number_of_saved_places (result :total))))))

(defn load-keywords
  "Load the keywords from the database for the given place; returns the place with keywords"
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select name from tags where id in (select tag_id from place_tags where place_id = ?)" (place :id)]
      (assoc place :keywords (map #(% :name) (doall res))))))

(defn load-features
  "Load the features from the database for the given place; returns the place with features"
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select name from features where id in (select feature_id from place_features where place_id = ?)" (place :id)]
      (assoc place :features (map #(% :name) (doall res))))))

(defn load-moods
  "Load the moods from the database for the given place; returns the place with moods"
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select name from moods where id in (select distinct mood_id from place_moods where place_id = ?)" (place :id)]
      (assoc place :moods (map #(% :name) (doall res))))))

(defn load-contributors [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select sources.id, sources.name, publicly_visible, creator from contributors, sources
        where contributors.place_id =? and contributors.source_id = sources.id
        order by first_contribution_at" (place :id)]
      (assoc place :contributors (map #(create-contributor %) (doall res))))))

(defn select-photo
  "Select a single photo to store with the place in the search index"
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select original.id, original.filename as original, square.s3_key as square, large.s3_key as large,
          map.s3_key as map, details.s3_key as details,
          (select count(1) from photos tally where tally.place_id = original.place_id) as total_photos
        from photos original
        left join photo_modifications square on original.id = square.photo_id and square.name = 'square'
        left join photo_modifications large on original.id = large.photo_id and large.name = 'large'
        left join photo_modifications map on original.id = map.photo_id and map.name = 'map'
        left join photo_modifications details on original.id = details.photo_id and details.name = 'details'
        where place_id = ? limit 1" (place :id)]
      (assoc place :photo (first res)))))

(defn rating
  "Get the average rating for a place"
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select avg(rating) as average_rating, count(1) as number_of_ratings from source_place_ratings
        where place_id = ?" (place :id)]
      (let [result (first res)]
        (assoc place :average_rating (or (result :average_rating) 0.0) :number_of_ratings (result :number_of_ratings))))))

(defn load-details
  "Load the place attributes and values from the database for the given place; returns the place with the
  attributes.  Careful:  the reduce at the end of this method will mess with your head."
  [place]
  (sql/with-connection *db*
    (sql/with-query-results res
      ["select attribute_definition, value from place_attributes pa, place_values pv
        where pa.place_id = ? and pa.id = pv.place_attribute_id
        order by attribute_definition, value" (place :id)]
      (assoc place :details
        (reduce
          (fn [details ad]
            (let [definition (ad :attribute_definition)]
              (assoc details definition (conj (get details definition []) (ad :value)))))
          (hash-map) (doall res))))))

(defn find-places [count]
  (sql/with-connection *db*
    (sql/with-query-results res ["
          select
            places.id, places.name, places.slug, st_y(position) as latitude, st_x(position) as longitude,
            places.created_at, places.updated_at, route, region,
            categories.id as category_id, categories.name as category_name, categories.slug as category_slug
          from categories, places
          left join place_routes on places.id = place_routes.place_id
          left join place_regions on places.id = place_regions.place_id
          where
            places.category_id = categories.id
          limit ?
        " count]
      (into [] (for [place (doall res)] (create-place place))))))

(defn find-place
  "Look up the place record, category, region and route for a single place"
  [place_id]
  (sql/with-connection *db*
    (sql/with-query-results res ["
          select
            places.id, places.name, places.slug, st_y(position) as latitude, st_x(position) as longitude,
            places.created_at, places.updated_at, st_astext(route) as route, st_astext(region) as region,
            length_in_meters, area_in_sq_meters,
            categories.id as category_id, categories.name as category_name, categories.slug as category_slug
          from categories, places
          left join place_routes on places.id = place_routes.place_id
          left join place_regions on places.id = place_regions.place_id
          where
            places.id = ? and
            places.category_id = categories.id
        " place_id]
      (create-place (first res)))))

(defn updated
  "Update the search_updated_at timestamp on the given place"
  [place_id]
  (sql/with-connection *db*
    (sql/update-values :places ["id = ?" place_id] {:search_updated_at (Timestamp. (System/currentTimeMillis))})))

(defn grab-places
  "Get a bunch of places to reindex"
  [count]
  (map #(saved-places %)
    (map #(rating %)
      (map #(select-photo %)
        (map #(load-contributors %)
          (map #(load-details %)
            (map #(load-moods %)
              (map #(load-features %)
                (map #(load-keywords %)
                  (find-places count))))))))))

(defn get-place
  "Pull in all the indexable information about a single place."
  [place_id]
  (let [place (rating (
      select-photo (
        load-contributors (
          load-details (
            load-moods (
              load-features (
                load-keywords (
                  find-place place_id))))))))]

    ; Don't know why, but this won't work if I wrap it around rating...maybe too deep???
    (saved-places (belongs-to place))))
