REST API
========

The APIs in the backend are built around the entities (or "things") that the
data is about. Examples of entity types are:

* Locations
* Products
* Occupations
* Industries
* Partner Countries

The API can be queried about one or a combination entities. Entities are
usually come as a hierarchy of things, meaning there are different levels of
entities. As an example, let's look at some entities of type Location:

* Colombia (Country, id: 0)

  * Bogota (Department, id: 3)

    * Chia (Municipality, id: 569)

Country, Department and Municipality are the different levels of the "Location"
entity type. The entity types and their level names can be found in the
:ref:`models` section.

There are two main kinds of API endpoints, ones for data and ones for metadata.
Data APIs can be queried to get timeseries data concerning any given entities,
while Metadata APIs return information about the entities themselves.

Metadata APIs
-------------

Metadata APIs are a cross-reference to look up the meanings of `id` s of things
like Location ids, Product ids, Industry ids, etc. For example, we can query

.. sourcecode:: http

   GET /metadata/locations/ HTTP/1.1
   Host: example.com
   Accept: application/json, text/javascript

and get a result like:

.. sourcecode:: javascript

    {
      "data": [
        {
          "code": "COL",
          "description_en": null,
          "description_es": null,
          "id": 0,
          "level": "country",
          "name_en": "Colombia",
          "name_es": "Colombia",
          "name_short_en": "Colombia",
          "name_short_es": "Colombia",
          "parent_id": null
        },
        {
          "code": "05",
          "description_en": null,
          "description_es": null,
          "id": 1,
          "level": "department",
          "name_en": "Antioquia",
          "name_es": "Antioquia",
          "name_short_en": "Antioquia",
          "name_short_es": "Antioquia",
          "parent_id": 0
        },
        ...
    ]}

This gives a lookup table that tells us that `Antioquia` has `id: 1` and that it is
in the `department` level of the `Location` entity type. Its parent has `id:0`,
meaning Colombia. There are also additional fields for descriptions, names and
short names in different languages. Using this information, one can look up
information for a specific entity, or decode the results of a data request.

Endpoints
"""""""""

.. http:get:: /metadata/(str:entity_type_plural)/

   Get all the entities of entity type `entity_type_plural`. For more info on
   specific entities, check out :ref:`models`.

   :param entity_type_plural: The plural name of the entity, like "industries".
   :query level: Optional. If specified, filters by level field by given value.


.. http:get:: /metadata/(str:entity_type_plural)/(int:id)

   Get info for a specific entity with id `id` of entity type
   `entity_type_plural`. For more info on specific entities, check out
   :ref:`models`.

   :param id: Integer id of entity.
   :param entity_type_plural: The plural name of the entity, like "industries".


Data APIs
---------

Data endpoints are for querying timeseries data. An example would be: "What
are the trade partners of Bogota?" or more generically "How much did bogota
import-export to every other country across the years?". Given that Bogota is
id: 3, we can query:

.. sourcecode:: http

   GET /data/location/3/partners/?level=country HTTP/1.1
   Host: example.com
   Accept: application/json, text/javascript

which gives a list of results that specify for each country and for each year,
how much was imported and exported:

.. sourcecode:: javascript

    {
      "data": [
        {
          "country_id": 0,
          "export_value": 0,
          "import_value": 11676609,
          "location_id": 3,
          "year": 2006
        },
        {
          "country_id": 0,
          "export_value": 0,
          "import_value": 17160192,
          "location_id": 3,
          "year": 2007
        },
        {
          "country_id": 0,
          "export_value": 3516560,
          "import_value": 46348176,
          "location_id": 3,
          "year": 2008
        },
        ...
    ]}

**Building Blocks and API Types**

There are enough datasets and enough ways to query them that it gets confusing
quick, so it's useful to have a scheme to distinguish and categorize them.

Since this data is usually used to populate visualizations, it's helpful to
think about the data in terms of the building block of each visualization,
which often happens to correspond to each row of data returned from the API.
Some examples:

  If you want a dotplot comparing the GDPs of all the states in the US, each dot
  would be for a specific location id. There is only one entity in this case,
  the location. Thus, this is an entity-year API.

  If you want a treemap of the products exported by France, each block would be
  an export value for a specific product id (different id for each) and a
  specific location id (the one for France). There are two id fields, and thus
  two entities. Thus, this is an entity-entity-year API.

To summarize, we categorize APIs based on how many entities are referred to in
each data point that the API returns. We refer to these as entity-year,
entity-entity-year and entity-entity-entity-year APIs. Or EY, EEY and EEEY for
short.


Endpoints
"""""""""

All data API requests *must* supply a ?level= query parameter.

**Entity-Year**

.. http:get:: /data/(string:entity_type)/
.. http:get:: /data/product/
.. http:get:: /data/industry/
.. http:get:: /data/location/

   Get entity-year info about the given entity. Examples of Entity-Year
   variables include:

   * Complexity of each product or industry
   * Total wages for each industry
   * Export volume for each location
   * Total wages / employment for each location

   :param entity_type: Singular name of the entity type, like "industry"
   :query level: Determines the entity level of each building block returned,
     e.g. section, 4digit, etc.


**Entity-Entity-year**

.. http:get:: /data/(string:entity_type)/(int:entity_id)/(string:subdataset)/
.. http:get:: /data/product/(int:entity_id)/exporters/
.. http:get:: /data/industry/(int:entity_id)/participants/
.. http:get:: /data/industry/(int:entity_id)/occupations/
.. http:get:: /data/location/(int:entity_id)/products/
.. http:get:: /data/location/(int:entity_id)/industries/
.. http:get:: /data/location/(int:entity_id)/subregions_trade/
.. http:get:: /data/location/(int:entity_id)/partners/

   Get entity-entity-year info about a given entity. Examples of
   Entity-Entity-Year variables include:

   * RCA / Distance / Opportunity Gain of a Product / Industry at a given Location
   * Export volume of a product for each location
   * Export volume of a location for each product
   * Total wages for each industry at a location

   :param entity_type: Singular name of the entity type, like "industry"
   :param entity_id: Numeric id of the given entity.
   :param subdataset: Which dataset to query about the given `entity_id`?
   :query level: Determines the entity level of each building block returned,
     e.g. section, 4digit, etc.

**Entity-Entity-Entity-year**

.. http:get:: /data/(string:entity_type)/(int:entity_id)/(string:subdataset)/(int:sub_id)
.. http:get:: /data/location/(int:entity_id)/products/(int:sub_id)

   :param entity_type: singular name of the entity type, like "industry"
   :param entity_id: numeric id of the given entity.
   :param subdataset: which dataset to query about the given `entity_id`?
   :param sub_id: An additional parameter.
   :query level: Determines the entity level of each building block returned,
     e.g. section, 4digit, etc.


..
  .. autoflask:: colombia:create_app({'DEBUG':False})
      :undoc-endpoints:
      :include-empty-docstring:
      :blueprints: data

