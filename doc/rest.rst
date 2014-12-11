REST API
========

This is the main API that'll be used on the website. There are two main kinds
of API endpoints, ones for data and ones for metadata.

Metadata endpoints are for things like Cities, Departments, Products,
Industries, things that come up often in data requests, and are probably a good
idea to prefetch once, and cache. These are used to "decode" identifiers for
fields in the responses that data endpoints give, like department ids.

Data endpoints are more for looking up, say, the imports of a country in a
specific year, or across years. Especially for queries across years, the result
set can go up to several megabytes.


.. autoflask:: colombia:create_app({'DEBUG':False})
    :undoc-static:
    :include-empty-docstring:
