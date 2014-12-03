.. _projectmessage:

Cats
====

A cat is the animal which is the core of our business model.

The data is stored in the :py:class:`~colombia.models.Cat` model and the reasons
for this are discussed in the :ref:`architecture` section.

Examples
--------

First, import your favourite HTTP / REST client::

    >>> import slumber
    >>> zephyr = slumber.API("example.com", append_slash=False)
    >>> cat_id = 3584442
    >>> cat = api.cats(message_id)

Fetching a cat is easy::

    >>> cat.get()
    {'cat_id': 3584442,
    'name': Norman,
    'born_at': 141612055}


REST API Reference
------------------

.. autoflask:: colombia:create_app({'DEBUG':False})
    :undoc-static:

Python API Reference
--------------------

.. autoclass:: colombia.models.Cat
    :members:

.. autoclass:: colombia.models.HSProduct
    :members:

.. autoclass:: colombia.models.Location
    :members:

.. autoclass:: colombia.models.Municipality
    :members:

Mixins
------

.. autoclass:: colombia.models.LanguageMixin
    :members:

.. autoclass:: colombia.models.IDMixin
    :members:

