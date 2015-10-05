Subnational Atlas
=================


Usage
-----

Run `make dev` to run the dev server. It'll install all the dependencies if it
has to.

For production, you're looking for runserver.py. The `app` variable has what
you want. Alternatively, the `create_app` factory could also be used.


<table>
<tr><th>Command</th><th> What it does </th></tr>
<tr><td>`make dev` </td><td> Run the flask test server in debug mode. </td></tr>
<tr><td>`make test` </td><td> run tests with coverage</td></tr>
<tr><td>`make dummy` </td><td> Generates some dummy data and dumps ids.</td></tr>
<tr><td>`make shell` </td><td> run an ipython shell where you can play around with objects. The variables `app`, `db` and `models` come preloaded.</td></tr>
<tr><td>`make docs` </td><td> Builds pretty docs and pops open a browser window</td></tr>
<tr><td>`make clean` </td><td> Clean up all the generated gunk</td></tr>
</table>

Configuration
-------------

The `/conf/` directory contains configuration files. `dev.py` is included by
default. You pass the `CHASSIS_CONFIG="path/to/config.py"` environment variable
to pick a settings file. It's this way to make sure no one accidentally loads
the dev config in production.

Changelog
---------

* Nothing here yet until release.
