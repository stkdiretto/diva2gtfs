# DIVA2GTFS


This is an experimental suite used to import DIVA data structures and parse it into GTFS. It is currently neither well designed nor particularly user-friendly, but it gets the job done, somehow.

## Prerequisites

I recommend running the scripts in a GNU/Linux environment. It might be possible to use this suite in MS-Windows, but I would not know how to perform the necessary coordinate transformations :(

You will need:
 * perl(1)
 * [proj(1)](http://trac.osgeo.org/proj/) (for the cs2cs coordinate transformation tool)
 * sqlite3 (1) (or another database of your choice)

```
$ apt-get install perl proj sqlite3
```

Additionally, the following Perl modules are necessary:
 * DBI
 * DateTime
 * Date::Holidays::DE (for taking care of German holidays)
 * DateTime::Format::Strptime
 * Getopt::Long
 * File::Path

```
$ perl -MCPAN -e shell
cpan[n]> install DBI
cpan[n]> install DateTime
cpan[n]> install Date::Holidays::DE
cpan[n]> install DateTime::Format::Strptime
cpan[n]> install File::Path
```

If you do not have installation privileges on your machine, you might want to use local::lib.
<http://jjnapiorkowski.typepad.com/modern-perl/2010/02/bootstrapping-locallib.html> describes a one-step-solution how to do it; the necessary script can be found at <https://github.com/jjn1056/bootstrap-locallib.pl>

## Usage

### Step 1: Setting up the databases

initdb will take care of setting up the sqlite databases. Just run `./initdb.pl --create all` to create both DIVA and GTFS databases: divadata.db and diva2gtfs.db

initdb supports the following options:
```
--create <option> # creates tables, if not already existing
--drop <option>   # drops tables if existing
--clear <option>  # drops tables and rebuild them
```

Options:
 * divaservice -- DIVA tables concerning service restrictions
 * divalnrlit -- DIVA tables pointing to route definition files
 * divastops -- DIVA stop definition
 * diva -- _all_ of the DIVA tables
 * gtfsstops -- GTFS stop table
 * gtfsruns -- GTFS trips, routes and stop_times tables
 * gtfscalendar -- GTFS calendar and calendar_dates tables
 * gtfs -- _all_ GTFS tables (includes tables not accessed by the gtfs options above)

### Step 2: Load DIVA data into database

`loaddiva.pl` will stupidly parse any DIVA file you give it as a command-line argument, parse it for tabular content and insert it into the pre-defined DIVA database. If a table is not defined, this will fail spectacularly.

Please provide loaddiva with all stop definition (`haltestellen.\*` but _not_ `haltestellen.format32.\*`), service restriction (`vbesch.\*`), agencies descriptions (`bzw`), guaranteed transfers (`anschlb.\*`) and route tables (`lnrlit`) and let it churn through them.

### Step 3: Convert DIVA tables to GTFS
```
./stops2gtfs.pl
./service2gtfs.pl
```

Both scripts will go through the DIVA tables and transform their content into GTFS format. For the coordinate transformation, `cs2cs` from `proj(1)` is needed. Currently, only a subset of coordinate reference systems (specified in the column `plan` in the DIVA tables) will be converted.
Support for other CRS (e.g. GIP1) still needs to be implemented... sometimes... by someone (pull requests are appreciated).

### Step 4: Load route files

After everything has been prepared, the magic can happen. Call
```
./diva2gtfs.pl --path /path/to/diva/basedirectory/
```

If the `lnrlit` table was correctly populated, everything should happen automagically. Or not. I am lacking test cases, so far.

### Step 5: Fine-tuning the data

Optionally, you can now use the guaranteed transfers data out of the DIVA anschlb table and update your trips table: `./transfers2gtfs.pl`

Also, you might want to import Shapes from your friendly EFA. This is possible through `./efaShapeExporter.pl`, which is currently hardcoded to the DING EFA. Messy coding, again, sorry.

### Finally: Output GTFS data

Run the following command:

```
./export.pl
```

## Further reading

This script was created alongside my diploma thesis, [Opening Public Transit Data in Germany – A Status Quo](http://dbis.eprints.uni-ulm.de/1054/), which includes more detailed information on the DIVA data format.
However, this is nothing more than the result of my reverse engineering the format – without any guarantees as to completeness and/or accuracy.

## Contact the author

Reach me via [Twitter (@_stk)](http://www.twitter.com/_stk), or E-Mail `transit at shutterworks dot org`. Further ramblings on public transit and open data (mostly in German) on [my blog](http://stefan.bloggt.es). More Open Data tinkerers and their projects on [UlmAPI](http://www.ulmapi.de), the project of Ulm's _datalove_ working group.
