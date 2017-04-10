# DIVA2GTFS

This is an experimental suite used to import DIVA data structures and parse it into GTFS. It is currently neither well designed nor particularly user-friendly, but it gets the job done, somehow.

## Prerequisites

I recommend running the scripts in a GNU/Linux environment. It might be possible to use this suite in MS-Windows, but I would not know how to perform the necessary coordinate transformations :(

You will need:
 * perl
 * [proj](http://trac.osgeo.org/proj/) (for the cs2cs coordinate transformation tool)
 * sqlite3 (or another database of your choice)

```
$ apt-get install perl proj sqlite3 (for debian based distributions)
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
cpan[n]> install Date::Holidays::DE (or Date::Holidays::AT)
cpan[n]> install DateTime::Format::Strptime
cpan[n]> install File::Path
```

If you do not have installation privileges on your machine, you might want to use local::lib.
<http://jjnapiorkowski.typepad.com/modern-perl/2010/02/bootstrapping-locallib.html> describes a one-step-solution how to do it; the necessary script can be found at <https://github.com/jjn1056/bootstrap-locallib.pl>

## Quick start

If you do not need to fine-tune your data and just want to get out a GTFS file from your DIVA data you can use the `all.sh` script.
To run it just pass in the path to your diva data and the name of your agency:

	./all.sh ~/documents/diva/myAgency myAgency

After some computation times you can find your data as GTFS-file in the folder ./build/gtfs/myAgency and ./build/gtfs/myAgency.zip.

## Detailed Usage

### Step 1: Setting up the databases

initdb will take care of setting up the sqlite databases.   
Just run `./initdb.pl --create all` to create both DIVA and GTFS databases: `build/data/divadata.db` and `build/data/diva2gtfs.db`

	Usage: ./initdb command <options>
	Command:
		--create # creates tables, if not already existing
		--drop # drops tables if existing
		--clear  # drops tables and rebuild them
	Options:
		all -- all of the DIVA and GTFS tables
		diva -- all of the DIVA tables
		divalnrlit -- DIVA tables pointing to route definition files
		divaservice -- DIVA tables concerning service restrictions
		divastops -- DIVA stop definition
		gtfs -- all GTFS tables (includes tables not accessed by the gtfs options above)
		gtfscalendar -- GTFS calendar and calendar_dates tables
		gtfsruns -- GTFS trips, routes and stop_times tables
		gtfsstops -- GTFS stop table

### Step 2: Load DIVA data into database

`loaddiva.pl` will stupidly parse any DIVA file you give it as a command-line argument, parse it for tabular content and insert it into the pre-defined DIVA database. If a table is not defined, this will fail spectacularly.

Please provide loaddiva with all stop definition (`haltestellen.\*` but _not_ `haltestellen.format32.\*`), service restriction (`vbesch.\*`), agencies descriptions (`bzw`), guaranteed transfers (`anschlb.\*`) and route tables (`lnrlit`) and let it churn through them.

### Step 3: Convert DIVA tables to GTFS

	./agencies2gtfs.pl
	./stops2gtfs.pl
	./service2gtfs.pl


All three scripts will go through the DIVA tables and transform their content into GTFS format.
Agencies can be filled with additional information using the parameter "set":

	./agencies2gtfs.pl --set agency_url="http://www.meinVerkehrsbetrieb.de" --set agency_phone=00491234567

For the coordinate transformation, `cs2cs` from `proj(1)` is needed. Currently, only a subset of coordinate reference systems (specified in the column `plan` in the DIVA tables) will be converted:

| Keyword | Ellipsoid    | CRS                 | Offset    |
| ------- | ------------ | ------------------- | ----------|
| MVTT    | [Bessel 1841](https://en.wikipedia.org/wiki/Bessel_ellipsoid) | [Gauss-Krüger](https://en.wikipedia.org/wiki/Gauss%E2%80%93Kr%C3%BCger_coordinate_system) Zone 2 | 6 160 100 |
| NAV2    | Bessel 1841  | Gauss-Krüger Zone 2 | 6 160 100 |
| NAV3    | Bessel 1841  | Gauss-Krüger Zone 3 | 6 160 100 |
| NAV4    | Bessel 1841  | Gauss-Krüger Zone 4 | 6 160 100 |
| NAV5    | Bessel 1841  | Gauss-Krüger Zone 5 | 6 160 100 |
| NBWT    | Bessel 1841  | Gauss-Krüger Zone 3 | 6 160 100 |
| STVH    | Bessel 1841  | [ÖBMN](https://de.wikipedia.org/wiki/%C3%96sterreichisches_Bundesmeldenetz) M34 | 1 000 000 |
| VVTT    | Bessel 1841  | ÖBMN M28            | 1 000 000 |

Support for other CRS (e.g. GIP1, TFLV, ITMR, MTCV, GDAV) still needs to be implemented... sometimes... by someone (pull requests are appreciated). See [Appendix A.2.2](http://dbis.eprints.uni-ulm.de/1054/1/Kaufmann2014.pdf) for details.

### Step 4: Load route files

After everything has been prepared, the magic can happen.
The file `mapping_route_types.txt` can be extended by DIVA route types, so that they are correctly in the resulting GTFS file. After checking the mappings call

	./diva2gtfs.pl --path </path/to/diva/basedirectory/>

If the `lnrlit` table was correctly populated, everything should happen automagically. Or not. I am lacking test cases, so far.

### Step 5: Fine-tuning the data

Optionally, you can now use the guaranteed transfers data out of the DIVA anschlb table to update your trips table:

	./transfers2gtfs.pl

Also, you might want to import Shapes from your friendly EFA.
This is possible through `./efaShapeExporter.pl`, which is currently hardcoded to the DING EFA.
Messy coding, again, sorry.

### Finally: Output GTFS data

Run the following command:

	./export.sh <agencyName>


## Further reading

This script was created alongside Stefans diploma thesis, [Opening Public Transit Data in Germany – A Status Quo](http://dbis.eprints.uni-ulm.de/1054/), which includes more detailed information on the DIVA data format.
However, this is nothing more than the result of reverse engineering the format – without any guarantees as to completeness and/or accuracy.
Niko expanded on the features and some shortcomings of the implementation during his dissertation.

## Contact the author(s)

Reach Stefan via [Twitter (@_stk)](http://www.twitter.com/_stk), or E-Mail `transit at shutterworks dot org`. Further ramblings on public transit and open data (mostly in German) on [Stefans blog](http://stefan.bloggt.es). More Open Data tinkerers and their projects can be found on [UlmAPI](http://www.ulmapi.de), the home of Ulm's _datalove_ working group.

Reach Niko by E-Mail `niko at krismer dot de` or directly via [Github](https://github.com/nikolauskrismer/).
