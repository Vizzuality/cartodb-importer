# CartoDB importer #

CartoDB importer is a Ruby gem that makes your life easier when importing data from a file into a PostGIS database. The accepted formats for input files are:

  - CSV
  - SHP (in a zip file)
  - ODS
  - XLX(S)
  
## Installation and dependencies ##

To install Ruby dependencies just install `bundler` gem and run the command `bundle install` in your shell.

There are also some dependencies of external Python libraries (WTF!):

- Python setup tools: `$ sudo apt-get install python-setuptools`
    
- Python GDAL: `$ sudo apt-get install python-gdal`
    
- Python Chardet (install from http://chardet.feedparser.org/download/)
    
- Python ArgParse (install from http://code.google.com/p/argparse/)
    
- Brewery:

    `$ git clone git://github.com/Stiivi/brewery.git`
    `$ python setup.py install`

## How to use it? ##

The way to use this gem is to initialize a object of class Cartodb::Importer using the appropiate parameters.

    importer = Cartodb::Importer.new :import_from_file => "path to CSV file", :srid => 4326, :database => "...",
                                     :username => "...", :password => "..."
    result = importer.import!
  
If everything works fine, a new table will exist in the given database. A `result` object is return with some information about the import, such as the number of rows, or the name of the table.

    puts result.rows_imported
    # > 43243
  
If any error happens, an exception could be raised.

This is the list with all the available options to use in the constructor:

  - import_from_file: a file descriptor, Tempfile or URL with the URL from which import the data
  - srid: the value of the SRID 
  - database: the name of the database where import the data
  - username: the owner of the database
  - password: the password to connect to the database
  - extra_columns: a SQL string with some extra columns that should be added to the imported table. If any of these columns already exists an error will be raised
  
## Running the specs ##

CartoDB Importer has a suite of specs which define its specification. To run this suite a database named cartodb_importer_test must exist. You can create this database by running:

    CREATE DATABASE cartodb_importer_test
    WITH TEMPLATE = template_postgis
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT=-1

Then, to run the specs just run this command:

    bundle exec rspec spec/import_spec.rb
    
