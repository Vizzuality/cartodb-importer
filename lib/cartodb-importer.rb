# coding: UTF-8

require 'rubygems'
require 'bundler'
Bundler.setup

require 'rgeo'
require 'rgeo/geo_json'
require 'roo'
require 'csv'
require 'tempfile'
require 'ostruct'

require 'core_ext/string'
require 'core_ext/hash'
require 'core_ext/blank'

# load preprocessors and loaders
Dir[File.dirname(__FILE__) + '/cartodb-importer/lib/*.rb'].each {|file| require file }

# load factories
require 'cartodb-importer/decompressors/factory'
require 'cartodb-importer/preprocessors/factory'
require 'cartodb-importer/loaders/factory'

Dir[File.dirname(__FILE__) + '/cartodb-importer/decompressors/*.rb',
    File.dirname(__FILE__) + '/cartodb-importer/preprocessors/*.rb',
    File.dirname(__FILE__) + '/cartodb-importer/loaders/*.rb'].each {|file| require file }

# main file last
require 'cartodb-importer/importer'
