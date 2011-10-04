# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "cartodb-importer/version"

Gem::Specification.new do |s|
  s.name        = "cartodb-importer"
  s.version     = CartoDB::Importer::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Fernando Blat", "Andrew Hill", "Javier de la Torre", "Simon Tokumine"]
  s.email       = ["andrew@vizzuality.com"]
  s.homepage    = ""
  s.summary     = %q{Import CSV, SHP, and other files with data into a PostgreSQL table}
  s.description = %q{Import CSV, SHP, and other files with data into a PostgreSQL table}

  s.rubyforge_project = "cartodb-importer"

  s.files         = `git ls-files`.split("\n").reject{|fn| fn =~ /spec\/support\/data/}
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n").reject{|fn| fn =~ /spec\/support\/data/}
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.add_runtime_dependency "pg", "~> 0.11"
  s.add_runtime_dependency "sequel", "~> 3.28.0"
  s.add_runtime_dependency "roo", "~> 1.9.7"
  s.add_runtime_dependency "spreadsheet", "~> 0.6.5.9"
  s.add_runtime_dependency "google-spreadsheet-ruby", "~> 0.1.5"
  s.add_runtime_dependency "rubyzip", "~> 0.9.4"
  s.add_runtime_dependency "builder"
  s.add_runtime_dependency "rgeo", "~> 0.3.2"
  s.add_runtime_dependency "rgeo-geojson", "~> 0.2.1"
  
  s.add_development_dependency "rspec", "~> 2.6.0"
  s.add_development_dependency "mocha", "~> 0.10.0"
  s.add_development_dependency "ruby-debug19", "~> 0.11.6"
end