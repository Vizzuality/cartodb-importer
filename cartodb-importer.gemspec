# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "cartodb-importer/version"

Gem::Specification.new do |s|
  s.name        = "cartodb-importer"
  s.version     = CartoDB::Importer::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Fernando Blat"]
  s.email       = ["ferblape@gmail.com"]
  s.homepage    = ""
  s.summary     = %q{Import CSV, SHP, and other files with data into a PostgreSQL table}
  s.description = %q{Import CSV, SHP, and other files with data into a PostgreSQL table}

  s.rubyforge_project = "cartodb-importer"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.add_runtime_dependency 'sequel'
  s.add_runtime_dependency "pg", "0.11"
  s.add_runtime_dependency "sequel"
  s.add_runtime_dependency "roo"
  s.add_runtime_dependency "spreadsheet"
  s.add_runtime_dependency "google-spreadsheet-ruby"
  s.add_runtime_dependency "rubyzip"
  s.add_runtime_dependency "builder"
  
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rspec-core'
  s.add_development_dependency 'mocha'
  s.add_development_dependency "ruby-debug19"
end
