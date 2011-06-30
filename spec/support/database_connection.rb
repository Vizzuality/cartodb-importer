# coding: UTF-8

module CartoDB
  class DatabaseConnection
    def self.connection
      @@connection ||= nil
      if @@connection
        @@connection
      else
        c = ::Sequel.connect('postgres://postgres:@localhost:5432/cartodb_importer_test')
        begin
          c.test_connection
          @@connection = c
        rescue
          c = ::Sequel.connect('postgres://postgres:@localhost:5432')
          c.run <<-SQL
CREATE DATABASE cartodb_importer_test
WITH TEMPLATE = template_postgis
OWNER = postgres
SQL
          @@connection = ::Sequel.connect('postgres://postgres:@localhost:5432/cartodb_importer_test')
        end
        return @@connection
      end
    end
  end
end