# coding: UTF-8

module CartoDB
  class Importer
    RESERVED_COLUMN_NAMES = %W{ oid tableoid xmin cmin xmax cmax ctid }
    SUPPORTED_FORMATS = %W{ .csv .shp .ods .xls .xlsx .tif .tiff }
    
    class << self
      attr_accessor :debug
    end
    @@debug = true
    
    attr_accessor :import_from_file,:import_from_url, :suggested_name,
                  :ext, :db_configuration, :db_connection
                  
    attr_reader :table_created, :force_name

    def initialize(options = {})
      @@debug = options[:debug] if options[:debug]
      @table_created = nil
      
      if !options[:import_from_url].blank?
        #download from internet first
        potential_name = File.basename(options[:import_from_url])
        curl_cmd = "curl -0 \"#{options[:import_from_url]}\" > /tmp/#{potential_name}"
        #log curl_cmd
        `#{curl_cmd}`
        @import_from_file = "/tmp/#{potential_name}"
      else
        @import_from_file = options[:import_from_file]
      end
      
      raise "import_from_file value can't be nil" if @import_from_file.nil?

      @db_configuration = options.slice(:database, :username, :password, :host, :port)
      @db_configuration[:port] ||= 5432
      @db_configuration[:host] ||= '127.0.0.1'
      @db_connection = Sequel.connect("postgres://#{@db_configuration[:username]}:#{@db_configuration[:password]}@#{@db_configuration[:host]}:#{@db_configuration[:port]}/#{@db_configuration[:database]}")

      unless options[:suggested_name].nil? || options[:suggested_name].blank?
        @force_name = true
        @suggested_name = get_valid_name(options[:suggested_name])
      else
        @force_name = false
      end
      
      if @import_from_file.is_a?(String)
        if @import_from_file =~ /^http/
          @import_from_file = URI.escape(@import_from_file)
        end
        open(@import_from_file) do |res|
          file_name = File.basename(import_from_file)
          @ext = File.extname(file_name)
          @suggested_name ||= get_valid_name(File.basename(import_from_file, @ext).downcase.sanitize)
          @import_from_file = Tempfile.new([@suggested_name, @ext])
          @import_from_file.write res.read.force_encoding('utf-8')
          @import_from_file.close
        end
      else
        original_filename = if @import_from_file.respond_to?(:original_filename)
          @import_from_file.original_filename
        else
          @import_from_file.path
        end
        @ext = File.extname(original_filename)
        @suggested_name ||= get_valid_name(File.basename(original_filename,@ext).tr('.','_').downcase.sanitize)
        @ext ||= File.extname(original_filename)
      end
    rescue => e
      log $!
      log e.backtrace
      raise e
    end
    
    def import!
      path = if @import_from_file.respond_to?(:tempfile)
        @import_from_file.tempfile.path
      else
        @import_from_file.path
      end
      python_bin_path = `which python`.strip
      psql_bin_path = `which psql`.strip
      
      entries = []
      if @ext == '.zip'
        log "Importing zip file: #{path}"
        Zip::ZipFile.foreach(path) do |entry|
          name = entry.name.split('/').last
          next if name =~ /^(\.|\_{2})/
          entries << "/tmp/#{name}"
          if SUPPORTED_FORMATS.include?(File.extname(name))
            @ext = File.extname(name)
            @suggested_name = get_valid_name(File.basename(name,@ext).tr('.','_').downcase.sanitize) unless @force_name
            path = "/tmp/#{name}"
            log "Found original @ext file named #{name} in path #{path}"
          end
          if File.file?("/tmp/#{name}")
            FileUtils.rm("/tmp/#{name}")
          end
          entry.extract("/tmp/#{name}")
        end
      end
        
      import_type = @ext
      runlog = OpenStruct.new
      runlog.log = Array.new
      runlog.stdout = Array.new
      runlog.err = Array.new
      
      # These types of files are converted to CSV
      if %W{ .xls .xlsx .ods }.include?(@ext)
        new_path = "/tmp/#{@suggested_name}.csv"
        case @ext
          when '.xls'
            Excel.new(path)
          when '.xlsx'
            Excelx.new(path)
          when '.ods'
            Openoffice.new(path)
          else
            runlog.log << "Don't know how to open file #{new_path}"
            raise ArgumentError, "Don't know how to open file #{new_path}"
        end.to_csv(new_path)
        @import_from_file = File.open(new_path,'r')
        @ext = '.csv'
        path = @import_from_file.path
      end
      
      if @ext == '.csv'
        
        ogr2ogr_bin_path = `which ogr2ogr`.strip
        ogr2ogr_command = %Q{#{ogr2ogr_bin_path} -f "PostgreSQL" PG:"host=#{@db_configuration[:host]} port=#{@db_configuration[:port]} user=#{@db_configuration[:username]} dbname=#{@db_configuration[:database]}" #{path} -nln #{@suggested_name}}
          
        out = `#{ogr2ogr_command}`
        if 0 < out.strip.length
          runlog.stdout << out
        end
        
        # Check if the file had data, if not rise an error because probably something went wrong
        if @db_connection["SELECT * from #{@suggested_name} LIMIT 1"].first.nil?
          runlog.err << "Empty table"
          raise "Empty table"
        end
        
        # Sanitize column names where needed
        column_names = @db_connection.schema(@suggested_name).map{ |s| s[0].to_s }
        need_sanitizing = column_names.each do |column_name|
          if column_name != column_name.sanitize_column_name
            @db_connection.run("ALTER TABLE #{@suggested_name} RENAME COLUMN \"#{column_name}\" TO #{column_name.sanitize_column_name}")
          end
        end
        
        @table_created = true
        
        FileUtils.rm_rf(path)
        rows_imported = @db_connection["SELECT count(*) as count from #{@suggested_name}"].first[:count]
        
        return OpenStruct.new({
          :name => @suggested_name, 
          :rows_imported => rows_imported,
          :import_type => import_type,
          :log => runlog
          })
      end
      if @ext == '.shp'
        
        shp2pgsql_bin_path = `which shp2pgsql`.strip

        host = @db_configuration[:host] ? "-h #{@db_configuration[:host]}" : ""
        port = @db_configuration[:port] ? "-p #{@db_configuration[:port]}" : ""
        #@suggested_name = get_valid_name(File.basename(path).tr('.','_').downcase.sanitize) unless @force_name
        random_table_name = "importing_#{Time.now.to_i}_#{@suggested_name}"
        
        normalizer_command = "#{python_bin_path} -Wignore #{File.expand_path("../../../misc/shp_normalizer.py", __FILE__)} #{path} #{random_table_name}"
        out = `#{normalizer_command}`
        shp_args_command = out.split( /, */, 4 )
        
        if shp_args_command.length != 4
          runlog.log << "Error running python shp_normalizer script: #{normalizer_command}"
          runlog.stdout << out
          raise "Error running python shp_normalizer script: #{normalizer_command}"
        end
        
        full_shp_command = "#{shp2pgsql_bin_path} -s #{shp_args_command[0]} -e -i -g the_geom -W #{shp_args_command[1]} #{shp_args_command[2]} #{shp_args_command[3].strip} | #{psql_bin_path} #{host} #{port} -U #{@db_configuration[:username]} -w -d #{@db_configuration[:database]}"
        log "Running shp2pgsql: #{full_shp_command}"
        
        out = `#{full_shp_command}`
        if 0 < out.strip.length
          runlog.stdout << out
        end
        
        if shp_args_command[1] != '4326'
          begin  
            @db_connection.run("SELECT UpdateGeometrySRID('#{random_table_name}', 'the_geom', 4326)")
            @db_connection.run("UPDATE \"#{random_table_name}\" SET the_geom = ST_Transform(the_geom, 4326)")
            @db_connection.run("CREATE INDEX \"#{random_table_name}_the_geom_gist\" ON \"#{random_table_name}\" USING GIST (the_geom)")
          rescue Exception => msg  
            runlog.err << msg
          end  
        end
        
        begin
          @db_connection.run("ALTER TABLE \"#{random_table_name}\" RENAME TO \"#{@suggested_name}\"")
          @table_created = true
        rescue Exception => msg  
          runlog.err << msg
        end  
        entries.each{ |e| FileUtils.rm_rf(e) } if entries.any?
        rows_imported = @db_connection["SELECT count(*) as count from \"#{@suggested_name}\""].first[:count]
        @import_from_file.unlink

        return OpenStruct.new({
          :name => @suggested_name, 
          :rows_imported => rows_imported,
          :import_type => import_type,
          :log => runlog
        })
      end
      if %W{ .tif .tiff }.include?(@ext)  
        log "Importing raster file: #{path}"
        
        raster2pgsql_bin_path = `which raster2pgsql.py`.strip
        
        host = @db_configuration[:host] ? "-h #{@db_configuration[:host]}" : ""
        port = @db_configuration[:port] ? "-p #{@db_configuration[:port]}" : ""
        
        random_table_name = "importing_#{Time.now.to_i}_#{@suggested_name}"
        
        gdal_command = "#{python_bin_path} -Wignore #{File.expand_path("../../../misc/srid_from_gdal.py", __FILE__)} #{path}"
        rast_srid_command = `#{gdal_command}`.strip
        
        if 0 < rast_srid_command.strip.length
          runlog.stdout << rast_srid_command
        end
        
        
        log "SRID : #{rast_srid_command}"
        
        blocksize = "180x180"
        full_rast_command = "#{raster2pgsql_bin_path} -I -s #{rast_srid_command.strip} -k #{blocksize} -t  #{random_table_name} -r #{path} | #{psql_bin_path} #{host} #{port} -U #{@db_configuration[:username]} -w -d #{@db_configuration[:database]}"
        log "Running raster2pgsql: #{raster2pgsql_bin_path}  #{full_rast_command}"
        out = `#{full_rast_command}`
        if 0 < out.strip.length
          runlog.stdout << out
        end
        
        begin
          @db_connection.run("CREATE TABLE \"#{@suggested_name}\" AS SELECT * FROM \"#{random_table_name}\"")
          @db_connection.run("DROP TABLE \"#{random_table_name}\"")
          @table_created = true
        rescue Exception => msg  
          runlog.err << msg
        end  
        
        entries.each{ |e| FileUtils.rm_rf(e) } if entries.any?
        rows_imported = @db_connection["SELECT count(*) as count from \"#{@suggested_name}\""].first[:count]
        @import_from_file.unlink
        
        @table_created = true
        
        entries.each{ |e| FileUtils.rm_rf(e) } if entries.any?
        rows_imported = @db_connection["SELECT count(*) as count from \"#{@suggested_name}\""].first[:count]
        @import_from_file.unlink
        
        return OpenStruct.new({
          :name => @suggested_name, 
          :rows_imported => rows_imported,
          :import_type => import_type,
          :log => runlog
        })
        
      end
    rescue => e
      log "====================="
      log $!
      log e.backtrace
      log "====================="
      if @table_created == nil
        @db_connection.drop_table(@suggested_name)
      end
      raise e
    ensure
      @db_connection.disconnect
      if @import_from_file.is_a?(File)
        File.unlink(@import_from_file) if File.file?(@import_from_file.path)
      elsif @import_from_file.is_a?(Tempfile)
        @import_from_file.unlink
      end
    end
    
    private
    
    def guess_schema(path)
      @col_separator = ','
      options = {:col_sep => @col_separator}
      schemas = []
      uk_column_counter = 0

      csv = CSV.open(path, options)
      column_names = csv.gets

      if column_names.size == 1
        candidate_col_separators = {}
        column_names.first.scan(/([^\w\s])/i).flatten.uniq.each do |candidate|
          candidate_col_separators[candidate] = 0
        end
        candidate_col_separators.keys.each do |candidate|
          csv = CSV.open(path, options.merge(:col_sep => candidate))
          column_names = csv.gets
          candidate_col_separators[candidate] = column_names.size
        end
        @col_separator = candidate_col_separators.sort{|a,b| a[1]<=>b[1]}.last.first
        csv = CSV.open(path, options.merge(:col_sep => @col_separator))
        column_names = csv.gets
      end

      column_names = column_names.map do |c|
        if c.blank?
          uk_column_counter += 1
          "unknow_name_#{uk_column_counter}"
        else
          c = c.force_encoding('utf-8').encode
          results = c.scan(/^(["`\'])[^"`\']+(["`\'])$/).flatten
          if results.size == 2 && results[0] == results[1]
            @quote = $1
          end
          c.sanitize_column_name
        end
      end

      while (line = csv.gets)
        line.each_with_index do |field, i|
          next if line[i].blank?
          unless @quote
            results = line[i].scan(/^(["`\'])[^"`\']+(["`\'])$/).flatten
            if results.size == 2 && results[0] == results[1]
              @quote = $1
            end
          end
          if schemas[i].nil?
            if line[i] =~ /^\-?[0-9]+[\.|\,][0-9]+$/
              schemas[i] = "float"
            elsif line[i] =~ /^[0-9]+$/
              schemas[i] = "integer"
            else
              schemas[i] = "varchar"
            end
          else
            case schemas[i]
            when "integer"
              if line[i] !~ /^[0-9]+$/
                if line[i] =~ /^\-?[0-9]+[\.|\,][0-9]+$/
                  schemas[i] = "float"
                else
                  schemas[i] = "varchar"
                end
              elsif line[i].to_i > 2147483647
                schemas[i] = "float"
              end
            end
          end
        end
      end

      result = []
      column_names.each_with_index do |column_name, i|
        if RESERVED_COLUMN_NAMES.include?(column_name.to_s)
          column_name = "_#{column_name}"
        end
        result << "#{column_name} #{schemas[i] || "varchar"}"
      end
      return result
    end
    
    def get_valid_name(name)
      existing_names = @db_connection["select relname from pg_stat_user_tables WHERE schemaname='public' and relname ilike '#{name}%'"].map(:relname)
      testn = 1
      uniname = name
      while true==existing_names.include?("#{uniname}")
        uniname = "#{name}_#{testn}"
        testn = testn + 1
      end
      return uniname
    end
    
    def log(str)
      if @@debug
        puts str
      end
    end
  end
end
