require 'rubygems'
require 'rubygems/command.rb'
require 'rubygems/dependency_installer.rb'

inst = Gem::DependencyInstaller.new
begin
  unless RUBY_VERSION.include?("jruby")
    inst.install "bson_ext"
  end
rescue
  exit(1)
end

File.open(File.join(File.dirname(__FILE__), "Makefile"), "w") do |f|
  f.write("install:\n\ttrue\n")
end
