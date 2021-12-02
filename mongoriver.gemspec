# -*- coding: utf-8 -*-
$:.unshift(File.expand_path("lib", File.dirname(__FILE__)))
require 'mongoriver/version'

Gem::Specification.new do |gem|
  gem.authors       = ["Greg Brockman"]
  gem.email         = ["gdb@gregbrockman.com"]
  gem.description   = %q{Some tools and libraries to simplify tailing the mongod oplog}
  gem.summary       = %q{monogdb oplog-tailing utilities.}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "mongoriver"
  gem.require_paths = ["lib"]
  gem.version       = Mongoriver::VERSION

  gem.add_runtime_dependency('mongo', '>= 2.0')
  gem.add_runtime_dependency('bson_ext')
  gem.add_runtime_dependency('log4r')

  gem.add_development_dependency('rake')
  gem.add_development_dependency('minitest')
  gem.add_development_dependency('mocha', '>= 0.13')
end
