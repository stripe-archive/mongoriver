# -*- encoding: utf-8 -*-
require File.expand_path('../lib/mongoriver/version', __FILE__)

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

  gem.add_runtime_dependency('mongo', '>= 1.7')
  gem.add_runtime_dependency('bson_ext')
  gem.add_runtime_dependency('log4r')
end
