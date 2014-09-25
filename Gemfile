source 'https://rubygems.org'

platforms :mri do
  # Conditionally install bson_ext if we are running mri. This logic is
  # replicated in the gemspec through an "extension" which install bson_ext
  # if we are not running jruby.
  gem "bson_ext"
end

gemspec
